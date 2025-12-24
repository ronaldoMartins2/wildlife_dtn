import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import numpy as np
import pandas as pd
import argparse
import os
import matplotlib.pyplot as plt

# SEED
torch.manual_seed(42)
np.random.seed(42)

class TrajectoryDataset(Dataset):
    def __init__(self, data_path, window_size=30, mask_range=(3, 10), mode='train'):
        self.df = pd.read_csv(data_path)
        self.window_size = window_size
        self.mask_range = mask_range
        self.mode = mode
        self.windows = []
        
        # Group by session
        # Ensure data is sorted by session and time (done in preprocess but safe to check)
        # We assume preprocess gave us clean session_ids
        
        for sid, group in self.df.groupby('session_id'):
            coords = group[['pos_x', 'pos_y']].values.astype(np.float32)
            times = pd.to_datetime(group['timestamp'])
            dt = times.diff().dt.total_seconds().fillna(0).values.astype(np.float32)
            
            n = len(group)
            if n < window_size:
                continue
            
            # Stride
            stride = 5 if mode == 'train' else window_size // 2
            
            for i in range(0, n - window_size + 1, stride):
                w_coords = coords[i:i+window_size]
                w_dt = dt[i:i+window_size]
                self.windows.append((w_coords, w_dt))

    def __len__(self):
        return len(self.windows)

    def __getitem__(self, idx):
        coords, dt = self.windows[idx]
        
        # Mean Instance Normalization
        mean = np.mean(coords, axis=0)
        std = np.std(coords, axis=0) + 1e-6 
        
        coords_norm = (coords - mean) / std
        
        # Log dt
        dt_log = np.log1p(dt)
        
        # Create Mask
        seq_len = len(coords)
        mask = np.zeros(seq_len, dtype=np.float32)
        
        if self.mode == 'train':
            gap_len = np.random.randint(self.mask_range[0], self.mask_range[1] + 1)
            # Ensure gap fits and has padding
            if seq_len - gap_len - 2 > 2:
                start_idx = np.random.randint(2, seq_len - gap_len - 2)
                mask[start_idx : start_idx + gap_len] = 1.0
        else:
            # Deterministic mask for validation
            gap_len = self.mask_range[0] # Fixed size
            start_idx = seq_len // 2 - gap_len // 2
            mask[start_idx : start_idx + gap_len] = 1.0

        # Input Construction
        # Masked coords are 0
        input_coords = coords_norm.copy()
        input_coords[mask == 1] = 0.0
        
        # Features: x, y, dt_log, mask_flag
        x_in = np.column_stack([input_coords, dt_log, mask])
        
        return {
            'input': torch.tensor(x_in, dtype=torch.float32),
            'target': torch.tensor(coords_norm, dtype=torch.float32),
            'mask': torch.tensor(mask, dtype=torch.float32),
            'mean': torch.tensor(mean, dtype=torch.float32),
            'std': torch.tensor(std, dtype=torch.float32),
            'dt_raw': torch.tensor(dt, dtype=torch.float32)
        }

class BFBiLSTM(nn.Module):
    def __init__(self, input_dim=4, hidden_dim=64):
        super().__init__()
        # Input dim: x, y, dt, mask (4)
        self.lstm = nn.LSTM(input_dim, hidden_dim, batch_first=True, bidirectional=True)
        # Concatenation of Forward and Backward hidden states -> 2 * hidden_dim
        self.fc = nn.Linear(hidden_dim * 2, 2)
        
    def forward(self, x):
        out, _ = self.lstm(x)
        pred = self.fc(out)
        return pred

def physics_loss(pred_norm, target_norm, mask, mean, std, dt_raw, w_mse, w_kin, w_bio, v_max):
    mse_crit = nn.MSELoss(reduction='none')
    
    # 1. MSE on Gaps (Reconstruction)
    # Mask indicates GAP.
    # We want to minimize error specifically where mask == 1 (the missing parts)
    
    loss_map = mse_crit(pred_norm, target_norm).sum(dim=2) # [B, L]
    
    # Only average over masked tokens
    # Add epsilon to mask sum to avoid NaN if mask is empty
    masked_loss = (loss_map * mask).sum() / (mask.sum() + 1e-6)
    
    if w_kin == 0 and w_bio == 0:
        return masked_loss, masked_loss.item(), 0.0
        
    # 2. Physics Constraints
    # Reconstruction in Real Space
    # Normalize: pred_norm is prediction for EVERYTHING. 
    # But for physics, we should arguably use Known info where available?
    # Actually, if the model predicts the whole sequence, we check the physics of the *predicted* sequence on the gaps.
    # Or strict physics on the whole sequence?
    # Let's inspect the whole predicted sequence for smoothness.
    
    pred_real = pred_norm * std.unsqueeze(1) + mean.unsqueeze(1)
    
    # Velocity
    d_pos = pred_real[:, 1:, :] - pred_real[:, :-1, :]
    dist = torch.norm(d_pos, dim=2)
    dt_seg = dt_raw[:, 1:] + 1e-6 # Avoid div 0
    
    v = dist / dt_seg
    
    # Kinematic Penalty
    v_excess = torch.relu(v - v_max)
    loss_kin = v_excess.mean()
    
    # Bio Penalty (Quadratic)
    loss_bio = (v_excess ** 2).mean()
    
    total_loss = w_mse * masked_loss + w_kin * loss_kin + w_bio * loss_bio
    
    return total_loss, masked_loss.item(), loss_kin.item()

def train(args):
    dataset_name = args.dataset
    if dataset_name == 'jaguar':
        csv_path = "jaguar_preprocessed.csv"
        v_max = 1.5 # m/s (5.4 km/h)
        epochs = 20
    else:
        csv_path = "tangara_preprocessed.csv"
        v_max = 20.0
        epochs = 20
        
    full_path = os.path.join(args.data_dir, csv_path)
    
    ds = TrajectoryDataset(full_path, mode='train')
    dl = DataLoader(ds, batch_size=32, shuffle=True)
    
    model = BFBiLSTM().cuda() if torch.cuda.is_available() else BFBiLSTM()
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)
    
    optimizer = optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, 'min', patience=3)
    
    print(f"Starting training for {dataset_name} on {device}...")
    print(f"Dataset size: {len(ds)} windows")
    
    metrics = []
    
    for epoch in range(epochs):
        model.train()
        total_loss = 0
        total_mse = 0
        total_kin = 0
        
        # Curriculum: Ramp up physics weights
        # Start pure MSE, then add physics
        if epoch < 5:
            w_kin = 0.0
            w_bio = 0.0
        else:
            w_kin = args.w_kin * (min((epoch-5)/5.0, 1.0)) # Ramp up over 5 epochs
            w_bio = args.w_bio * (min((epoch-5)/5.0, 1.0))
            
        for batch in dl:
            x = batch['input'].to(device)
            target = batch['target'].to(device)
            mask = batch['mask'].to(device)
            mean = batch['mean'].to(device)
            std = batch['std'].to(device)
            dt_raw = batch['dt_raw'].to(device)
            
            optimizer.zero_grad()
            
            pred = model(x)
            
            loss, mse_val, kin_val = physics_loss(pred, target, mask, mean, std, dt_raw, 
                                                 1.0, w_kin, w_bio, v_max)
            
            if torch.isnan(loss):
                print(f"Loss is NaN at epoch {epoch}, batch!")
                continue

            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            
            total_loss += loss.item()
            total_mse += mse_val
            total_kin += kin_val
            
        avg_loss = total_loss / len(dl)
        avg_mse = total_mse / len(dl)
        print(f"Epoch {epoch+1}/{epochs} | Loss: {avg_loss:.4f} | MSE (Norm): {avg_mse:.4f} | Kin: {total_kin/len(dl):.4f} | W_Kin: {w_kin:.2f}")
        
        metrics.append({
            'epoch': epoch,
            'mse': avg_mse,
            'kin': total_kin/len(dl)
        })
        
        scheduler.step(avg_mse)

    # Save Results
    results_file = os.path.join(args.results_dir, f"{dataset_name}_metrics.csv")
    pd.DataFrame(metrics).to_csv(results_file, index=False)
    
    # Save Model
    model_path = os.path.join(args.results_dir, f"{dataset_name}_model.pth")
    torch.save(model.state_dict(), model_path)
    print(f"Saved model to {model_path}")

    # --- Validation / Test Sanity Check ---
    model.eval()
    
    # Get a batch for validation
    val_ds = TrajectoryDataset(full_path, mode='val') 
    if len(val_ds) == 0:
        print("Validation set empty!")
        return

    val_dl = DataLoader(val_ds, batch_size=min(len(val_ds), 128))
    
    all_v = []
    total_ade = 0
    total_count = 0
    
    with torch.no_grad():
        for batch in val_dl:
            x = batch['input'].to(device)
            target = batch['target'].to(device)
            mask = batch['mask'].to(device)
            mean = batch['mean'].to(device)
            std = batch['std'].to(device)
            dt_raw = batch['dt_raw'].to(device)
            
            pred = model(x)
            
            # Denormalize
            pred_real = pred * std.unsqueeze(1) + mean.unsqueeze(1)
            target_real = target * std.unsqueeze(1) + mean.unsqueeze(1)
            
            # Calculate ADE/FDE on Masked Region
            diff = torch.norm(pred_real - target_real, dim=2) # [B, L]
            
            # ADE: Mean error on masked points
            ade_sum = (diff * mask).sum()
            mask_sum = mask.sum()
            
            if mask_sum > 0:
                total_ade += ade_sum.item()
                total_count += mask_sum.item()
            
            # Velocity sanity check (histogram data)
            d_pos = pred_real[:, 1:, :] - pred_real[:, :-1, :]
            dist = torch.norm(d_pos, dim=2)
            dt_seg = dt_raw[:, 1:] + 1e-6
            v = dist / dt_seg
            
            v_np = v.cpu().numpy().flatten()
            # Remove NaNs if any
            v_np = v_np[~np.isnan(v_np)]
            all_v.extend(v_np)
        
        avg_ade = total_ade / total_count if total_count > 0 else 0
        print(f"Validation ADE: {avg_ade:.2f} meters")
        
        # Flatten velocities
        v_flat = np.array(all_v)
        
        if len(v_flat) > 0:
            plt.figure()
            try:
                plt.hist(v_flat, bins=50, alpha=0.7, label='Predicted Velocities')
                plt.axvline(v_max, color='r', linestyle='--', label='Max Limit')
                plt.title(f"Velocity Distribution ({dataset_name})")
                plt.xlabel("Velocity (m/s)")
                plt.legend()
                plt.savefig(os.path.join(args.results_dir, f"{dataset_name}_vel_hist.png"))
                print(f"Saved velocity histogram.")
            except Exception as e:
                print(f"Plotting failed: {e}")
            
            # Check for Super-Jaguar/Tangara (percentile)
            p99 = np.percentile(v_flat, 99)
            print(f"99th Percentile Velocity: {p99:.2f} m/s (Limit: {v_max} m/s)")
            if p99 > v_max * 1.5:
                 print("WARNING: 'Super-Animal' anomaly detected! Kinematic constraints may need higher weight.")
        else:
            print("No velocity data collected.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True, choices=['jaguar', 'tangara'])
    parser.add_argument('--data_dir', type=str, default='/home/abinadabe/projetos/wildlife_dtn/scripts/Results/PIDL_Preprocessed')
    parser.add_argument('--results_dir', type=str, default='/home/abinadabe/projetos/wildlife_dtn/scripts/Results/PIDL_Output')
    parser.add_argument('--w_kin', type=float, default=0.1)
    parser.add_argument('--w_bio', type=float, default=0.01)
    
    args = parser.parse_args()
    
    os.makedirs(args.results_dir, exist_ok=True)
    train(args)
