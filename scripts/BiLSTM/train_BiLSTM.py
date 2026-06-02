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
    def __init__(self, windows, mask_range=(3, 10), mode='train'):
        self.windows = windows
        self.mask_range = mask_range
        self.mode = mode

    def __len__(self):
        return len(self.windows)

    def __getitem__(self, idx):
        coords, dt = self.windows[idx]
        mean = np.mean(coords, axis=0)
        std = np.std(coords, axis=0) + 1e-6 
        
        coords_norm = (coords - mean) / std
    
        dt_log = np.log1p(dt)
    
        seq_len = len(coords)
        mask = np.zeros(seq_len, dtype=np.float32)
        
        if self.mode == 'train':
            gap_len = np.random.randint(self.mask_range[0], self.mask_range[1] + 1)
            # Ensure gap fits and has padding
            if seq_len - gap_len - 2 > 2:
                start_idx = np.random.randint(2, seq_len - gap_len - 2)
                mask[start_idx : start_idx + gap_len] = 1.0
        else:
            # Deterministic mask for validation/test
            gap_len = self.mask_range[0] # Fixed size
            start_idx = seq_len // 2 - gap_len // 2
            mask[start_idx : start_idx + gap_len] = 1.0

        input_coords = coords_norm.copy()
        input_coords[mask == 1] = 0.0
    
        x_in = np.column_stack([input_coords, dt_log, mask])
        
        return {
            'input': torch.tensor(x_in, dtype=torch.float32),
            'target': torch.tensor(coords_norm, dtype=torch.float32),
            'mask': torch.tensor(mask, dtype=torch.float32),
            'mean': torch.tensor(mean, dtype=torch.float32),
            'std': torch.tensor(std, dtype=torch.float32),
            'dt_raw': torch.tensor(dt, dtype=torch.float32)
        }

def create_windows(data_path, window_size=30, stride=5):
    df = pd.read_csv(data_path)
    windows = []
    
    # Group by session
    # Ensure data is sorted by session and time (done in preprocess but safe to check)
    for sid, group in df.groupby('session_id'):
        coords = group[['pos_x', 'pos_y']].values.astype(np.float32)
        times = pd.to_datetime(group['timestamp'])
        dt = times.diff().dt.total_seconds().fillna(0).values.astype(np.float32)
        
        n = len(group)
        if n < window_size:
            continue
        
        for i in range(0, n - window_size + 1, stride):
            w_coords = coords[i:i+window_size]
            w_dt = dt[i:i+window_size]
            windows.append((w_coords, w_dt))
            
    return windows

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
    
    loss_map = mse_crit(pred_norm, target_norm).sum(dim=2) # [B, L]
    
    masked_loss = (loss_map * mask).sum() / (mask.sum() + 1e-6)
    
    if w_kin == 0 and w_bio == 0:
        return masked_loss, masked_loss.item(), 0.0
    
    pred_real = pred_norm * std.unsqueeze(1) + mean.unsqueeze(1)
    
    d_pos = pred_real[:, 1:, :] - pred_real[:, :-1, :]
    dist = torch.norm(d_pos, dim=2)
    dt_seg = dt_raw[:, 1:] + 1e-6
    
    v = dist / dt_seg
    
    v_excess = torch.relu(v - v_max)
    loss_kin = v_excess.mean()
    
    loss_bio = (v_excess ** 2).mean()
    
    total_loss = w_mse * masked_loss + w_kin * loss_kin + w_bio * loss_bio
    
    return total_loss, masked_loss.item(), loss_kin.item()

def train(args):
    dataset_name = args.dataset
    if dataset_name == 'jaguar':
        csv_path = "jaguar_preprocessed.csv"
        #v_max = 1.5 # m/s (5.4 km/h) #Original
        v_max = 0.07 # m/s (0.25 km/h)
    else:
        csv_path = "tangara_preprocessed.csv"
        v_max = 20.0
        
    epochs = args.epochs
        
    full_path = os.path.join(args.data_dir, csv_path)

    print(f"Generating windows from {full_path}...")
    all_windows = create_windows(full_path, window_size=30, stride=5)
    total_windows = len(all_windows)

    indices = np.random.permutation(total_windows)
    
    n_train = int(total_windows * 0.70)
    n_val = int(total_windows * 0.15)
    n_test = total_windows - n_train - n_val
    
    train_idx = indices[:n_train]
    val_idx = indices[n_train:n_train+n_val]
    test_idx = indices[n_train+n_val:]
    
    train_windows = [all_windows[i] for i in train_idx]
    val_windows = [all_windows[i] for i in val_idx]
    test_windows = [all_windows[i] for i in test_idx]
    
    print(f"Total windows: {total_windows}")
    print(f"Train: {len(train_windows)} | Val: {len(val_windows)} | Test: {len(test_windows)}")
    
    # Datasets
    train_ds = TrajectoryDataset(train_windows, mode='train')
    val_ds = TrajectoryDataset(val_windows, mode='val')
    test_ds = TrajectoryDataset(test_windows, mode='test')
    
    train_dl = DataLoader(train_ds, batch_size=32, shuffle=True)
    val_dl = DataLoader(val_ds, batch_size=128, shuffle=False)
    test_dl = DataLoader(test_ds, batch_size=128, shuffle=False)
    
    model = BFBiLSTM().cuda() if torch.cuda.is_available() else BFBiLSTM()
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)
    
    optimizer = optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, 'min', patience=3)
    
    print(f"Starting training for {dataset_name} on {device}...")
    
    metrics = []
    
    for epoch in range(epochs):
        model.train()
        total_loss = 0
        total_mse = 0
        total_kin = 0
        
        if epoch < 5:
            w_kin = 0.0
            w_bio = 0.0
        else:
            w_kin = args.w_kin * (min((epoch-5)/5.0, 1.0)) 
            w_bio = args.w_bio * (min((epoch-5)/5.0, 1.0))
            
        for batch in train_dl:
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
            
        avg_loss = total_loss / len(train_dl)
        avg_mse = total_mse / len(train_dl)
        print(f"Epoch {epoch+1}/{epochs} | Loss: {avg_loss:.4f} | MSE: {avg_mse:.4f} | Kin: {total_kin/len(train_dl):.4f}")
        
        # Validation Step
        model.eval()
        total_val_ade = 0
        total_val_count = 0
        
        with torch.no_grad():
            for batch in val_dl:
                x = batch['input'].to(device)
                target = batch['target'].to(device)
                mask = batch['mask'].to(device)
                mean = batch['mean'].to(device)
                std = batch['std'].to(device)
                 
                pred = model(x)
                 
                pred_real = pred * std.unsqueeze(1) + mean.unsqueeze(1)
                target_real = target * std.unsqueeze(1) + mean.unsqueeze(1)
                 
                diff = torch.norm(pred_real - target_real, dim=2)
                ade_sum = (diff * mask).sum()
                mask_sum = mask.sum()
                 
                if mask_sum > 0:
                    total_val_ade += ade_sum.item()
                    total_val_count += mask_sum.item()
        
        val_ade = total_val_ade / total_val_count if total_val_count > 0 else 0
        print(f"  >> Val ADE: {val_ade:.4f}")
        
        metrics.append({
            'epoch': epoch,
            'mse': avg_mse,
            'kin': total_kin/len(train_dl),
            'val_ade': val_ade
        })
        
        scheduler.step(val_ade) 

    # Save Results
    results_file = os.path.join(args.results_dir, f"{dataset_name}_metrics.csv")
    pd.DataFrame(metrics).to_csv(results_file, index=False)
    
    # Save Model
    model_path = os.path.join(args.results_dir, f"{dataset_name}_model.pth")
    torch.save(model.state_dict(), model_path)
    print(f"Saved model to {model_path}")

    # --- Test Logic ---
    print("\n--- Running Test ---")
    model.eval()
    
    total_test_ade = 0
    total_test_count = 0
    all_v = []
    
    with torch.no_grad():
        for batch in test_dl:
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
            
            # Calculate ADE
            diff = torch.norm(pred_real - target_real, dim=2) 
            ade_sum = (diff * mask).sum()
            mask_sum = mask.sum()
            
            if mask_sum > 0:
                total_test_ade += ade_sum.item()
                total_test_count += mask_sum.item()
            
            d_pos = pred_real[:, 1:, :] - pred_real[:, :-1, :]
            dist = torch.norm(d_pos, dim=2)
            dt_seg = dt_raw[:, 1:] + 1e-6
            v = dist / dt_seg
            
            v_np = v.cpu().numpy().flatten()
            v_np = v_np[~np.isnan(v_np)]
            all_v.extend(v_np)
        
        test_ade = total_test_ade / total_test_count if total_test_count > 0 else 0
        print(f"TEST ADE: {test_ade:.4f} meters")
        
        v_flat = np.array(all_v)
        
        if len(v_flat) > 0:
            plt.figure()
            try:
                plt.hist(v_flat, bins=50, alpha=0.7, label='Predicted Velocities (Test)')
                plt.axvline(v_max, color='r', linestyle='--', label='Max Limit')
                plt.title(f"Velocity Distribution ({dataset_name}) - Test Set")
                plt.xlabel("Velocity (m/s)")
                plt.legend()
                plt.savefig(os.path.join(args.results_dir, f"{dataset_name}_vel_hist.png"))
                print(f"Saved velocity histogram.")
            except Exception as e:
                print(f"Plotting failed: {e}")
            
            p99 = np.percentile(v_flat, 99)
            print(f"99th Percentile Velocity: {p99:.2f} m/s (Limit: {v_max} m/s)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True, choices=['jaguar', 'tangara'])
    parser.add_argument('--data_dir', type=str, default='scripts/Results/BiLSTM_Preprocessed')
    parser.add_argument('--results_dir', type=str, default='scripts/Results/BiLSTM_Output')
    parser.add_argument('--w_kin', type=float, default=0.1)
    parser.add_argument('--w_bio', type=float, default=0.01)
    parser.add_argument('--epochs', type=int, default=20)
    
    args = parser.parse_args()
    
    os.makedirs(args.results_dir, exist_ok=True)
    train(args)
