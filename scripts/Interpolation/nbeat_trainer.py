# nbeat_trainer.py
import os
import json
import torch
import torch.nn as nn
import pandas as pd
import numpy as np
import joblib
import random
from torch.utils.data import TensorDataset, DataLoader
from torch.nn.utils import clip_grad_norm_

from Common.utils import results_folder, read_field_from_json
from Interpolation.nbeat_model import NBeats
from Interpolation.nbeat_data_prep import preprocess_nbeats_data

def train_nbeats_model_single(current_animal, file_rawdata_name, file_rawdata_columns):
    print(f"[NBEATS] Starting pipeline for animal {current_animal}...")
    
    # 1. Load Data
    results_dir = results_folder(file_rawdata_name)
    # Ideally we load from the raw source, but the current pipeline seems to look for 'map_{ID}.csv' in results
    # or we can assume we load from the main raw file and filter. 
    # The 'check_app.py' or 'app_wildlife.py' usually calls this.
    # Let's try to load 'map_{current_animal}.csv' from results first as previous code did.
    
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    try:
        df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return

    # 2. Hyperparameters
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    # Defaults matching Prompt/Appendix
    epochs = read_field_from_json(hyperparam_path, 'epochs_nbeats') or 100
    lr = read_field_from_json(hyperparam_path, 'lr_nbeats') or 0.0003
    batch_size = read_field_from_json(hyperparam_path, 'batch_size_nbeat') or 32
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat") or 64
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat") or 2
    
    # New params for Multi-Horizon
    input_width = 10 
    forecast_horizon = 5 # Can be parameterized
    
    # 3. Data Prep
    X_train, y_train, X_val, y_val, X_test, y_test, scaler, metadata = preprocess_nbeats_data(
        df, file_rawdata_columns, input_width=input_width, forecast_horizon=forecast_horizon
    )
    
    # Update metadata with model params
    metadata['hidden_dim'] = int(hidden_dim)
    metadata['num_blocks'] = int(num_blocks)
    
    if X_train is None:
        print("Data Prep failed (too short or empty).")
        return

    # 4. Tensors
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"[NBEATS] Device: {device}")

    X_train_t = torch.tensor(X_train, dtype=torch.float32)
    y_train_t = torch.tensor(y_train, dtype=torch.float32)
    train_loader = DataLoader(TensorDataset(X_train_t, y_train_t), batch_size=batch_size, shuffle=True)
    
    val_loader = None
    if X_val is not None:
        X_val_t = torch.tensor(X_val, dtype=torch.float32)
        y_val_t = torch.tensor(y_val, dtype=torch.float32)
        val_loader = DataLoader(TensorDataset(X_val_t, y_val_t), batch_size=batch_size, shuffle=False)

    # 5. Model Setup
    # Input/Output features = 2 (Delta_E, Delta_N)
    input_features = 2 
    
    model = NBeats(
        input_steps=input_width,
        output_steps=forecast_horizon,
        input_features=input_features,
        hidden_dim=hidden_dim,
        num_blocks=num_blocks
    ).to(device)
    
    class PhysicsInformedLoss(nn.Module):
        def __init__(self, mean, std, speed_limit_meters=1666.0, penalty_weight=0.1):
            """
            speed_limit_meters: Max plausible speed per step in meters. 
                                6 km/h = 6000 m/h. 
                                If freq is 1H, limit is 6000.
                                If freq is 10 min, limit is 1000.
            penalty_weight: Weight of the physics penalty.
            """
            super().__init__()
            self.mse = nn.MSELoss()
            self.mean = torch.tensor(mean, dtype=torch.float32).to(device)
            self.std = torch.tensor(std, dtype=torch.float32).to(device)
            self.limit = speed_limit_meters
            self.lamb = penalty_weight
            
        def forward(self, pred_scaled, target_scaled):
            # 1. Standard MSE
            mse_loss = self.mse(pred_scaled, target_scaled)
            
            # 2. Physics Penalty
            # Unscale to get real meters
            # pred: (Batch, Horizon, 2)
            # mean/std: (2,)
            
            pred_real = pred_scaled * self.std + self.mean
            
            # Calculate distance per step (speed if step is fixed)
            # dist = sqrt(delta_e^2 + delta_n^2)
            dist = torch.norm(pred_real, p=2, dim=2) # (Batch, Horizon)
            
            # Penalty: only if dist > limit
            excess = torch.relu(dist - self.limit)
            
            # Mean excess penalty
            penalty = excess.mean()
            
            return mse_loss + self.lamb * penalty

    # Extract scaler stats
    # scaler.mean_ and scaler.scale_ are numpy arrays
    scaler_mean = scaler.mean_
    scaler_std = scaler.scale_
    
    animal_name = file_rawdata_name.split('_')[0]



    # Calculate speed limit based on median_delta
    # 6 km/h = 1.67 m/s
    median_seconds = metadata['median_delta_seconds']
    max_speed_mps = 1.67 # ~6 km/h
    # Relax it a bit to 2.5 m/s (~9 km/h) to avoid penalizing running/sprinting too hard
    # but still kill the 20 km/h jumps.
    #max_speed_mps = 2.5 
    if  "jaguar" in animal_name:
        max_speed_mps = 0.07 # ~0.25 km/h
    elif "tangara" in animal_name:
        max_speed_mps  = 8 # ~28.8 km/h
    
    step_limit_meters = max_speed_mps * median_seconds
    
    print(f"[Loss] Speed limit set to {step_limit_meters:.2f} meters per step ({median_seconds}s).")

    criterion = PhysicsInformedLoss(
        mean=scaler_mean, 
        std=scaler_std, 
        speed_limit_meters=step_limit_meters,
        penalty_weight=0.1
    )
    
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, betas=(0.9, 0.999), weight_decay=1e-5)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, factor=0.5, patience=20, min_lr=1e-6
    )

    # 6. Training Loop
    best_val = float('inf')
    best_epoch = 0
    history = {'train_loss': [], 'val_loss': []}
    patience = 20 # Early stopping
    
    print(f"[NBEATS] Training {current_animal} | Ep: {epochs} | LR: {lr}")

    for epoch in range(1, epochs+1):
        model.train()
        epoch_losses = []
        for xb, yb in train_loader:
            xb = xb.to(device)
            yb = yb.to(device)
            
            # Data Augmentation: Jitter
            noise = torch.randn_like(xb) * 0.01
            xb_aug = xb + noise
            
            optimizer.zero_grad()
            out = model(xb_aug)
            loss = criterion(out, yb)
            loss.backward()
            clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            epoch_losses.append(loss.item())
            
        train_loss = np.mean(epoch_losses)
        
        # Validation
        val_loss = train_loss
        if val_loader:
            model.eval()
            val_losses = []
            with torch.no_grad():
                for xb, yb in val_loader:
                    xb = xb.to(device)
                    yb = yb.to(device)
                    out = model(xb)
                    loss = criterion(out, yb)
                    val_losses.append(loss.item())
            val_loss = np.mean(val_losses)
        
        history['train_loss'].append(train_loss)
        history['val_loss'].append(val_loss)
        
        scheduler.step(val_loss)
        
        if val_loss < best_val:
            best_val = val_loss
            best_epoch = epoch
            # Save Model
            models_dir = os.path.join(script_dir, 'models')
            os.makedirs(models_dir, exist_ok=True)
            torch.save(
                {
                    'model_state_dict': model.state_dict(),
                    'metadata': metadata
                },
                os.path.join(models_dir, f'nbeats_model_{current_animal}.pth')
            )
            # Save Scaler
            joblib.dump(scaler, os.path.join(models_dir, f'scaler_{current_animal}.pkl'))
        
        if epoch % 10 == 0:
            print(f"Ep {epoch} | Train: {train_loss:.5f} | Val: {val_loss:.5f}")
            
        if epoch - best_epoch > patience:
            print(f"Early stopping at {epoch}")
            break

    # Save History
    models_dir = os.path.join(script_dir, 'models')
    with open(os.path.join(models_dir, f'nbeats_history_{current_animal}.json'), 'w') as f:
        json.dump(history, f, indent=2)

    print(f"✅ Finished {current_animal}. Best Val: {best_val:.5f} @ Ep {best_epoch}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 4:
        train_nbeats_model_single(sys.argv[1], sys.argv[2], sys.argv[3])