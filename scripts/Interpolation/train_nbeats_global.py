import sys
import os
import json
import argparse
import pandas as pd
import numpy as np
import torch
import joblib
from torch.utils.data import TensorDataset, DataLoader

# Add scripts folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Common.utils import get_list_animals, results_folder, read_field_from_json
from Interpolation.nbeat_model import NBeats
from Interpolation.nbeat_data_prep import preprocess_nbeats_data
from Interpolation.nbeat_trainer import train_model, PhysicsInformedLoss

def train_nbeats_global(file_rawdata, file_columns, forecast_steps=50):
    print(f"--- Starting Global N-Beats Training for ALL animals in {file_rawdata} ---")
    
    # 1. Hyperparameters
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    
    epochs = read_field_from_json(hyperparam_path, 'epochs_nbeats') or 100
    lr = read_field_from_json(hyperparam_path, 'lr_nbeats') or 0.0003
    batch_size = read_field_from_json(hyperparam_path, 'batch_size_nbeat') or 32
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat") or 64
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat") or 2
    
    input_width = 10 
    forecast_horizon = 5

    # 2. Accumulate Data
    list_animals = get_list_animals(file_rawdata, file_columns)
    print(f"Found {len(list_animals)} animals: {list_animals}")
    
    X_train_list, y_train_list = [], []
    X_val_list, y_val_list = [], []
    
    global_scaler = None
    global_metadata = None
    
    # We need a unified scaler. 
    # Option A: Fit scaler on ALL concatenated deltas (Memory intensive?).
    # Option B: Use incremental fitting (StandardScaler supports partial_fit).
    # Option C: Fit independent scalers per animal? NO, global model needs global normalization params.
    # We will go with Option B: Partial Fit.
    
    from sklearn.preprocessing import StandardScaler
    global_scaler = StandardScaler()
    
    # Phase 1: Collect Data and Partial Fit Scaler
    print(">>> Phase 1: Loading Data and Fitting Scaler...")
    
    valid_animals_count = 0
    animal_data_cache = {} # Cache pre-split, non-scaled data
    
    for animal_id in list_animals:
        results_dir = results_folder(file_rawdata)
        file_path = os.path.join(results_dir, f'map_{animal_id}.csv')
        
        if not os.path.exists(file_path):
            continue
            
        try:
            df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
            
            # Use preprocess to get UN-SCALED windows if possible?
            # preprocess_nbeats_data applies scaling internally.
            # We need to modify preprocess or refactor it.
            # Actually, `preprocess_nbeats_data` in `nbeat_data_prep.py` mixes everything (prep + split + scale).
            # We strictly need to modify it or handle scaling ourselves.
            # Let's import the internals or hack it.
            
            # Simpler approach for now:
            # Let `preprocess_nbeats_data` do its thing, BUT we ignore its scaler and inverse transform X/y to get raw deltas.
            # Then we re-scale globally. 
            # This is inefficient but safe without changing `nbeat_data_prep.py` heavily.
            
            xt, yt, xv, yv, xtest, ytest, scaler_local, meta = preprocess_nbeats_data(
                 df, file_columns, input_width, forecast_horizon, verbose=False
            )
            
            if xt is None:
                continue

            # Inverse Transform to get Raw Deltas
            # shapes: (N, T, F) -> (N*T, F)
            F = xt.shape[2]
            
            def inverse(tensor, scl):
                if tensor is None: return None
                shape = tensor.shape
                flat = tensor.reshape(-1, F)
                inv = scl.inverse_transform(flat)
                return inv.reshape(shape)

            xt_raw = inverse(xt, scaler_local)
            yt_raw = inverse(yt, scaler_local)
            xv_raw = inverse(xv, scaler_local)
            yv_raw = inverse(yv, scaler_local)
            
            animal_data_cache[animal_id] = (xt_raw, yt_raw, xv_raw, yv_raw)
            
            # Partial Fit on TRAIN data
            flat_train = xt_raw.reshape(-1, F)
            global_scaler.partial_fit(flat_train)
            
            valid_animals_count += 1
            if not global_metadata:
                 global_metadata = meta # Copy first valid metadata structure
                 
        except Exception as e:
            print(f"Skipping {animal_id}: {e}")
            continue

    if valid_animals_count == 0:
        print("No valid data found for any animal.")
        return

    print(f"Collected data from {valid_animals_count} animals. Global Scaler fitted.")
    
    # Phase 2: Transform and Accumulate
    print(">>> Phase 2: Transforming and Stacking Data...")
    
    for animal_id, (xtr, ytr, xvr, yvr) in animal_data_cache.items():
        F = xtr.shape[2]
        
        def transform(arr):
             if arr is None: return None
             shape = arr.shape
             flat = arr.reshape(-1, F)
             sc = global_scaler.transform(flat)
             return sc.reshape(shape)
             
        X_train_list.append(transform(xtr))
        y_train_list.append(transform(ytr))
        
        if xvr is not None:
             X_val_list.append(transform(xvr))
             y_val_list.append(transform(yvr))

    X_train_global = np.concatenate(X_train_list, axis=0)
    y_train_global = np.concatenate(y_train_list, axis=0)
    
    X_val_global = np.concatenate(X_val_list, axis=0) if X_val_list else None
    y_val_global = np.concatenate(y_val_list, axis=0) if y_val_list else None
    
    print(f"Global Train Size: {X_train_global.shape}")
    if X_val_global is not None:
        print(f"Global Val Size:   {X_val_global.shape}")

    # 3. Helpers
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")
    
    # Update Metadata
    global_metadata['hidden_dim'] = int(hidden_dim)
    global_metadata['num_blocks'] = int(num_blocks)
    
    # 4. Tensors & Loaders
    X_train_t = torch.tensor(X_train_global, dtype=torch.float32)
    y_train_t = torch.tensor(y_train_global, dtype=torch.float32)
    train_loader = DataLoader(TensorDataset(X_train_t, y_train_t), batch_size=batch_size, shuffle=True)
    
    val_loader = None
    if X_val_global is not None:
        X_val_t = torch.tensor(X_val_global, dtype=torch.float32)
        y_val_t = torch.tensor(y_val_global, dtype=torch.float32)
        val_loader = DataLoader(TensorDataset(X_val_t, y_val_t), batch_size=batch_size, shuffle=False)

    # 5. Model
    model = NBeats(
        input_steps=input_width,
        output_steps=forecast_horizon,
        input_features=2,
        hidden_dim=hidden_dim,
        num_blocks=num_blocks
    ).to(device)
    
    # 6. Loss
    scaler_mean = global_scaler.mean_
    scaler_std = global_scaler.scale_
    
    animal_name = os.path.basename(file_rawdata).lower()
    median_seconds = global_metadata['median_delta_seconds']
    
    # Heuristic Speed Limits
    max_speed_mps = 1.67 
    if "jaguar" in animal_name:
        max_speed_mps = 0.07 
    elif "tangara" in animal_name:
        max_speed_mps = 8 
    
    step_limit_meters = max_speed_mps * median_seconds
    print(f"[Loss] Speed limit set to {step_limit_meters:.2f} meters per step.")

    criterion = PhysicsInformedLoss(
        mean=scaler_mean, 
        std=scaler_std, 
        speed_limit_meters=step_limit_meters,
        penalty_weight=0.1,
        device=device
    )
    
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, betas=(0.9, 0.999), weight_decay=1e-5)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, factor=0.5, patience=20, min_lr=1e-6
    )

    # 7. Training
    models_dir = os.path.join(script_dir, 'models')
    save_path_model = os.path.join(models_dir, f'nbeats_model_global.pth')
    save_path_scaler = os.path.join(models_dir, f'scaler_global.pkl')
    
    history, best_val, best_epoch = train_model(
        model, train_loader, val_loader, criterion, optimizer, scheduler, device, 
        epochs, 20, save_path_model, save_path_scaler, global_scaler, global_metadata
    )
    
    # Save History
    with open(os.path.join(models_dir, f'nbeats_history_global.json'), 'w') as f:
        json.dump(history, f, indent=2)

    print(f"✅ Finished Global Training. Best Val: {best_val:.5f} @ Ep {best_epoch}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python run_nbeats_global.py <file_rawdata> <file_columns>")
    else:
        train_nbeats_global(sys.argv[1], sys.argv[2])
