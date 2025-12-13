import os
import json
import torch
import numpy as np
import pandas as pd
import joblib
from pyproj import Transformer
from math import radians, cos, sin, asin, sqrt

from Common.utils import results_folder
from Interpolation.nbeat_model import NBeats
from Interpolation.nbeat_data_prep import preprocess_nbeats_data

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 * 1000 # Radius of earth in meters
    return c * r

def evaluate_nbeats_model(current_animal, file_rawdata_name, file_rawdata_columns):
    print(f"[EVAL] Evaluating {current_animal}...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = os.path.join(script_dir, 'models')
    
    # Load Model & Scaler & Metadata
    model_path = os.path.join(models_dir, f'nbeats_model_{current_animal}.pth')
    if not os.path.exists(model_path):
        print("Model not found.")
        return
        
    checkpoint = torch.load(model_path)
    metadata = checkpoint['metadata']
    model_state = checkpoint['model_state_dict']
    
    scaler = joblib.load(os.path.join(models_dir, f'scaler_{current_animal}.pkl'))
    
    # Load Data (Test Set)
    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    try:
        df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    except:
        return

    # Reprocess to get Test Split
    _, _, _, _, X_test, y_test, _, _ = preprocess_nbeats_data(
        df, file_rawdata_columns, 
        input_width=metadata['input_width'], 
        forecast_horizon=metadata['forecast_horizon'],
        verbose=False
    )
    
    if X_test is None or len(X_test) == 0:
        print("No test data available.")
        return

    # Prepare Device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = NBeats(
        input_steps=metadata['input_width'],
        output_steps=metadata['forecast_horizon'],
        input_features=len(metadata['feature_names']),
        hidden_dim=metadata.get('hidden_dim', 64), 
        num_blocks=metadata.get('num_blocks', 2)
    ).to(device)
    model.load_state_dict(model_state)
    model.eval()

    # Inference
    X_test_t = torch.tensor(X_test, dtype=torch.float32).to(device)
    with torch.no_grad():
        y_pred_scaled = model(X_test_t).cpu().numpy()
        
    # Inverse Transform
    # Reshape to (N*Horizon, Features) for scaler
    B, H, F = y_pred_scaled.shape
    y_pred_flat = y_pred_scaled.reshape(-1, F)
    y_truth_flat = y_test.reshape(-1, F)
    
    y_pred_deltas = scaler.inverse_transform(y_pred_flat).reshape(B, H, F)
    y_truth_deltas = scaler.inverse_transform(y_truth_flat).reshape(B, H, F)
    
    # Metrics Calculation
    # 1. Delta Errors (RMSE)
    mse = np.mean((y_pred_deltas - y_truth_deltas)**2)
    rmse = np.sqrt(mse)
    mae = np.mean(np.abs(y_pred_deltas - y_truth_deltas))
    
    print(f"[EVAL] Delta Metrics - RMSE: {rmse:.4f}, MAE: {mae:.4f}")
    
    # 2. Trajectory Reconstruction (ADE/FDE)
    # We need the "Previous" absolute coordinates to reconstruct.
    # But preprocess_nbeats_data stripped them. 
    # Limitation: We only have deltas.
    # Approximation: We can calculate the Euclidean error of the *Deltas* themselves, 
    # which corresponds to displacement error per step relative to "expected" step.
    
    # ADE: Average Displacement Error per step
    # Euclidean dist between predicted delta vector and true delta vector
    diff = y_pred_deltas - y_truth_deltas # (B, H, 2)
    dist_per_step = np.sqrt(diff[:,:,0]**2 + diff[:,:,1]**2) # (B, H)
    ade = np.mean(dist_per_step)
    
    # FDE: Final Displacement Error
    # To do this properly, we should accumulate deltas IF we wanted absolute position error.
    # However, since we define "Target" as the Delta for that step, the "Cumulative" error
    # is the vector sum of differences.
    # Vector Sum of Pred Deltas vs Vector Sum of True Deltas
    # Sum over Horizon
    sum_pred = np.sum(y_pred_deltas, axis=1) # (B, 2)
    sum_true = np.sum(y_truth_deltas, axis=1) # (B, 2)
    fde_diff = sum_pred - sum_true
    fde = np.mean(np.sqrt(fde_diff[:,0]**2 + fde_diff[:,1]**2))
    
    print(f"[EVAL] Trajectory Metrics (Meters) - ADE: {ade:.4f}, FDE: {fde:.4f}")
    
    # Save Metrics
    metrics = {
        'rmse_deltas': float(rmse),
        'mae_deltas': float(mae),
        'ade_meters': float(ade),
        'fde_meters': float(fde)
    }
    
    results_dir = results_folder(file_rawdata_name)
    metrics_path = os.path.join(results_dir, 'Interpolation', f'metrics_nbeats_{current_animal}.json')
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
        
    print(f"✅ Metrics saved to {metrics_path}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 4:
        evaluate_nbeats_model(sys.argv[1], sys.argv[2], sys.argv[3])
