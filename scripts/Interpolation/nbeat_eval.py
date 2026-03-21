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
from Evaluation.metrics import calculate_mae, calculate_mse, calculate_rmse
from Evaluation.biological_metrics import compute_all_biological_metrics


def sanitize_for_json(obj):
    """Recursively convert numpy types to JSON-serializable formats."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.generic):
        return obj.item()
    return obj

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
    mse = calculate_mse(y_truth_deltas, y_pred_deltas)
    rmse = calculate_rmse(y_truth_deltas, y_pred_deltas)
    mae = calculate_mae(y_truth_deltas, y_pred_deltas)
    
    print(f"[EVAL] Delta Metrics - RMSE: {rmse:.4f}, MAE: {mae:.4f}")
    
    diff = y_pred_deltas - y_truth_deltas # (B, H, 2)
    dist_per_step = np.sqrt(diff[:,:,0]**2 + diff[:,:,1]**2) # (B, H)
    ade = np.mean(dist_per_step)
    
    sum_pred = np.sum(y_pred_deltas, axis=1) # (B, 2)
    sum_true = np.sum(y_truth_deltas, axis=1) # (B, 2)
    fde_diff = sum_pred - sum_true
    fde = np.mean(np.sqrt(fde_diff[:,0]**2 + fde_diff[:,1]**2))
    
    print(f"[EVAL] Trajectory Metrics (Meters) - ADE: {ade:.4f}, FDE: {fde:.4f}")
    
    # Save basic Metrics
    metrics = {
        'rmse_deltas': float(rmse),
        'mae_deltas': float(mae),
        'ade_meters': float(ade),
        'fde_meters': float(fde)
    }

    # Attempt to compute biological metrics by reconstructing absolute trajectories
    try:
        # metadata should include 'utm_epsg', 'freq_str', 'median_delta_seconds', 'input_width', 'forecast_horizon'
        epsg = metadata.get('utm_epsg') if metadata else None
        freq_str = metadata.get('freq_str') if metadata else None
        median_delta_seconds = metadata.get('median_delta_seconds', None) if metadata else None
        input_width = metadata.get('input_width', None) if metadata else None
        forecast_horizon = metadata.get('forecast_horizon', None) if metadata else None

        if epsg is not None and freq_str is not None and input_width is not None:
            # Project original lon/lat to UTM and resample
            transformer_to_utm = Transformer.from_crs('EPSG:4326', f'EPSG:{int(epsg)}', always_xy=True)
            transformer_to_wgs = Transformer.from_crs(f'EPSG:{int(epsg)}', 'EPSG:4326', always_xy=True)

            df_local = df.copy()
            df_local['Timestamp'] = pd.to_datetime(df_local['Timestamp'], errors='coerce')
            df_local = df_local.dropna(subset=['Timestamp']).sort_values('Timestamp').reset_index(drop=True)
            e, n = transformer_to_utm.transform(df_local['Longitude'].values, df_local['Latitude'].values)
            df_local['E'] = e
            df_local['N'] = n
            df_local = df_local.set_index('Timestamp')

            df_resampled = df_local[['E', 'N']].resample(freq_str).mean()

            # Recompute deltas and valid segments (match preprocessing)
            df_resampled['Delta_E'] = df_resampled['E'].diff()
            df_resampled['Delta_N'] = df_resampled['N'].diff()
            valid_mask = df_resampled['E'].notna() & df_resampled['N'].notna()
            df_resampled['segment_id'] = (valid_mask != valid_mask.shift()).cumsum()
            df_valid = df_resampled[valid_mask].copy()
            df_valid['Delta_E'] = df_valid.groupby('segment_id')['E'].diff()
            df_valid['Delta_N'] = df_valid.groupby('segment_id')['N'].diff()
            df_valid = df_valid.dropna(subset=['Delta_E', 'Delta_N']).reset_index(drop=True)

            # Build windows to find last validation end point used for single-start forecasting
            data_values = df_valid[['Delta_E', 'Delta_N']].values.astype(np.float32)
            total_window_size = input_width + (forecast_horizon or 1)
            num_windows = len(data_values) - total_window_size + 1
            if num_windows > 0:
                i_train = int(num_windows * 0.70)
                i_val = int(num_windows * 0.85)

                # Determine start absolute position: end of last validation input window
                last_val_window_idx = max(0, i_val - 1)
                end_idx = last_val_window_idx + input_width - 1
                if end_idx < len(df_valid):
                    start_pos_E = float(df_valid['E'].iloc[end_idx])
                    start_pos_N = float(df_valid['N'].iloc[end_idx])

                    # Seed input: the input window deltas (raw, not scaled)
                    X_all = np.array([data_values[i:i+input_width] for i in range(num_windows)])
                    seed_input = X_all[last_val_window_idx].copy()  # shape (input_width, 2)

                    # Iteratively predict next steps using the trained model
                    model.to('cpu')
                    model.eval()
                    preds_E = []
                    preds_N = []

                    curr_input = seed_input.copy()
                    n_forecast_steps = len(df_valid) - (end_idx + 1)
                    for _ in range(n_forecast_steps):
                        # scale current input
                        cur_scaled = scaler.transform(curr_input.reshape(-1, 2)).reshape(1, input_width, -1)
                        cur_tensor = torch.tensor(cur_scaled, dtype=torch.float32)
                        with torch.no_grad():
                            y_pred_scaled = model(cur_tensor).cpu().numpy()
                        # model outputs horizon steps; take first step
                        B, H, F = y_pred_scaled.shape
                        y_pred_flat = y_pred_scaled.reshape(-1, F)
                        y_pred_deltas = scaler.inverse_transform(y_pred_flat).reshape(B, H, F)
                        pred_step = y_pred_deltas[0, 0, :]

                        # update absolute position
                        start_pos_E += float(pred_step[0])
                        start_pos_N += float(pred_step[1])
                        preds_E.append(start_pos_E)
                        preds_N.append(start_pos_N)

                        # shift input window and append predicted delta
                        curr_input = np.vstack([curr_input[1:], pred_step])

                    # True positions for the same range
                    true_slice = df_valid[['E', 'N']].iloc[end_idx+1:end_idx+1+len(preds_E)]
                    if len(preds_E) > 0 and len(true_slice) == len(preds_E):
                        lons_true, lats_true = transformer_to_wgs.transform(true_slice['E'].values, true_slice['N'].values)
                        lons_pred, lats_pred = transformer_to_wgs.transform(np.array(preds_E), np.array(preds_N))

                        # Build time arrays (seconds) if median_delta_seconds available
                        if median_delta_seconds is None:
                            times = np.arange(len(lons_true))
                        else:
                            times = np.arange(len(lons_true)) * float(median_delta_seconds)

                        bio = compute_all_biological_metrics(
                            lons_true, lats_true, times,
                            lons_pred, lats_pred, times,
                            species='jaguar'
                        )
                        metrics['biological_metrics'] = bio
    except Exception as e:
        # do not fail evaluation if bio metrics fail
        print(f"Warning: could not compute biological metrics for {current_animal}: {e}")

    # persist metrics
    results_dir = results_folder(file_rawdata_name)
    metrics_path = os.path.join(results_dir, 'Interpolation', f'metrics_nbeats_{current_animal}.json')
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    with open(metrics_path, 'w') as f:
        json.dump(sanitize_for_json(metrics), f, indent=2)

    print(f"✅ Metrics saved to {metrics_path}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 4:
        evaluate_nbeats_model(sys.argv[1], sys.argv[2], sys.argv[3])
