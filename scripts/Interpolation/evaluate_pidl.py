import os
import sys
import numpy as np
import pandas as pd
import torch
import json
from datetime import timedelta

# Add scripts directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Common.utils import results_folder, get_list_animals, get_id_from_json, TRAINNING_SET, VALIDATION_SET, TESTING_SET
from Data_preparation.data_field import DataField
from Interpolation.pidl_interpolation import BFBiLSTM, get_utm_zone, project_to_utm, interpolate_session
from Evaluation.metrics import (
    calculate_mae, calculate_mse, calculate_rmse, 
    calculate_mape, calculate_smape, calculate_mase, calculate_owa
)

def evaluate_pidl_animal(current_animal, file_rawdata, file_rawdata_columns, model, device, epsg):
    print(f"Evaluating PIDL for {current_animal}...")
    results_dir = results_folder(file_rawdata)
    input_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        print(f"Skipping {current_animal}: No data.")
        return None

    # Load Data
    try:
        df = pd.read_csv(input_path, header=None, names=['ID', 'timestamp', 'Longitude', 'Latitude'])
    except Exception as e:
        print(f"Error reading {current_animal}: {e}")
        return None

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Split Data (Train/Test)
    n = len(df)
    i_train = int(TRAINNING_SET * n)
    i_val = i_train + int(VALIDATION_SET * n)
    
    df_train = df.iloc[:i_train].copy()
    df_test = df.iloc[i_val:].copy()
    
    if len(df_test) < 10:
        print("Test set too small.")
        return None
        
    # Project Test Data to UTM
    df_test, transformer = project_to_utm(df_test, epsg)
    # Project Train Data for MASE denominator calculation
    df_train, _ = project_to_utm(df_train, epsg)

    # Prepare Evaluation: Randomly mask points in Test set to simulate gaps
    # We will use "Imputation" evaluation: Mask X% and predict them.
    mask_ratio = 0.20 # Mask 20% of the points
    np.random.seed(42) # Reproducibility
    
    # We need to process by session if there are large gaps, but for evaluation simplified view:
    # We'll treat the test set as one session (or split if huge gaps exist)
    # For simplicity, we just take the valid points, create a masked copy, interpolate, and compare.
    
    # Resampling is part of PIDL logic. 
    # To get grounded truth, we should rely on the timestamps present in Test.
    # PIDL `interpolate_session` resamples to a fixed freq.
    # If we pass the original test df, it will resample. 
    # This aligns predictions to a grid, which might not match exact original timestamps if they were irregular.
    
    # Strategy:
    # 1. Resample Test DF to a fixed grid (Logic from `interpolate_session`).
    # 2. Assume this resampled grid is the "Ground Truth" (linear interp of original).
    # 3. Mask random points in this grid.
    # 4. Predict.
    # 5. Compare.
    
    # Re-implement resampling logic locally to get the "Ground Truth" grid
    times = df_test['timestamp']
    diffs = times.diff().dt.total_seconds().dropna()
    median_dt = diffs.median()
    if median_dt > 3600 * 5: freq = '6h'
    elif median_dt < 60: freq = '1min'
    else: freq = f"{int(median_dt)}s"
    
    df_session = df_test.set_index('timestamp')
    # Ground Truth on Grid (Linear Interpolation to fill grid)
    df_res = df_session[['pos_x', 'pos_y']].resample(freq).mean()
    # Fill small gaps with linear for Ground Truth approximation?
    # Actually, we should only evaluate on points that were ORIGINALLY present (or close to).
    # But `interpolate_session` outputs the *whole* grid.
    
    # Let's perform masking on the `df_res` (Grid).
    # First, let's keep only grid points that had data (or fill assuming linear is truth for small steps).
    # Simplest: Linear interpolate `df_res` to get full ground truth trajectories.
    df_gt = df_res.interpolate(method='linear')
    
    # Generate Mask
    n_points = len(df_gt)
    n_mask = int(n_points * mask_ratio)
    mask_indices = np.random.choice(n_points, n_mask, replace=False)
    
    # Create Input DF with missing values at mask_indices
    df_input = df_gt.copy().reset_index()
    # Set masked points to NaN (Simulate Gaps)
    # Note: `interpolate_session` expects NaNs in 'pos_x'/'pos_y' to treat them as gaps.
    df_input.loc[mask_indices, 'pos_x'] = np.nan
    df_input.loc[mask_indices, 'pos_y'] = np.nan
    
    # Run PIDL
    df_pred_pidl = interpolate_session(model, df_input, gap_threshold_min=0)
    
    # Run Naive Baseline (Linear Interpolation) for OWA/Comparison
    # Since df_input already has NaNs, pandas interpolate('linear') does exactly this.
    df_pred_naive = df_input.set_index('timestamp').interpolate(method='linear').reset_index()
    # Fill remaining NaNs (edges) with ffill/bfill
    df_pred_naive = df_pred_naive.ffill().bfill()
    
    # Extract Ground Truth and Predictions at masked indices
    gt_x = df_gt.iloc[mask_indices]['pos_x'].values
    gt_y = df_gt.iloc[mask_indices]['pos_y'].values
    
    pred_pidl_x = df_pred_pidl.iloc[mask_indices]['pos_x'].values
    pred_pidl_y = df_pred_pidl.iloc[mask_indices]['pos_y'].values
    
    pred_naive_x = df_pred_naive.iloc[mask_indices]['pos_x'].values
    pred_naive_y = df_pred_naive.iloc[mask_indices]['pos_y'].values
    
    # Transform back to Lat/Lon for physical metrics (Meters/Degrees)
    # We use the transformer from earlier
    # transformer definition needed. 
    # Use existing `project_back` logic or direct calls.
    # We need to recreate transformer or return it from project_to_utm.
    # Updating project_to_utm to return transformer.
    
    # Use a fresh transformer to be safe
    transformer_back = lambda x, y: transformer.transform(x, y, direction='INVERSE') # Not supported by pyproj object directly this way usually?
    # The `project_to_utm` returned a transformer that goes Lat/Lon -> UTM.
    # Inverse:
    lons_gt, lats_gt = transformer.transform(gt_x, gt_y, direction='INVERSE')
    lons_pred, lats_pred = transformer.transform(pred_pidl_x, pred_pidl_y, direction='INVERSE')
    lons_naive, lats_naive = transformer.transform(pred_naive_x, pred_naive_y, direction='INVERSE')
    
    # Train Data (Converted to Lat/Lon)
    train_x = df_train['pos_x'].values
    train_y = df_train['pos_y'].values
    lons_train, lats_train = transformer.transform(train_x, train_y, direction='INVERSE')

    # Calculate Metrics
    metrics_lat = {}
    metrics_lon = {}
    metrics_lat_naive = {}
    metrics_lon_naive = {}

    # Latitude
    metrics_lat['MAE'] = calculate_mae(lats_gt, lats_pred)
    metrics_lat['MSE'] = calculate_mse(lats_gt, lats_pred)
    metrics_lat['RMSE'] = calculate_rmse(lats_gt, lats_pred)
    metrics_lat['MAPE'] = calculate_mape(lats_gt, lats_pred)
    metrics_lat['SMAPE'] = calculate_smape(lats_gt, lats_pred)
    metrics_lat['MASE'] = calculate_mase(lats_gt, lats_pred, lats_train)
    
    # Naive Latitude
    metrics_lat_naive['SMAPE'] = calculate_smape(lats_gt, lats_naive)
    metrics_lat_naive['MASE'] = calculate_mase(lats_gt, lats_naive, lats_train)
    
    metrics_lat['OWA'] = calculate_owa(metrics_lat, metrics_lat_naive)

    # Longitude
    metrics_lon['MAE'] = calculate_mae(lons_gt, lons_pred)
    metrics_lon['MSE'] = calculate_mse(lons_gt, lons_pred)
    metrics_lon['RMSE'] = calculate_rmse(lons_gt, lons_pred)
    metrics_lon['MAPE'] = calculate_mape(lons_gt, lons_pred)
    metrics_lon['SMAPE'] = calculate_smape(lons_gt, lons_pred)
    metrics_lon['MASE'] = calculate_mase(lons_gt, lons_pred, lons_train)
    
    # Naive Longitude
    metrics_lon_naive['SMAPE'] = calculate_smape(lons_gt, lons_naive)
    metrics_lon_naive['MASE'] = calculate_mase(lons_gt, lons_naive, lons_train)
    
    metrics_lon['OWA'] = calculate_owa(metrics_lon, metrics_lon_naive)
    
    return {
        'animal': current_animal,
        'metrics_lat': metrics_lat,
        'metrics_lon': metrics_lon
    }

def run_evaluation_all_pidl(file_rawdata, file_rawdata_columns):
    print("Starting PIDL Evaluation Pipeline...")
    results_dir = results_folder(file_rawdata)
    
    # 1. Detect Model
    if "jaguar" in file_rawdata.lower():
        model_name = "jaguar_model.pth"
        epsg, _ = get_utm_zone(file_rawdata)
    elif "tangara" in file_rawdata.lower():
        model_name = "tangara_model.pth"
        epsg, _ = get_utm_zone(file_rawdata)
    else:
        model_name = "tangara_model.pth"
        epsg = "EPSG:32723"

    model_path = os.path.join(os.path.dirname(__file__), '../Results/PIDL_Output', model_name)
    
    if not os.path.exists(model_path):
        print(f"Model not found at {model_path}. Cannot evaluate.")
        return

    # 2. Load Model
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = BFBiLSTM().to(device)
    model.load_state_dict(torch.load(model_path, map_location=device))
    model.eval()
    
    list_animals = get_list_animals(file_rawdata, file_rawdata_columns)
    
    all_results = []
    
    for animal in list_animals:
        res = evaluate_pidl_animal(animal, file_rawdata, file_rawdata_columns, model, device, epsg)
        if res:
            row = {'animal': res['animal']}
            for k, v in res['metrics_lat'].items(): row[f'lat_{k}'] = v
            for k, v in res['metrics_lon'].items(): row[f'lon_{k}'] = v
            all_results.append(row)
            
    if all_results:
        df_out = pd.DataFrame(all_results)
        base = os.path.basename(file_rawdata).split('.')[0]
        out_csv = os.path.join(os.path.dirname(model_path), f'pidl_eval_summary_{base}.csv')
        df_out.to_csv(out_csv, index=False)
        print(f"PIDL Evaluation saved to {out_csv}")
    else:
        print("No PIDL evaluation results generated.")

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        run_evaluation_all_pidl(sys.argv[1], sys.argv[2])
