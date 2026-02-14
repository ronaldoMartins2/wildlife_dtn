import os
import json
import math
import joblib
import pandas as pd
import numpy as np
from pyproj import Transformer

import torch
from Interpolation.nbeat_model import NBeats
from Common.utils import read_field_from_json
from Evaluation.metrics import (
    calculate_mae, calculate_mse, calculate_rmse, 
    calculate_mape, calculate_smape, calculate_mase, calculate_owa
)


def haversine(lon1, lat1, lon2, lat2):
    # returns meters
    R = 6371000.0
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c


def predict_n_steps_from_obs(obs_row, n_steps, model, scaler_X, scaler_y, transformer_to_utm, transformer_to_wgs, median_time_diff=0.5, device='cpu'):
    curr_lon = float(obs_row['Longitude'])
    curr_lat = float(obs_row['Latitude'])
    curr_e, curr_n = transformer_to_utm.transform(curr_lon, curr_lat)

    prev_time_diff = median_time_diff
    preds = []

    model.to(device)
    model.eval()
    for _ in range(n_steps):
        input_feat = np.array([[prev_time_diff, curr_e, curr_n]], dtype=np.float32)
        input_scaled = scaler_X.transform(input_feat)
        input_tensor = torch.tensor(input_scaled, dtype=torch.float32).to(device)
        with torch.no_grad():
            pred_scaled = model(input_tensor).cpu().numpy()
        pred = scaler_y.inverse_transform(pred_scaled).flatten()
        pred_time = float(pred[0])
        pred_e = float(pred[1])
        pred_n = float(pred[2])
        if pred_time <= 0.01:
            pred_time = median_time_diff

        curr_e = curr_e + pred_e
        curr_n = curr_n + pred_n
        curr_lon, curr_lat = transformer_to_wgs.transform(curr_e, curr_n)
        preds.append({'Longitude': curr_lon, 'Latitude': curr_lat, 'TimeDiff': pred_time})
        prev_time_diff = pred_time

    return preds


def evaluate(file_map_path, file_rawdata_name, file_rawdata_columns, model_dir=None, horizons=[1,5,10]):
    # infer filename key
    filename = os.path.basename(file_rawdata_name).split('.')[0]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = model_dir or os.path.join(script_dir, 'models')

    model_path = os.path.join(models_dir, f'nbeats_model_general_{filename}.pth')
    scaler_x_path = os.path.join(models_dir, f'scaler_x_{filename}.pkl')
    scaler_y_path = os.path.join(models_dir, f'scaler_y_{filename}.pkl')

    if not os.path.exists(model_path) or not os.path.exists(scaler_x_path) or not os.path.exists(scaler_y_path):
        raise FileNotFoundError('Model or scalers not found for ' + filename)

    # Load model and scalers
    scaler_X = joblib.load(scaler_x_path)
    scaler_y = joblib.load(scaler_y_path)
    # get hyperparams
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    hidden_dim = read_field_from_json(hyperparam_path, 'hidden_dim_nbeat') or 64
    num_blocks = read_field_from_json(hyperparam_path, 'num_blocks_nbeat') or 2

    # load map csv
    df = pd.read_csv(file_map_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    df = df.dropna(subset=['Timestamp']).sort_values('Timestamp').reset_index(drop=True)

    # splits
    from Common.utils import TRAINNING_SET, VALIDATION_SET, TESTING_SET
    n = len(df)
    i_train = int(TRAINNING_SET * n)
    i_val = i_train + int(VALIDATION_SET * n)
    df_t = df.iloc[:i_train].copy()
    df_v = df.iloc[i_train:i_val].copy()
    df_te = df.iloc[i_val:].copy()

    # load model (need input/output dims)
    # infer dims from scaler shapes
    input_dim = scaler_X.mean_.shape[0]
    output_dim = scaler_y.mean_.shape[0]
    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks)
    model.load_state_dict(torch.load(model_path, map_location='cpu')['model_state_dict'])

    # determine UTM
    utm_epsg = None
    # try metadata
    meta_path = os.path.join(models_dir, f'nbeats_metadata_{filename}.json')
    if os.path.exists(meta_path):
        with open(meta_path, 'r') as fh:
            meta = json.load(fh)
            utm_epsg = meta.get('utm_epsg')

    if utm_epsg is None:
        median_lon = df['Longitude'].median()
        median_lat = df['Latitude'].median()
        try:
            zone = int((median_lon + 180) / 6) + 1
        except Exception:
            zone = 23
        is_northern = True if median_lat >= 0 else False
        utm_epsg = 32600 + zone if is_northern else 32700 + zone

    transformer_to_utm = Transformer.from_crs('EPSG:4326', f'EPSG:{int(utm_epsg)}', always_xy=True)
    transformer_to_wgs = Transformer.from_crs(f'EPSG:{int(utm_epsg)}', 'EPSG:4326', always_xy=True)

    # median time diff from training for initial prev_time
    df['TimeDiff'] = df['Timestamp'].diff().dt.total_seconds() / 3600.0
    median_time_diff = float(df['TimeDiff'].median() or 0.5)

    n_test = len(df_te)
    results = {'horizons': {}, 'n_test': n_test}

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    for h in horizons:
        true_lons = []
        true_lats = []
        pred_lons = []
        pred_lats = []
        naive_lons = []
        naive_lats = []

        # sliding windows over test
        for s in range(0, n_test - h + 1):
            if s == 0:
                start_row = df_v.iloc[-1] if len(df_v)>0 else df_t.iloc[-1]
            else:
                start_row = df_te.iloc[s-1]

            preds = predict_n_steps_from_obs(start_row, h, model, scaler_X, scaler_y, transformer_to_utm, transformer_to_wgs, median_time_diff, device=device)
            pred_pos = preds[-1]
            actual = df_te.iloc[s + h - 1]
            
            # Naive forecast: last observation carried forward
            naive_lon = float(start_row['Longitude'])
            naive_lat = float(start_row['Latitude'])

            dist = haversine(pred_pos['Longitude'], pred_pos['Latitude'], float(actual['Longitude']), float(actual['Latitude']))
            dists.append(dist)
            
            true_lons.append(float(actual['Longitude']))
            true_lats.append(float(actual['Latitude']))
            pred_lons.append(pred_pos['Longitude'])
            pred_lats.append(pred_pos['Latitude'])
            naive_lons.append(naive_lon)
            naive_lats.append(naive_lat)

        dists = np.array(dists)
        
        # Calculate extended metrics for Lat and Lon
        metrics_lat = {}
        metrics_lon = {}
        metrics_lat_naive = {}
        metrics_lon_naive = {}
        
        if len(dists) > 0:
            # Training data for MASE (Lat/Lon)
            train_lons = df_t['Longitude'].values
            train_lats = df_t['Latitude'].values

            # --- Latitude Metrics ---
            metrics_lat['MAE'] = calculate_mae(true_lats, pred_lats)
            metrics_lat['MSE'] = calculate_mse(true_lats, pred_lats)
            metrics_lat['RMSE'] = calculate_rmse(true_lats, pred_lats)
            metrics_lat['MAPE'] = calculate_mape(true_lats, pred_lats)
            metrics_lat['SMAPE'] = calculate_smape(true_lats, pred_lats)
            metrics_lat['MASE'] = calculate_mase(true_lats, pred_lats, train_lats)

            # Naive Latitude Metrics
            metrics_lat_naive['SMAPE'] = calculate_smape(true_lats, naive_lats)
            metrics_lat_naive['MASE'] = calculate_mase(true_lats, naive_lats, train_lats)
            
            metrics_lat['OWA'] = calculate_owa(metrics_lat, metrics_lat_naive)

            # --- Longitude Metrics ---
            metrics_lon['MAE'] = calculate_mae(true_lons, pred_lons)
            metrics_lon['MSE'] = calculate_mse(true_lons, pred_lons)
            metrics_lon['RMSE'] = calculate_rmse(true_lons, pred_lons)
            metrics_lon['MAPE'] = calculate_mape(true_lons, pred_lons)
            metrics_lon['SMAPE'] = calculate_smape(true_lons, pred_lons)
            metrics_lon['MASE'] = calculate_mase(true_lons, pred_lons, train_lons)

            # Naive Longitude Metrics
            metrics_lon_naive['SMAPE'] = calculate_smape(true_lons, naive_lons)
            metrics_lon_naive['MASE'] = calculate_mase(true_lons, naive_lons, train_lons)
            
            metrics_lon['OWA'] = calculate_owa(metrics_lon, metrics_lon_naive)

        results['horizons'][str(h)] = {
            'count': int(len(dists)),
            'mae_m': float(np.mean(np.abs(dists))) if len(dists)>0 else None,
            'rmse_m': float(np.sqrt(np.mean(dists**2))) if len(dists)>0 else None,
            'median_m': float(np.median(dists)) if len(dists)>0 else None,
            'metrics_lat': metrics_lat,
            'metrics_lon': metrics_lon
        }
        
        if len(dists) > 0:
            l,u = bootstrap_ci(dists, n_boot=1000, ci=95)
            results['horizons'][str(h)]['mae_ci95'] = [l,u]
        else:
            results['horizons'][str(h)]['mae_ci95'] = [None, None]

    # ADE/FDE from single-start (start at end of validation)
    if n_test>0:
        start_row = df_v.iloc[-1] if len(df_v)>0 else df_t.iloc[-1]
        preds_full = predict_n_steps_from_obs(start_row, n_test, model, scaler_X, scaler_y, transformer_to_utm, transformer_to_wgs, median_time_diff, device=device)
        dists_full = []
        for i, p in enumerate(preds_full):
            actual = df_te.iloc[i]
            d = haversine(p['Longitude'], p['Latitude'], float(actual['Longitude']), float(actual['Latitude']))
            dists_full.append(d)
        dists_full = np.array(dists_full)
        results['ADE_m'] = float(np.mean(dists_full))
        results['FDE_m'] = float(dists_full[-1])
    else:
        results['ADE_m'] = None
        results['FDE_m'] = None

    # compute bootstrap CIs for horizons
    def bootstrap_ci(arr, n_boot=1000, ci=95):
        if len(arr) == 0:
            return (None, None)
        boot_stats = []
        for _ in range(n_boot):
            sample = np.random.choice(arr, size=len(arr), replace=True)
            boot_stats.append(np.mean(sample))
        lower = np.percentile(boot_stats, (100-ci)/2)
        upper = np.percentile(boot_stats, 100 - (100-ci)/2)
        return float(lower), float(upper)

    for h_str, stats in results['horizons'].items():
        h = int(h_str)
        # recompute dists for horizon to perform bootstrap
        dists = []
        for s in range(0, n_test - h + 1):
            if s == 0:
                start_row = df_v.iloc[-1] if len(df_v)>0 else df_t.iloc[-1]
            else:
                start_row = df_te.iloc[s-1]
            preds = predict_n_steps_from_obs(start_row, h, model, scaler_X, scaler_y, transformer_to_utm, transformer_to_wgs, median_time_diff, device=device)
            pred_pos = preds[-1]
            actual = df_te.iloc[s + h - 1]
            dist = haversine(pred_pos['Longitude'], pred_pos['Latitude'], float(actual['Longitude']), float(actual['Latitude']))
            dists.append(dist)
        dists = np.array(dists)
        if len(dists) > 0:
            l,u = bootstrap_ci(dists, n_boot=1000, ci=95)
            results['horizons'][h_str]['mae_ci95'] = [l,u]
        else:
            results['horizons'][h_str]['mae_ci95'] = [None, None]

    # save results JSON and CSV per-horizon
    out_path = os.path.join(models_dir, f'nbeats_eval_{filename}.json')
    with open(out_path, 'w') as fh:
        json.dump(results, fh, indent=2)

    # CSV summary per horizon
    rows = []
    for h_str, stats in results['horizons'].items():
        rows.append({
            'filename': filename,
            'horizon': int(h_str),
            'count': stats.get('count'),
            'mae_m': stats.get('mae_m'),
            'rmse_m': stats.get('rmse_m'),
            'median_m': stats.get('median_m'),
            'mae_ci95_lower': (stats.get('mae_ci95') or [None, None])[0],
            'mae_ci95_upper': (stats.get('mae_ci95') or [None, None])[1]
        })
        
        # Flatten metrics for CSV
        m_lat = stats.get('metrics_lat', {})
        m_lon = stats.get('metrics_lon', {})
        
        for k, v in m_lat.items():
            rows[-1][f'lat_{k}'] = v
        for k, v in m_lon.items():
            rows[-1][f'lon_{k}'] = v
    df_out = pd.DataFrame(rows)
    csv_out = os.path.join(models_dir, f'nbeats_eval_summary_{filename}.csv')
    df_out.to_csv(csv_out, index=False)

    return results


if __name__ == '__main__':
    import sys
    # usage: python evaluate_nbeats.py <map_csv_path> <rawdata_filename> <rawdata_columns_json>
    if len(sys.argv) < 4:
        print('Usage: evaluate_nbeats.py <map_csv_path> <rawdata_filename> <rawdata_columns_json>')
        raise SystemExit(1)
    map_path = sys.argv[1]
    rawdata = sys.argv[2]
    rawcols = sys.argv[3]
    res = evaluate(map_path, rawdata, rawcols)
    print('Evaluation results:')
    print(json.dumps(res, indent=2))
