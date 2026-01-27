import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import os
import sys
import pyproj
from datetime import timedelta

# Ensure scripts folder is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Common.utils import results_folder, get_list_animals, get_id_from_json
from Data_preparation.data_field import DataField

# --- Model Definition (Must match train_pidl.py) ---
class BFBiLSTM(nn.Module):
    def __init__(self, input_dim=4, hidden_dim=64):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, batch_first=True, bidirectional=True)
        self.fc = nn.Linear(hidden_dim * 2, 2)
        
    def forward(self, x):
        out, _ = self.lstm(x)
        pred = self.fc(out)
        return pred

# --- Utilities ---
def get_utm_zone(file_rawdata):
    # Heuristic based on filename or explicit logic
    if "jaguar" in file_rawdata.lower():
        return "EPSG:32720", 2.0 # UTM 20S, Max Vel ~1.5 m/s (relaxed to 2.0)
    elif "tangara" in file_rawdata.lower():
        return "EPSG:32723", 25.0 # UTM 23S, Max Vel ~20 m/s (relaxed)
    else:
        return "EPSG:32723", 10.0 # Default

def interpolate_session(model, df_session, gap_threshold_min=15):
    """
    Interpolate gaps in a session using the model.
    df_session: DataFrame with ['pos_x', 'pos_y', 'timestamp']
    """
    # Prepare data for model inference
    # We need to normalize based on the session stats (Mean Instance Norm strategy from training)
    
    coords = df_session[['pos_x', 'pos_y']].values.astype(np.float32)
    times = pd.to_datetime(df_session['timestamp'])
    
    # Calculate dt (time delta in seconds)
    # For training we used diff(); here we need to reconstruct the sequence.
    # Actually, the model takes (norm_x, norm_y, log_dt, mask)
    
    # Identify gaps.
    # In the pipeline, "gaps" are implicit between rows if time diff is large?
    # Or are we filling specific missing timestamps?
    # The N-BEATS pipeline resamples to a fixed frequency and fills NaNs.
    # We should do the same to be consistent.
    
    # Determing resampling frequency
    # Jaguar: ~6h (nominal), Tangara: Irregular.
    # If we want to produce a regular output, we must resample.
    # If the input is irregular, resampling might create MANY gaps.
    
    # Strategy: Resample to the median frequency of the data.
    diffs = times.diff().dt.total_seconds().dropna()
    median_dt = diffs.median()
    
    if median_dt > 3600 * 5: # > 5 hours -> Jaguar likely
        freq = '6h' 
    elif median_dt < 60: # High freq -> Tangara
        freq = '1min' # Tangara is bursty... 1min might be too fine if average is 20min.
    else:
        freq = f"{int(median_dt)}s"

    # Resample
    df_session = df_session.set_index('timestamp')
    
    # Retain first valid point per bin
    df_res = df_session[['pos_x', 'pos_y']].resample(freq).first()
    
    # Identify NaNs (Gaps)
    mask = df_res['pos_x'].isna()
    
    if not mask.any():
        return df_res.reset_index()
        
    # Prepare Input Tensor
    # We need to feed the ENTIRE sequence (with gaps masked) into the BiLSTM.
    # The model was trained with masked inputs = 0.
    
    coords_full = df_res[['pos_x', 'pos_y']].values.astype(np.float32) # Contains NaNs
    
    # Mean Instance Norm logic handles NaNs? numpy nanmean
    mean = np.nanmean(coords_full, axis=0)
    std = np.nanstd(coords_full, axis=0) + 1e-6
    
    coords_norm = (coords_full - mean) / std
    
    # Fill NaNs with 0 for input
    mask_np = np.isnan(coords_full[:, 0]).astype(np.float32)
    coords_in = np.nan_to_num(coords_norm, nan=0.0)
    
    # DT Calculation:
    # Since we resampled, dt is constant (approx). 
    # But wait, training used REAL dt between points.
    # Here dt is uniform due to resampling.
    # dt = seconds in 'freq'
    dt_val = pd.to_timedelta(freq).total_seconds()
    dt_seq = np.full((len(coords_full),), dt_val, dtype=np.float32)
    dt_log = np.log1p(dt_seq)
    
    # Tensor Construction
    # Input: [x, y, dt_log, mask]
    x_in_np = np.column_stack([coords_in, dt_log, mask_np])
    x_tensor = torch.tensor(x_in_np, dtype=torch.float32).unsqueeze(0) # Batch size 1
    
    # Inference
    device = next(model.parameters()).device
    x_tensor = x_tensor.to(device)
    
    with torch.no_grad():
        pred_norm = model(x_tensor).cpu().numpy().squeeze(0) # [L, 2]
        
    # Denormalize
    pred_real = pred_norm * std + mean
    
    # Fill gaps in the original structure
    # We only take predictions where mask == 1
    
    df_filled = df_res.copy()
    
    # Use loc to fill NaNs
    # Efficient assignment
    fill_indices = np.where(mask_np == 1)[0]
    
    if len(fill_indices) > 0:
        df_filled.iloc[fill_indices, 0] = pred_real[fill_indices, 0] # pos_x
        df_filled.iloc[fill_indices, 1] = pred_real[fill_indices, 1] # pos_y
    
    return df_filled.reset_index()

def project_to_utm(df, epsg):
    transformer = pyproj.Transformer.from_crs("EPSG:4326", epsg, always_xy=True)
    df['pos_x'], df['pos_y'] = transformer.transform(df['Longitude'].values, df['Latitude'].values)
    return df, transformer

def project_back(df, transformer):
    # Inverse transform
    # transformer direction is forward. We need a new one or use transform(..., direction=INVERSE) but pyproj simple API is easier
    # Actually transformer has definition.
    # Let's just make a reverse transformer to be safe and clear.
    source_crs = transformer.source_crs
    target_crs = transformer.target_crs
    reverse_trans = pyproj.Transformer.from_crs(target_crs, source_crs, always_xy=True)
    
    df['Longitude'], df['Latitude'] = reverse_trans.transform(df['pos_x'].values, df['pos_y'].values)
    return df

def run_single_pidl(current_animal, file_rawdata, model, device, epsg):
    results_dir = results_folder(file_rawdata)
    input_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    
    if not os.path.exists(input_path) or os.path.getsize(input_path) == 0:
        print(f"Skipping {current_animal}: No data.")
        return

    # Load Individual Data
    try:
        df = pd.read_csv(input_path, header=None, names=['ID', 'timestamp', 'Longitude', 'Latitude'])
    except Exception as e:
        print(f"Error reading {current_animal}: {e}")
        return

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Project
    df, transformer = project_to_utm(df, epsg)
    
    # Run Interpolation (Break by huge gaps if necessary? Model handles it via sessions)
    # Simpler approach: Treat whole file as one session if gaps aren't MASSIVE (like years).
    # Tangara has 18 month gap. We MUST split.
    
    df['dt'] = df['timestamp'].diff().dt.total_seconds().fillna(0)
    # Split sessions if gap > 1 day (Jaguar) or > 4 hours (Tangara) ??
    # Let's use a safe threshold of 24h.
    split_threshold = 24 * 3600
    df['session_id'] = (df['dt'] > split_threshold).cumsum()
    
    interpolated_dfs = []
    
    for _, session in df.groupby('session_id'):
        if len(session) < 5:
            interpolated_dfs.append(session) # Keep original if too short
            continue
            
        filled_session = interpolate_session(model, session)
        interpolated_dfs.append(filled_session)
        
    df_imputed = pd.concat(interpolated_dfs).sort_values('timestamp')
    
    # Project Back
    df_imputed = project_back(df_imputed, transformer)
    
    # Format for Output
    df_imputed['ID'] = current_animal
    
    # Save (Append mode or overwrite?) Overwrite.
    # Match N-BEATS format: ID, Timestamp, Long, Lat (No header)
    
    out_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_pidl.csv')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    
    # Format
    '''
    try:
        mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
        if mask:
             df_imputed['timestamp'] = df_imputed['timestamp'].dt.strftime(mask)
        else:
             df_imputed['timestamp'] = df_imputed['timestamp'].dt.strftime('%m/%d/%y %H:%M')
    except:
        df_imputed['timestamp'] = df_imputed['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    '''
    
    df_imputed['timestamp'] = df_imputed['timestamp'].dt.strftime('%m/%d/%y %H:%M')

    save_cols = ['ID', 'timestamp', 'Longitude', 'Latitude']
    df_imputed[save_cols].to_csv(out_path, index=False, header=False)
    print(f"Saved PIDL interpolation for {current_animal}")


def run_pipeline_all_pidl(file_rawdata, file_rawdata_columns):
    print("Starting PIDL Interpolation Pipeline...")
    
    # 1. Detect Model
    if "jaguar" in file_rawdata.lower():
        model_name = "jaguar_model.pth"
        epsg, _ = get_utm_zone(file_rawdata)
    elif "tangara" in file_rawdata.lower():
        model_name = "tangara_model.pth"
        epsg, _ = get_utm_zone(file_rawdata)
    else:
        print("Unknown dataset type in filename. Defaulting to Tangara model.")
        model_name = "tangara_model.pth"
        epsg = "EPSG:32723"

    model_path = os.path.join(os.path.dirname(__file__), '../Results/PIDL_Output', model_name)
    
    if not os.path.exists(model_path):
        print(f"Model not found at {model_path}. Please train first.")
        return

    # 2. Load Model
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = BFBiLSTM().to(device)
    model.load_state_dict(torch.load(model_path, map_location=device))
    model.eval()
    print(f"Loaded {model_name} on {device}")

    # 3. Get Animals
    list_animals = get_list_animals(file_rawdata, file_rawdata_columns)
    print(f"Found {len(list_animals)} animals.")

    # 4. Process Each
    for animal in list_animals:
        run_single_pidl(animal, file_rawdata, model, device, epsg)

    print("PIDL Pipeline Complete.")

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        run_pipeline_all_pidl(sys.argv[1], sys.argv[2])
