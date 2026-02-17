import pandas as pd
import torch
import numpy as np
import joblib
import os
import sys
import json
# Add scripts folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import timedelta
from pyproj import Transformer
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
from Common.utils import results_folder
from Interpolation.nbeat_model import NBeats
from Interpolation.nbeat_data_prep import preprocess_nbeats_data, get_utm_proj

def generate_forecast_raw(model, scaler, input_deltas, metadata, steps=10, noise_level=0.1):
    """
    Core generation loop that takes raw deltas (numpy) and returns predicted deltas (numpy).
    """
    device = next(model.parameters()).device
    input_width = metadata['input_width']
    forecast_horizon = metadata['forecast_horizon']
    
    # Sanitize input_deltas immediately to prevent Scaler errors
    input_deltas = np.nan_to_num(input_deltas, nan=0.0, posinf=0.0, neginf=0.0)
    
    current_input = input_deltas.copy() # (input_width, 2)
    generated_deltas = []
    
    loops = int(np.ceil(steps / forecast_horizon))
    
    for _ in range(loops):
        # Scale
        curr_shape = current_input.shape
        input_flat = current_input.reshape(-1, 2)
        input_scaled = scaler.transform(input_flat).reshape(curr_shape)
        # Sanitize input
        input_scaled = np.nan_to_num(input_scaled, nan=0.0, posinf=0.0, neginf=0.0)
        
        # Tensor
        input_t = torch.tensor(input_scaled, dtype=torch.float32).unsqueeze(0).to(device)
        
        # Predict
        with torch.no_grad():
            pred_scaled = model(input_t).cpu().numpy().squeeze(0)
            # Sanitize output
            pred_scaled = np.nan_to_num(pred_scaled, nan=0.0, posinf=0.0, neginf=0.0)
            
        # Noise
        if noise_level > 0:
            noise = np.random.normal(0, noise_level, pred_scaled.shape)
            pred_scaled += noise
            
        # Inverse Scale
        pred_flat = pred_scaled.reshape(-1, 2)
        pred_deltas = scaler.inverse_transform(pred_flat)
        
        generated_deltas.extend(pred_deltas)
        
        # Update Input
        if forecast_horizon <= input_width:
             current_input = np.vstack([current_input[forecast_horizon:], pred_deltas])
        else:
             current_input = pred_deltas[-input_width:]
             
    return np.array(generated_deltas)[:steps]

def generate_bidirectional_forecast(df_gap_context, gap_size_steps, model, scaler, metadata):
    """
    Fills a gap using both Forward and Backward prediction.
    df_gap_context: DataFrame containing [Data Before] + [Gap (NaNs)] + [Data After]
    gap_size_steps: Number of missing steps.
    """
    input_width = metadata['input_width']
    
    # 1. Extract contexts
    # We assume df_gap_context is RESAMPLED and contains valid data before/after.
    # We need to calculate deltas.
    
    # E and N must be present
    e_vals = df_gap_context['E'].values
    n_vals = df_gap_context['N'].values
    
    # Identify indices
    # We have valid data up to index 'start_gap_idx'
    # Gap is from 'start_gap_idx + 1' to 'end_gap_idx - 1'
    # Valid data resumes at 'end_gap_idx'
    
    valid_mask = ~np.isnan(e_vals)
    valid_indices = np.where(valid_mask)[0]
    
    # Find the hole
    # Assuming one single gap in this context
    # The gap starts after the first block of valid data
    # and ends before the second block.
    
    # Simple check: find where diff of indices > 1
    diffs = np.diff(valid_indices)
    gap_starts = np.where(diffs > 1)[0]
    if len(gap_starts) == 0:
        return None # No gap?
        
    last_valid_before = valid_indices[gap_starts[0]]
    first_valid_after = valid_indices[gap_starts[0] + 1]
    
    real_gap_size = first_valid_after - last_valid_before - 1
    
    if real_gap_size != gap_size_steps:
        # Mismatch in expected gap size, but we trust the index
        gap_size_steps = real_gap_size
        
    # FORWARD Context
    # We need 'input_width' deltas ending at 'last_valid_before'
    # Delta[i] = P[i] - P[i-1]
    # We need P[last_valid_before - input_width] to P[last_valid_before]
    
    start_context_idx = max(0, last_valid_before - input_width)
    forward_segment = df_gap_context.iloc[start_context_idx : last_valid_before + 1][['E', 'N']].values
    # If segment is shorter than required (input_width+1 points), pad by repeating the first point
    needed_len = input_width + 1
    if forward_segment.shape[0] < needed_len:
        if forward_segment.shape[0] == 0:
            # no history at all, create zeros
            forward_segment = np.vstack([np.zeros(2) for _ in range(needed_len)])
        else:
            pad_count = needed_len - forward_segment.shape[0]
            pad = np.tile(forward_segment[0], (pad_count, 1))
            forward_segment = np.vstack([pad, forward_segment])

    # Calc deltas (N+1 points -> N deltas)
    forward_deltas = np.diff(forward_segment, axis=0)
    
    # BACKWARD Context
    # We need 'input_width' deltas starting from 'first_valid_after' going validly forward in time?
    # No, we need to go BACKWARDS from 'first_valid_after'.
    # So we take points from P[first_valid_after] to P[first_valid_after + input_width]
    # And we treat the sequence as P[N], P[N+1]...
    # REVERSE them: P[N+input_width] ... P[N]
    # Calculate deltas on reversed sequence.
    
    end_context_idx = min(len(df_gap_context) - 1, first_valid_after + input_width)
    backward_segment = df_gap_context.iloc[first_valid_after : end_context_idx + 1][['E', 'N']].values
    # If segment is shorter than required, pad by repeating the last point
    if backward_segment.shape[0] < needed_len:
        if backward_segment.shape[0] == 0:
            backward_segment = np.vstack([np.zeros(2) for _ in range(needed_len)])
        else:
            pad_count = needed_len - backward_segment.shape[0]
            pad = np.tile(backward_segment[-1], (pad_count, 1))
            backward_segment = np.vstack([backward_segment, pad])
    # Reverse the points to simulate walking backwards
    backward_segment_rev = backward_segment[::-1]
    backward_deltas = np.diff(backward_segment_rev, axis=0) # (10, 2)
    
    # 2. Predict
    # Forward prediction
    # Use less noise for interpolation to keep it connecting? Or keep it to add texture?
    pred_forward_deltas = generate_forecast_raw(model, scaler, forward_deltas, metadata, steps=gap_size_steps, noise_level=0.1)
    
    # Backward prediction
    pred_backward_deltas = generate_forecast_raw(model, scaler, backward_deltas, metadata, steps=gap_size_steps, noise_level=0.1)
    
    # 3. Reconstruct Paths
    # Forward Path (from last_valid_before)
    start_point = df_gap_context.iloc[last_valid_before][['E', 'N']].values
    path_forward = np.zeros((gap_size_steps, 2))
    curr = start_point
    for i in range(gap_size_steps):
        curr = curr + pred_forward_deltas[i]
        path_forward[i] = curr
        
    # Backward Path (from first_valid_after)
    # The backward deltas predict steps AWAY from the end point in reverse time.
    end_point = df_gap_context.iloc[first_valid_after][['E', 'N']].values
    path_backward = np.zeros((gap_size_steps, 2))
    
    curr = end_point
    path_backward_rev = []
    for i in range(gap_size_steps):
        curr = curr + pred_backward_deltas[i]
        path_backward_rev.append(curr)
        
    # Reverse back to normal time order
    path_backward = np.array(path_backward_rev)[::-1]
    
    # 4. Merge (Linear Weighted Average)
    # Weights for Forward: 1 -> 0
    # Weights for Backward: 0 -> 1
    
    weights = np.linspace(1, 0, gap_size_steps)
    weights = weights[:, None] 
    
    mixed_path = path_forward * weights + path_backward * (1 - weights)
    
    return mixed_path


def run(current_animal, legacy_number, file_rawdata_name, file_rawdata_columns):
    print(f"Running Bidirectional N-BEATS for {current_animal}...")

    # Load Data
    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    try:
        df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    except:
        print(f"Could not load map_{current_animal}.csv")
        return
        
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.sort_values('Timestamp')

    # Load Model
    script_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = os.path.join(script_dir, 'models')
    model_path = os.path.join(models_dir, f'nbeats_model_{current_animal}.pth')
    
    if not os.path.exists(model_path):
        print(f"Model for {current_animal} not found.")
        return
        
    checkpoint = torch.load(model_path)
    metadata = checkpoint['metadata']
    model_state = checkpoint['model_state_dict']
    scaler = joblib.load(os.path.join(models_dir, f'scaler_{current_animal}.pkl'))
    
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
    
    # 1. Project to UTM
    median_lon = df['Longitude'].median()
    median_lat = df['Latitude'].median()
    epsg = metadata['utm_epsg']
    transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{epsg}", always_xy=True)
    transformer_back = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)
    
    e, n = transformer.transform(df['Longitude'].values, df['Latitude'].values)
    df['E'] = e
    df['N'] = n
    
    # 2. Resample on Full Range
    # Leave gaps as NaNs
    freq_str = metadata['freq_str']
    df_resampled = df.set_index('Timestamp')[['E', 'N']].resample(freq_str).first()
    
    # Mask of valid data
    is_valid = df_resampled['E'].notna()
    valid_indices = np.where(is_valid)[0]
    
    if len(valid_indices) < 2:
        print("Not enough data to interpolate.")
        return
        
    first_valid = valid_indices[0]
    last_valid = valid_indices[-1]
    
    # Truncate to relevant range
    df_resampled = df_resampled.iloc[first_valid : last_valid + 1]
    
    # Re-calc mask
    is_valid = df_resampled['E'].notna().values
    nan_indices = np.where(~is_valid)[0]
    
    if len(nan_indices) == 0:
        print("No gaps to fill.")
        return
        
    # Group NaNs into segments
    from itertools import groupby
    from operator import itemgetter
    
    filled_df = df_resampled.copy()
    
    gap_count = 0
    filled_points = 0
    skipped_context_count = 0
    
    # Create a mask to track which points are interpolated
    is_interpolated = np.zeros(len(filled_df), dtype=bool)

    for k, g in groupby(enumerate(nan_indices), lambda x: x[0]-x[1]):
        group = list(map(itemgetter(1), g))
        start_gap = group[0]
        end_gap = group[-1]
        gap_len = end_gap - start_gap + 1
        
        # Check context
        if start_gap - metadata['input_width'] < 0:
             continue
        if end_gap + metadata['input_width'] >= len(df_resampled):
             continue
            
        # Extract context window
        c_start = start_gap - metadata['input_width']
        c_end = end_gap + metadata['input_width']

        # Clean Context Check
        context_subset = df_resampled.iloc[c_start : c_end + 1]
        
        pre_context = df_resampled.iloc[c_start : start_gap]['E']
        post_context = df_resampled.iloc[end_gap + 1 : c_end + 1]['E']
        
        if pre_context.isna().any() or post_context.isna().any():
             # RELAXED: Fill small gaps in context with linear interpolation
             # This allows N-BEATS to run even if history is imperfect.
             # BUT we must NOT fill the gap itself, otherwise generate_bidirectional_forecast sees no gap!
             
             temp_filled = context_subset.interpolate(method='linear', limit_direction='both').ffill().bfill()
             
             # Re-introduce the target gap (relative to context_subset start)
             rel_start = start_gap - c_start
             rel_end = end_gap - c_start
             
             # Ensure we don't zero out data if indices are weird, but here they are clean.
             # df columns are E, N.
             temp_filled.iloc[rel_start : rel_end + 1] = np.nan
             
             context_subset = temp_filled
        
        # Interpolate
        reconstructed_path = generate_bidirectional_forecast(context_subset, gap_len, model, scaler, metadata)
        
        if reconstructed_path is not None:
             filled_df.iloc[start_gap : end_gap + 1, 0] = reconstructed_path[:, 0]
             filled_df.iloc[start_gap : end_gap + 1, 1] = reconstructed_path[:, 1]
             # Mark these indices as interpolated
             is_interpolated[start_gap : end_gap + 1] = True
             gap_count += 1
             filled_points += len(reconstructed_path)
        
    print(f"Filled {gap_count} gaps ({filled_points} points).")

    
    # Filter for only interpolated points
    # We masked based on filled_df indices (which matches df_resampled)
    
    final_e = filled_df.loc[is_interpolated, 'E'].values
    final_n = filled_df.loc[is_interpolated, 'N'].values
    timestamps = filled_df.index[is_interpolated]
    
    out_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats.csv')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if len(final_e) == 0:
        print("No gaps were successfully filled properly. Saving empty interpolation file.")
        # Save empty file
        pd.DataFrame(columns=['ID', 'Timestamp', 'Longitude', 'Latitude']).to_csv(out_path, index=False, header=False)
        return

    final_lon, final_lat = transformer_back.transform(final_e, final_n)
    
    out_df = pd.DataFrame({
        'ID': current_animal,
        'Timestamp': timestamps,
        'Longitude': final_lon,
        'Latitude': final_lat
    })
    
    # Rounding
    out_df['Longitude'] = out_df['Longitude'].round(6)
    out_df['Latitude'] = out_df['Latitude'].round(6)
    
    # Format
    try:
        mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
        if mask:
             out_df['Timestamp'] = out_df['Timestamp'].dt.strftime(mask)
        else:
             out_df['Timestamp'] = out_df['Timestamp'].dt.strftime('%m/%d/%y %H:%M')
    except:
        out_df['Timestamp'] = out_df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')


    
    out_df.to_csv(out_path, index=False, header=False, date_format='%m/%d/%y %H:%M')
    print(f"Saved interpolated path to {out_path}")

if __name__ == "__main__":
    if len(sys.argv) >= 4:
        run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])