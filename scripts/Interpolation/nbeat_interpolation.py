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

def generate_forecast(df, model, scaler, metadata, future_steps=50, noise_level=0.1):
    """
    Generates future trajectory by iteratively predicting deltas.
    noise_level: Standard deviation of noise to add to scaled predictions (since scaler is Standard).
    """
    # 1. Prepare Initial Context
    # We need the last 'input_width' sequence of Deltas
    # Preprocess the entire dataframe to get the deltas in the same way as training
    # Note: We need a dummy "forecast_horizon" in prep just to get the function to work,
    # but we are interested in the LAST valid window of Deltas.
    
    # Using a helper to get deltas without splitting
    # We basically need to manually replicate prep logic to extract the last window
    
    # a. UTM Projection
    median_lon = df['Longitude'].median()
    median_lat = df['Latitude'].median()
    epsg = metadata['utm_epsg'] # Trust training EPSG or recalculate? Better to use training EPSG for consistency.
    
    transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{epsg}", always_xy=True)
    transformer_back = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)
    
    e, n = transformer.transform(df['Longitude'].values, df['Latitude'].values)
    df['E'] = e
    df['N'] = n
    
    # b. Resample (Must match training frequency)
    freq_str = metadata['freq_str']
    df['Timestamp'] = pd.to_datetime(df['Timestamp']) # Ensure datetime type
    df = df.set_index('Timestamp')
    df_resampled = df[['E', 'N']].resample(freq_str).mean()
    df_resampled = df_resampled.interpolate(method='linear', limit_direction='both')
    
    # Re-calculate Delta T info for reconstruction (frequency is fixed now)
    # We assume 'freq_str' implies the step size.
    # Parsing freq_str is complex (e.g. '360T').
    # metadata['median_delta_seconds'] is a better source for the step in seconds.
    step_seconds = metadata['median_delta_seconds']
    
    # c. Calculate Deltas
    df_resampled['Delta_E'] = df_resampled['E'].diff()
    df_resampled['Delta_N'] = df_resampled['N'].diff()
    
    # Drop NaNs
    df_clean = df_resampled.dropna().reset_index(drop=True) # Reset index to 0..N
    
    input_width = metadata['input_width']
    forecast_horizon = metadata['forecast_horizon'] # The model predicts this many steps at once
    
    if len(df_clean) < input_width:
        print("Not enough history to forecast.")
        return None
        
    # Get last window (Input)
    last_deltas = df_clean[['Delta_E', 'Delta_N']].values[-input_width:] # (input_width, 2)
    current_input = last_deltas.copy() # (10, 2)
    
    # Prepare for Iterative Prediction
    # We predict 'forecast_horizon' steps at a time.
    # Then shift the window? No, this is Multi-Horizon.
    # Usually: Input[t-10:t] -> Output[t:t+5]
    # IF we want 50 steps:
    # 1. Pred[t:t+5]
    # 2. Append Pred to Input -> Input[t-5 : t+5] -> take last 10 -> Input[t-5+5 : t+5]
    #    Wait, if Input size is 10, and we predict 5.
    #    New Input should be: Old_Input[5:] + New_Pred[0:5]
    
    generated_deltas = []
    
    device = next(model.parameters()).device
    
    loops = int(np.ceil(future_steps / forecast_horizon))
    
    for _ in range(loops):
        # Scale Input
        # Scaler expects (N, Features). We flatten our (Input, Features) to fit if scaler was fitted on flat?
        # In trainer: scaler.fit(X_train_flat) where features are (Delta_E, Delta_N) standard scaled.
        # So we scale frame by frame.
        
        curr_shape = current_input.shape
        input_flat = current_input.reshape(-1, 2)
        input_scaled = scaler.transform(input_flat).reshape(curr_shape)
        
        # Tensor (Batch=1, Input, Feat)
        input_t = torch.tensor(input_scaled, dtype=torch.float32).unsqueeze(0).to(device)
        
        # Predict
        with torch.no_grad():
            pred_scaled = model(input_t).cpu().numpy().squeeze(0) # (Horizon, 2)
            
        # Add Stochastic Noise (Gaussian)
        # scaler scales to Mean=0, Std=1. So noise_level=0.2 means 20% of standard deviation.
        if noise_level > 0:
            noise = np.random.normal(0, noise_level, pred_scaled.shape)
            pred_scaled += noise
            
        # Inverse Scale
        pred_flat = pred_scaled.reshape(-1, 2)
        pred_deltas = scaler.inverse_transform(pred_flat) # (Horizon, 2)
        
        generated_deltas.extend(pred_deltas)
        
        # Update Input for next loop
        # Shift window: remove first 'horizon' elements, append predicted 'horizon' elements
        # current_input (10, 2)
        # pred_deltas (5, 2)
        
        # If horizon < input_width (e.g. 5 < 10)
        # New input = [Old[5:], New]
        if forecast_horizon <= input_width:
            current_input = np.vstack([current_input[forecast_horizon:], pred_deltas])
        else:
            # If horizon > input_width (unlikely here but possible)
            # Just take last 'input_width' of prediction
            current_input = pred_deltas[-input_width:]
            
    # Trim to requested future_steps
    generated_deltas = np.array(generated_deltas)[:future_steps]
    
    # Reconstruct Absolute Paths
    # We need the Last Known Absolute Position (E, N) and Timestamp
    last_known_timestamp = df_clean.index[-1] # From resampled dataframe (Wait, reset_index dropped it!)
    # Actually df_resampled has the index if we didn't reset it, but we needed to dropna for deltas.
    # The last row of df_clean corresponds to the last valid Delta.
    # To get absolute position, we need the Row corresponding to df_clean.iloc[-1].
    # df_clean row i corresponds to Delta between i and i-1.
    # So df_clean.iloc[-1] is (Pos[T] - Pos[T-1]).
    # We need Pos[T].
    # Let's go back to df_resampled
    
    df_no_na = df_resampled.dropna()
    last_absolute_row = df_no_na.iloc[-1]
    
    start_E = last_absolute_row['E']
    start_N = last_absolute_row['N']
    # Timestamp: this is the index of the dataframe
    start_time = df_no_na.index[-1] 
    
    # Cumulative Sum of predicted deltas
    # Pred path relative to start:
    path_rel_E = np.cumsum(generated_deltas[:, 0])
    path_rel_N = np.cumsum(generated_deltas[:, 1])
    
    path_abs_E = start_E + path_rel_E
    path_abs_N = start_N + path_rel_N
    
    # Convert back to WGS84
    path_lon, path_lat = transformer_back.transform(path_abs_E, path_abs_N)
    
    # Timestamps
    # Each step is 'step_seconds'
    future_times = [start_time + timedelta(seconds=step_seconds * (i+1)) for i in range(future_steps)]
    
    return pd.DataFrame({
        'Timestamp': future_times,
        'Longitude': path_lon,
        'Latitude': path_lat
    })

def run(current_animal, number_of_predictions, file_rawdata_name, file_rawdata_columns):
    # number_of_predictions is legacy (e.g. "5"). 
    # But usually refers to "how many windows" or just "how many points".
    # Let's assume points for now, or default to a reasonable horizon (e.g. 50 points).
    if not number_of_predictions or int(number_of_predictions) < 5:
        max_steps = 50
    else:
        max_steps = int(number_of_predictions) * 5 # Legacy multiplier? Or just use directly. 
        # If user passes 5, maybe they mean 5 points? Let's default to enough to be useful.
        if max_steps < 20: max_steps = 20

    print(f"Generating {max_steps} forecast steps for {current_animal}...")

    # Load Data
    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    try:
        df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    except:
        print(f"Could not load map_{current_animal}.csv")
        return

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
    
    # Generate
    # Using noise_level=0.2 (20% of std dev) to add "animal-like" randomness
    forecast_df = generate_forecast(df, model, scaler, metadata, future_steps=max_steps, noise_level=0.2)
    
    if forecast_df is None:
        print("Forecast generation failed.")
        return

    # Save
    forecast_df['ID'] = current_animal
    
    # Format Timestamp
    # Try to match "M/D/YY HH:MM" (e.g. 8/1/15 22:00)
    # Linux/Python strftime doesn't easily support "no zero pad" cross-platform, but we can try basic.
    # Actually, let's just use a clean standard format, but round the floats.
    
    # Round coordinates to 6 decimals
    forecast_df['Longitude'] = forecast_df['Longitude'].round(6)
    forecast_df['Latitude'] = forecast_df['Latitude'].round(6)
    
    try:
        # User format seems to be M/D/YY HH:MM. Let's try to stick to standard or raw.
        # If we use the mask from utils, it might just work.
        mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
        if mask:
             forecast_df['Timestamp'] = forecast_df['Timestamp'].dt.strftime(mask)
        else:
             forecast_df['Timestamp'] = forecast_df['Timestamp'].dt.strftime('%m/%d/%y %H:%M')
    except:
        forecast_df['Timestamp'] = forecast_df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    out_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats.csv')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    
    forecast_df[['ID', 'Timestamp', 'Longitude', 'Latitude']].to_csv(out_path, index=False, header=False)
    print(f"Saved forecast to {out_path}")

if __name__ == "__main__":
    if len(sys.argv) >= 4:
        run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])