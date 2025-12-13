import pandas as pd
import numpy as np
from pyproj import Transformer
from sklearn.preprocessing import StandardScaler
from Common.utils import get_id_from_json
from Data_preparation.data_field import DataField

def get_utm_proj(longitude, latitude):
    """
    Determines the UTM EPSG code based on longitude and latitude.
    """
    zone = int((longitude + 180) / 6) + 1
    is_northern = latitude >= 0
    epgs_code = 32600 + zone if is_northern else 32700 + zone
    return epgs_code

def preprocess_nbeats_data(df, file_rawdata_columns, input_width=10, forecast_horizon=5, verbose=True):
    """
    Prepares data for N-Beats training with strict adherence to Appendix B:
    - Sorting by Timestamp
    - Global UTM Projection
    - Resampling to fixed frequency
    - Windowing (Sliding Window)
    - Strict Temporal Split (70|15|15)
    
    Returns:
        X_train, y_train, X_val, y_val, X_test, y_test, scalers, metadata
    """
    
    # 1. Basic Cleaning & Sorting
    if verbose: print(f"[Prep] Initial shape: {df.shape}")
    
    # Ensure Timestamp
    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    # Flexible parsing
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    df = df.dropna(subset=['Timestamp']).sort_values('Timestamp').reset_index(drop=True)
    
    if df.empty:
        if verbose: print("[Prep] DataFrame empty after timestamp parsing.")
        return None, None, None, None, None, None, None, None

    # 2. Metric Projection (UTM)
    median_lon = df['Longitude'].median()
    median_lat = df['Latitude'].median()
    epsg = get_utm_proj(median_lon, median_lat)
    
    transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{epsg}", always_xy=True)
    e, n = transformer.transform(df['Longitude'].values, df['Latitude'].values)
    df['E'] = e
    df['N'] = n
    
    # 3. Resampling
    # Calculate median time delta to determine natural frequency
    deltas = df['Timestamp'].diff().dropna()
    median_delta = deltas.median()
    
    # Round to nearest logical frequency (e.g., 10min, 1H, 4H)
    # Simple heuristic: round to nearest minute
    freq_seconds = median_delta.total_seconds()
    if freq_seconds < 60:
        freq_str = '1T' # 1 min
    else:
        freq_minutes = int(round(freq_seconds / 60))
        freq_str = f'{freq_minutes}T'
        
    if verbose: print(f"[Prep] Resampling to frequency: {freq_str} (Derived from median delta: {median_delta})")
    
    df = df.set_index('Timestamp')
    
    # Resample and interpolate coordinates linearly
    # limit_direction='both' ensures leading/trailing NaNs are filled if bins are empty at start/end
    df_resampled = df[['E', 'N']].resample(freq_str).mean()
    df_resampled = df_resampled.interpolate(method='linear', limit_direction='both')
    
    # Drop any remaining NaNs (rare but possible if ALL are NaN)
    df_resampled = df_resampled.dropna()

    # Re-calculate Delta T (now it is constant, but we keep the column for consistency)
    # Ideally should be constant.
    
    # 4. Feature Engineering (Deltas)
    df_resampled['Delta_E'] = df_resampled['E'].diff()
    df_resampled['Delta_N'] = df_resampled['N'].diff()
    
    # We drop the very first NaN from diff
    df_resampled = df_resampled.dropna().reset_index(drop=True)
    
    # Check for Infinity
    if np.isinf(df_resampled[['Delta_E', 'Delta_N']].values).any():
        if verbose: print("[Prep] Found infinity in deltas. Replacing with 0 or dropping.")
        df_resampled = df_resampled.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)

    
    if len(df_resampled) < (input_width + forecast_horizon + 10):
        if verbose: print("[Prep] Sequence too short after resampling.")
        return None, None, None, None, None, None, None, None

    # Data to window: We train on Deltas.
    # We also keep Mean/Std for denormalization later.
    data_values = df_resampled[['Delta_E', 'Delta_N']].values.astype(np.float32)
    
    # 5. Windowing
    # Create windows of size (input_width + forecast_horizon)
    # X: [t-w, ..., t-1], Y: [t, ..., t+h-1]
    
    total_window_size = input_width + forecast_horizon
    # sliding_window_view equivalent
    # shape: (num_windows, total_window_size, num_features)
    num_windows = len(data_values) - total_window_size + 1
    if num_windows <= 0:
        return None, None, None, None, None, None, None, None
        
    windows = []
    for i in range(num_windows):
        window = data_values[i : i + total_window_size]
        windows.append(window)
    windows = np.array(windows) # (N, T_total, Features)
    
    # Split X and Y
    X_all = windows[:, :input_width, :] # (N, Input, F)
    Y_all = windows[:, input_width:, :] # (N, Horizon, F)
    
    # 6. Strict Temporal Split (70 / 15 / 15)
    # We split the WINDOWS, ensuring order is preserved.
    n_samples = len(X_all)
    i_train = int(n_samples * 0.70)
    i_val = int(n_samples * 0.85) # 70 + 15
    
    X_train, y_train = X_all[:i_train], Y_all[:i_train]
    X_val, y_val = X_all[i_train:i_val], Y_all[i_train:i_val]
    X_test, y_test = X_all[i_val:], Y_all[i_val:]
    
    if verbose:
        print(f"[Prep] Split Sizes - Train: {len(X_train)}, Val: {len(X_val)}, Test: {len(X_test)}")
        
    # 7. Normalization (Fit on Train ONLY)
    # We normalize the Deltas.
    # Reshape to (Samples * Steps, Features) to fit scaler, then reshape back
    scaler = StandardScaler()
    
    # Train
    B_train, T_train, F = X_train.shape
    X_train_flat = X_train.reshape(-1, F)
    scaler.fit(X_train_flat)
    
    X_train_scaled = scaler.transform(X_train_flat).reshape(B_train, T_train, F)
    y_train_scaled = scaler.transform(y_train.reshape(-1, F)).reshape(y_train.shape)
    
    # Val
    if len(X_val) > 0:
        X_val_scaled = scaler.transform(X_val.reshape(-1, F)).reshape(X_val.shape)
        y_val_scaled = scaler.transform(y_val.reshape(-1, F)).reshape(y_val.shape)
    else:
        X_val_scaled, y_val_scaled = None, None
        
    # Test
    if len(X_test) > 0:
        X_test_scaled = scaler.transform(X_test.reshape(-1, F)).reshape(X_test.shape)
        y_test_scaled = scaler.transform(y_test.reshape(-1, F)).reshape(y_test.shape)
    else:
        X_test_scaled, y_test_scaled = None, None
        
    metadata = {
        'utm_epsg': epsg,
        'freq_str': freq_str,
        'median_delta_seconds': float(median_delta.total_seconds()),
        'input_width': input_width,
        'forecast_horizon': forecast_horizon,
        'feature_names': ['Delta_E', 'Delta_N']
    }
    
    return X_train_scaled, y_train_scaled, X_val_scaled, y_val_scaled, X_test_scaled, y_test_scaled, scaler, metadata
