import pandas as pd
import numpy as np
import pyproj
from datetime import timedelta
import os

def project_coordinates(df, lat_col, lon_col, utm_zone):
    """
    Project Lat/Lon to UTM.
    utm_zone: e.g., '20S' or '23S'
    Returns tuple (easting, northing)
    """
    # EPSG codes roughly:
    # WGS84 is 4326.
    # UTM 23S is 32723.
    # UTM 20S is 32720.
    
    crs_wgs84 = pyproj.CRS("EPSG:4326")
    
    if utm_zone == "23S":
        crs_utm = pyproj.CRS("EPSG:32723")
    elif utm_zone == "20S":
        crs_utm = pyproj.CRS("EPSG:32720")
    else:
        raise ValueError(f"Unknown UTM zone: {utm_zone}")
        
    transformer = pyproj.Transformer.from_crs(crs_wgs84, crs_utm, always_xy=True) # Lon, Lat order
    
    # pyproj expects (x, y) which is (lon, lat) clearly defined by always_xy=True
    xx, yy = transformer.transform(df[lon_col].values, df[lat_col].values)
    
    return xx, yy

def process_dataset(filepath, dataset_type, output_path):
    print(f"Processing {dataset_type} from {filepath}...")
    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return

    # standardize columns
    if dataset_type == 'tangara':
        time_col = 'timestamp'
        lon_col = 'location-long'
        lat_col = 'location-lat'
        utm_zone = '23S'
        gap_threshold = timedelta(hours=4)
        
        # Parse time with flexible parsing or specific format if needed
        df[time_col] = pd.to_datetime(df[time_col])

    elif dataset_type == 'jaguar':
        time_col = 'timestamp'
        lon_col = 'location.long'
        lat_col = 'location.lat'
        utm_zone = '20S'
        gap_threshold = timedelta(hours=24)
        
        # Parse time - format 3/12/14 17:39
        # Cleaning potentially messy spaces
        df[time_col] = pd.to_datetime(df[time_col], format='%m/%d/%y %H:%M', errors='coerce')
        # Drop rows where time couldn't be parsed
        df = df.dropna(subset=[time_col])

    # Sort
    df = df.sort_values(by=time_col).reset_index(drop=True)
    
    # Projection
    print(f"Projecting to UTM Zone {utm_zone}...")
    try:
        easting, northing = project_coordinates(df, lat_col, lon_col, utm_zone)
        df['pos_x'] = easting
        df['pos_y'] = northing
    except Exception as e:
        print(f"Projection error: {e}")
        return
    
    # Segmentation (Sessions/Trips)
    print("Segmenting sessions...")
    df['time_diff'] = df[time_col].diff()
    
    # Initialize session_id
    session_id = 0
    session_ids = [0] * len(df)
    
    # Iterative approach is safer for logic, though slower. 
    # Vectorized: cumsum of (diff > threshold)
    
    # We need to check if diff is NaT (first row)
    # Convert time_diff to boolean condition
    is_new_session = df['time_diff'] > gap_threshold
    # Fill first NaT with False (start of first session)
    is_new_session = is_new_session.fillna(False)
    
    df['session_id'] = is_new_session.cumsum()
    
    # Filter short sessions (< 10 points)
    session_counts = df['session_id'].value_counts()
    valid_sessions = session_counts[session_counts >= 10].index
    df_filtered = df[df['session_id'].isin(valid_sessions)].copy()
    
    print(f"Original records: {len(df)}")
    print(f"Filtered records (only sessions >= 10 pts): {len(df_filtered)}")
    print(f"Number of valid sessions: {len(valid_sessions)}")
    
    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_filtered.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")

if __name__ == "__main__":
    base_raw = "/home/abinadabe/projetos/wildlife_dtn/rawdata"
    base_out = "/home/abinadabe/projetos/wildlife_dtn/scripts/Results/PIDL_Preprocessed"
    
    # Process Tangara
    process_dataset(
        os.path.join(base_raw, "tangara_mata_atlantica.csv"),
        "tangara",
        os.path.join(base_out, "tangara_preprocessed.csv")
    )
    
    # Process Jaguar
    process_dataset(
        os.path.join(base_raw, "jaguar_mamiraua.csv"),
        "jaguar",
        os.path.join(base_out, "jaguar_preprocessed.csv")
    )
