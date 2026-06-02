
import os
import sys
import pandas as pd
import numpy as np

# Add scripts folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Common.utils import results_folder

def clean_interpolation_file(file_path):
    if not os.path.exists(file_path):
        return False

    try:
        # Load
        df = pd.read_csv(file_path, header=None)
        if df.shape[1] < 4:
            return False
            
        df.columns = ['ID', 'Timestamp', 'Longitude', 'Latitude']
        original_count = len(df)
        
        # 1. Drop NaNs
        df = df.dropna(subset=['Longitude', 'Latitude'])
        
        # 2. Coerce to Numeric
        df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
        df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
        df = df.dropna(subset=['Longitude', 'Latitude'])
        
        # 3. Filter Invalid Coordinates (0,0) or Out of Bounds
        # Valid Lat: -90 to 90, Valid Lon: -180 to 180
        mask_valid = (
            (df['Latitude'] >= -90) & (df['Latitude'] <= 90) &
            (df['Longitude'] >= -180) & (df['Longitude'] <= 180) &
            ((df['Latitude'] != 0) | (df['Longitude'] != 0))
        )
        df = df[mask_valid]
        
        final_count = len(df)
        if final_count < original_count:
            print(f"Cleaned {file_path}: {original_count} -> {final_count} rows (Removed {original_count - final_count})")
            
        # Save back (Overwrite)
        df.to_csv(file_path, index=False, header=False)
        return True
        
    except Exception as e:
        print(f"Error cleaning {file_path}: {e}")
        return False

def run_cleaning_pipeline(file_rawdata):
    print("--- Starting Interpolation Cleaning Pipeline ---")
    results_dir = results_folder(file_rawdata)
    interp_dir = os.path.join(results_dir, "Interpolation")
    
    if not os.path.exists(interp_dir):
        print("Interpolation directory not found.")
        return

    # Find all interpolation files
    files = [f for f in os.listdir(interp_dir) if f.endswith(".csv") and "interpolation" in f]
    
    count = 0
    for f in files:
        full_path = os.path.join(interp_dir, f)
        if clean_interpolation_file(full_path):
            count += 1
            
    print(f"--- Finished Cleaning. Processed {count} files. ---")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python clean_interpolations.py <file_rawdata>")
    else:
        run_cleaning_pipeline(sys.argv[1])
