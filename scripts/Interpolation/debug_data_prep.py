import sys
import os
import pandas as pd
import numpy as np

# Add scripts folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Interpolation.nbeat_data_prep import preprocess_nbeats_data
from Common.utils import results_folder

def debug_prep(animal_id, file_rawdata, file_columns):
    print(f"--- DEBUGGING {animal_id} ---")
    results_dir = results_folder(file_rawdata)
    file_path = os.path.join(results_dir, f'map_{animal_id}.csv')
    
    try:
        df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    except:
        print("File not found.")
        return

    print(f"Original Rows: {len(df)}")
    
    # Run Prep
    try:
        # We need to suppress print or just capture it? 
        # The function uses print(verbose=True) by default.
        X_train, y_train, X_val, y_val, X_test, y_test, scaler, metadata = preprocess_nbeats_data(
            df, file_columns, input_width=10, forecast_horizon=5, verbose=True
        )
        
        if X_train is None:
            print("PREP RETURNED NONE.")
            return

        print(f"Train Size: {len(X_train)}")
        print(f"Val Size: {len(X_val)}")
        print(f"Test Size: {len(X_test)}")
        print(f"Freq: {metadata.get('freq_str')}")
        print(f"Median Delta Seconds: {metadata.get('median_delta_seconds')}")
        
        # Check Scaler Params (Mean, Var)
        print(f"Scaler Mean: {scaler.mean_}")
        print(f"Scaler Var: {scaler.var_}")
        
    except Exception as e:
        print(f"PREP FAILED: {e}")

if __name__ == "__main__":
    debug_prep('95', 'rawdata/jaguar_mamiraua.csv', 'rawdata/jaguar_columns.json')
    print("\n")
    debug_prep('99', 'rawdata/jaguar_mamiraua.csv', 'rawdata/jaguar_columns.json')
