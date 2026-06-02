import os
import sys
import pandas as pd
import json
import shutil
from datetime import datetime

# Add scripts folder to path
sys.path.append(os.path.abspath('scripts'))

from Interpolation import nbeat_trainer
import importlib
# Dynamically load permissive interpolator (avoid package __init__ export restrictions)
try:
    nbeat_interpolation = importlib.import_module('Interpolation.nbeat_interpolation_permissive')
except Exception:
    # Fallback to default interpolator
    nbeat_interpolation = importlib.import_module('Interpolation.nbeat_interpolation')
from Data_preparation.data_field import DataField

def setup_verification():
    print("Setting up verification environment...")
    
    # 1. Define Paths
    base_dir = os.path.abspath('scripts')
    raw_csv = os.path.join(base_dir, 'jaguar_mamiraua.csv')
    results_dir = os.path.join(base_dir, 'Results/jaguar_mamiraua')
    os.makedirs(results_dir, exist_ok=True)
    
    # 2. Extract ID 93 data for training input
    # The trainer expects map_{ID}.csv in results folder?
    # Trainer code: file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    
    print(f"Reading {raw_csv}...")
    df = pd.read_csv(raw_csv)
    
    # Filter ID 93
    df_93 = df[df['individual.local.identifier (ID)'] == 93].copy()
    
    # Select columns
    # map_{ID}.csv format expected by training: ID, Timestamp, Lon, Lat (No Header)
    # Source colums: 'individual.local.identifier (ID)', 'timestamp', 'location.long', 'location.lat'
    
    df_export = df_93[['individual.local.identifier (ID)', 'timestamp', 'location.long', 'location.lat']]
    
    # Save to Results/jaguar_mamiraua/map_93.csv
    target_file = os.path.join(results_dir, 'map_93.csv')
    df_export.to_csv(target_file, index=False, header=False)
    print(f"Created {target_file} with {len(df_export)} rows.")
    
    # 3. Create Columns Mapping JSON
    columns_mapping = {
        "id": "individual.local.identifier (ID)",
        "timestamp": "timestamp",
        "longitude": "location.long",
        "latitude": "location.lat",
        "datetime_mask": "%m/%d/%y %H:%M"
    }
    mapping_file = os.path.join(base_dir, 'Data_preparation/columns_verify.json')
    with open(mapping_file, 'w') as f:
        json.dump(columns_mapping, f)
        
    return mapping_file

def modify_hyperparams():
    print("Modifying hyperparameters for quick run...")
    hp_file = os.path.abspath('scripts/Data_preparation/hyperparameters.json')
    backup_file = hp_file + '.bak'
    
    shutil.copy(hp_file, backup_file)
    
    with open(hp_file, 'r') as f:
        hp = json.load(f)
        
    hp['epochs_nbeats'] = 2 # Quick run
    hp['batch_size_nbeat'] = 16
    
    with open(hp_file, 'w') as f:
        json.dump(hp, f, indent=4)
        
    return backup_file

def restore_hyperparams(backup_file):
    print("Restoring hyperparameters...")
    hp_file = os.path.abspath('scripts/Data_preparation/hyperparameters.json')
    shutil.copy(backup_file, hp_file)
    os.remove(backup_file)

def run_verification(mapping_file):
    print("--- Starting Training ---")
    try:
        nbeat_trainer.train_nbeats_model_single('93', 'jaguar_mamiraua.csv', mapping_file)
    except Exception as e:
        print(f"Training failed: {e}")
        import traceback
        traceback.print_exc()
        return

    print("--- Starting Interpolation ---")
    try:
        nbeat_interpolation.run('93', '5', 'jaguar_mamiraua.csv', mapping_file)
    except Exception as e:
        print(f"Interpolation failed: {e}")
        import traceback
        traceback.print_exc()
        return
        
    # Validation
    print("--- Validating Results ---")
    results_dir = os.path.abspath('scripts/Results/jaguar_mamiraua/Interpolation')
    out_file = os.path.join(results_dir, 'map_93_interpolation_nbeats.csv')
    
    if not os.path.exists(out_file):
        print("Output file not found!")
        return
        
    df_out = pd.read_csv(out_file, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    print(f"Generated {len(df_out)} interpolated points.")
    
    # Check for NaN
    if df_out.isnull().any().any():
        print("WARNING: Output contains NaNs!")
    else:
        print("No NaNs detected.")
        
    # Speed check logic could go here, but visual inspection of log/file is first step.

if __name__ == "__main__":
    mapping_file = setup_verification()
    backup_hp = modify_hyperparams()
    try:
        run_verification(mapping_file)
    finally:
        restore_hyperparams(backup_hp)
        # Cleanup mapping file
        if os.path.exists(mapping_file):
            os.remove(mapping_file)
