import sys
import os

# Add scripts folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Interpolation.nbeat_trainer import train_nbeats_model_single
from Interpolation.nbeat_eval import evaluate_nbeats_model

def run_pipeline(animal_id, file_rawdata, file_columns):
    print(f"--- Starting N-Beats Pipeline for {animal_id} ---")
    
    # Train
    print("\n> TRAINING PHASE")
    train_nbeats_model_single(animal_id, file_rawdata, file_columns)
    
    # Evaluate
    print("\n> EVALUATION PHASE")
    evaluate_nbeats_model(animal_id, file_rawdata, file_columns)
    
    print("\n--- Pipeline Completed ---")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python run_nbeats_full.py <AnimalID> <RawDataPath> <ColumnsJSONPath>")
        sys.exit(1)
        
    run_pipeline(sys.argv[1], sys.argv[2], sys.argv[3])
