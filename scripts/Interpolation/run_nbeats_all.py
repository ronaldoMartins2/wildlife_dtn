import sys
import os
import argparse

# Add scripts folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Common.utils import get_list_animals, results_folder
from Interpolation.nbeat_trainer import train_nbeats_model_single
from Interpolation.nbeat_eval import evaluate_nbeats_model
from Interpolation.nbeat_interpolation import run as run_interpolation
from Data_preparation.separar_localizacoes_individuais import run as run_preparation

def run_pipeline_all(file_rawdata, file_columns, 
                     run_train=True, run_eval=True, run_predict=True, 
                     forecast_steps=50):
    
    print(f"--- Starting N-Beats Pipeline for ALL animals in {file_rawdata} ---")
    print(f"Modes -> Train: {run_train}, Eval: {run_eval}, Predict: {run_predict}")
    
    # 1. Get List of Animals
    list_animals = get_list_animals(file_rawdata, file_columns)
    print(f"Found {len(list_animals)} animals: {list_animals}")
    
    for animal_id in list_animals:
        print(f"\n==================================================")
        print(f" PROCESSING ANIMAL: {animal_id}")
        print(f"==================================================")
        
        # Ensure individual map file exists (Data Prep Step 0)
        try:
             results_dir = results_folder(file_rawdata)
             map_path = os.path.join(results_dir, f'map_{animal_id}.csv')
             if not os.path.exists(map_path):
                 print(f"Map file not found for {animal_id}. Running preparation...")
                 run_preparation(animal_id, file_rawdata, file_columns)
        except Exception as e:
            print(f"Error checking/preparing data for {animal_id}: {e}")
            continue

        # Train
        if run_train:
            try:
                print(f"> Training {animal_id}...")
                train_nbeats_model_single(animal_id, file_rawdata, file_columns)
            except Exception as e:
                print(f"❌ Error training {animal_id}: {e}")
                continue
            
        # Evaluate
        if run_eval:
            try:
                print(f"> Evaluating {animal_id}...")
                evaluate_nbeats_model(animal_id, file_rawdata, file_columns)
            except Exception as e:
                print(f"❌ Error evaluating {animal_id}: {e}")
                pass # continue to prediction even if eval fails? usually okay.

        # Interpolation / Forecasting
        if run_predict:
            try:
                print(f"> Generating Forecast for {animal_id}...")
                run_interpolation(animal_id, forecast_steps, file_rawdata, file_columns)
            except Exception as e:
                print(f"❌ Error generating forecast for {animal_id}: {e}")
                continue
            
    print("\n--- All Animals Completed ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run N-Beats Pipeline for All Animals")
    parser.add_argument("file_rawdata", help="Path to raw data CSV")
    parser.add_argument("file_columns", help="Path to columns JSON")
    
    # Flags for steps (if none specified, run all)
    parser.add_argument("--train", action="store_true", help="Run training step")
    parser.add_argument("--eval", action="store_true", help="Run evaluation step")
    parser.add_argument("--predict", action="store_true", help="Run prediction/interpolation step")
    
    parser.add_argument("--steps", type=int, default=50, help="Number of forecast steps (default: 50)")

    args = parser.parse_args()
    
    # If no specific flags provided, default to ALL
    if not (args.train or args.eval or args.predict):
        do_train = True
        do_eval = True
        do_predict = True
    else:
        do_train = args.train
        do_eval = args.eval
        do_predict = args.predict

    run_pipeline_all(
        args.file_rawdata, 
        args.file_columns, 
        run_train=do_train, 
        run_eval=do_eval, 
        run_predict=do_predict,
        forecast_steps=args.steps
    )
