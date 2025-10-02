import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from datetime import timedelta
import sys
import os
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField

from Common.utils import (
    create_clusterization_results,
    read_field_from_json,
    TRAINNING_SET,
    results_folder,
    remove_nan_data
)

from Interpolation.nhits_trainer import (
    getNhitsModel,
    getModelPath,
    NHiTSTrainer
)

from Data_preparation.clear_outtliers import (
    run as run_clear_outliers
)

from Interpolation.nhits_model import NHits

# python3 12_nhits_interpolation_2.py 94 '1/22/15 17:31' '7/31/15 17:31'

# Load your CSV data here
def load_data(filename, mask):
    df = pd.read_csv(filename, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
    return df

def load_trained_model(current_animal, file_rawdata_name):

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')  # Navigate to the parent directory and into 'Results'
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    input_dim = read_field_from_json(hyperparam_path, "input_dim_nhits")
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nhits")
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nhits")
    num_hierarchies = read_field_from_json(hyperparam_path, "num_hierarchies_nhits")

    hiper_content = []
    hiper_content.append( f"Hyper nhits input_dim {input_dim}" )
    hiper_content.append( f"Hyper nhits hidden_dim {hidden_dim}" )
    hiper_content.append( f"Hyper nhits num_blocks {num_blocks}" )
    hiper_content.append( f"Hyper nhits num_hierarchies {num_hierarchies}" )

    results_dir = results_folder(file_rawdata_name)
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')

    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    model_path = os.path.join(results_dir, f'nhits_model_general_{filename}.pth')
    
    # Create model with same architecture
    model = NHits(input_dim, hidden_dim, num_blocks, num_hierarchies)

    print(f'model_path >>> {model_path}')

    # Load trained weights
    checkpoint = torch.load(model_path, weights_only=False)

    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()

    return model

def predict_between_dates(start_date, end_date, df, model, trainer, mask, num_steps=1):
#def predict_between_dates(start_date, end_date, df, model, trainer, mask, num_steps=10):
    """
    Predict future points using a trained model with proper scaling.

    Parameters:
    - start_date: datetime
    - end_date: datetime
    - df: DataFrame with the last known data
    - model: trained PyTorch model
    - trainer: NHiTSTrainer instance containing scalers
    - mask: datetime format mask
    - num_steps: maximum number of steps to predict

    Returns:
    - new_data: list of [ID, Timestamp, Longitude, Latitude]
    """
    current_timestamp = start_date
    new_data = []

    # Get the last known row (most recent data point)
    last_row = df.iloc[-1]

    # Initial features
    last_timestamp = last_row['Timestamp']
    if isinstance(last_timestamp, str):
        last_timestamp = pd.to_datetime(last_timestamp)

    prev_time_diff = (current_timestamp - last_timestamp).total_seconds() / 3600.0

    longitude = last_row['Longitude']
    latitude = last_row['Latitude']

    for step in range(num_steps):
        # Scale input features
        input_raw = [[prev_time_diff, longitude, latitude]]
        input_scaled = trainer.scaler_features.transform(input_raw)
        input_tensor = torch.tensor(input_scaled, dtype=torch.float32)

        # Predict
        with torch.no_grad():
            time_diff_forecast, lon_forecast, lat_forecast = model(input_tensor)

        # Combine and inverse transform output
        output_scaled = np.array([[time_diff_forecast.item(), lon_forecast.item(), lat_forecast.item()]])
        output_unscaled = trainer.scaler_targets.inverse_transform(output_scaled)

        # Extract unscaled predictions
        predicted_time_diff = output_unscaled[0][0]
        predicted_longitude = output_unscaled[0][1]
        predicted_latitude = output_unscaled[0][2]

        # Stop if prediction is invalid
        if predicted_time_diff <= 0:
            print("Predicted time difference too small or negative. Stopping predictions.")
            break

        # Generate new timestamp
        new_timestamp = current_timestamp + timedelta(hours=predicted_time_diff)

        # Save prediction
        new_data.append([
            last_row['ID'],
            new_timestamp.strftime(mask),
            predicted_longitude,
            predicted_latitude
        ])

        # Update inputs for next iteration
        current_timestamp = new_timestamp
        prev_time_diff = predicted_time_diff
        longitude = predicted_longitude
        latitude = predicted_latitude

    return new_data

# Save the results to CSV
def save_to_csv(df, filename):
    df.to_csv(filename, index=False, header=False)
    print(f"Results saved to {filename}")

def getDataFromCSV( current_animal, file_rawdata_name ):
    # Read the CSV file into a DataFrame

    results_dir = results_folder( file_rawdata_name )
    
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    
    df = pd.read_csv( file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])

    df = remove_nan_data(df, current_animal)

    if len(df) != 0:
        if 'jaguar' in file_rawdata_name:
            df = run_clear_outliers( df, current_animal, file_rawdata_name, dataset_name="Jaguar" )
        else:
            df = run_clear_outliers( df, current_animal, file_rawdata_name, dataset_name="Tangará", exclude_cols=["manually-marked-outlier"] )
    else:
        return pd.DataFrame()

    columns_to_save = ['ID', 'Timestamp', 'Longitude', 'Latitude']
    file_path = os.path.join(results_dir, f'map_{current_animal}_outliers_less.csv')
    df[columns_to_save].to_csv( file_path, index=False, header=False)

    # Limitar a 80% dos registros
    limit = int(TRAINNING_SET * len(df))
    df = df.iloc[:limit]

    hiper_content = []

    hiper_content.append( f"Trainning nhits animal {current_animal} 80% {limit}" )

    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')

    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    return df

def run_mock():

    current_animal = sys.argv [1]
    run( current_animal )

def run(    current_animal,
            start_date, 
            end_date, 
            file_rawdata_name, 
            file_rawdata_columns ):

    df = getDataFromCSV( current_animal, file_rawdata_name )

    if df.empty:
        print(f"DataFrame is empty for animal {current_animal}. Skipping NHITS interpolation.")
        return

    len_animal_outliers_less = len(df)

    # Define your date range and input file

    start_date_str = start_date
    end_date_str = end_date

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

    start_date = pd.to_datetime(start_date_str, format=mask)
    end_date = pd.to_datetime(end_date_str, format=mask)

    results_dir = results_folder( file_rawdata_name )

    #file_path = os.path.join(results_dir, f'map_{current_animal}.csv')

    #df = load_data( file_path, mask )  # Make sure this file exists
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)

    len_animal = int(len_animal_outliers_less)
    predicted_df = pd.DataFrame()

    model = getNhitsModel()
    model_path = getModelPath( file_rawdata_name )

    trainer = NHiTSTrainer(model)
    trainer.load_model(model_path)

    if start_date <= df.iloc[-1]['Timestamp']:
        print("Adjusting start_date to just after last known timestamp.")
        start_date = df.iloc[-1]['Timestamp'] + timedelta(hours=1)
    
    print(f'********************************** len_animal {len_animal}  current_animal {current_animal} nhits')

    while len(predicted_df) < len_animal:
        # Ensure you predict only the remaining number of points
        remaining_predictions = len_animal - len(predicted_df)
        num_steps = min(remaining_predictions, 1)  # Predict at most 1 step, or the remaining points

        new_predictions = predict_between_dates(start_date, end_date, df, model, trainer, mask, num_steps=num_steps)

        if not new_predictions:
            print("No valid predictions returned. Exiting interpolation loop.")
            break  # Avoid infinite loop

        # Ensure we don’t exceed the length we need
        if len(predicted_df) + len(new_predictions) > len_animal:
            new_predictions = new_predictions[:len_animal - len(predicted_df)]  # Trim excess predictions

        new_predictions_df = pd.DataFrame(new_predictions, columns=['ID', 'Timestamp', 'Longitude', 'Latitude'])
        predicted_df = pd.concat([predicted_df, new_predictions_df], ignore_index=True)

        # Update df with new predictions for the next round
        df = pd.concat([df, new_predictions_df], ignore_index=True)

        # Update start_date for next round
        start_date = pd.to_datetime(new_predictions_df.iloc[-1]['Timestamp'])

    '''
    while len(predicted_df) < len_animal:
        new_predictions = predict_between_dates(start_date, end_date, df, model, trainer, mask, num_steps=20)

        if not new_predictions:
            print("No valid predictions returned. Exiting interpolation loop.")
            break  # Avoid infinite loop

        new_predictions_df = pd.DataFrame(new_predictions, columns=['ID', 'Timestamp', 'Longitude', 'Latitude'])
        predicted_df = pd.concat([predicted_df, new_predictions_df], ignore_index=True)
        
        # Also update df to include new predictions so future predictions use the latest point
        df = pd.concat([df, new_predictions_df], ignore_index=True)

        # Update start_date for next round
        start_date = pd.to_datetime(new_predictions_df.iloc[-1]['Timestamp'])

    '''

    print(f'********************************** len(predicted_df) {len(predicted_df)}  current_animal {current_animal} nhits')

    # Save the predictions to a CSV file
    results_dir = results_folder( file_rawdata_name )

    file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nhits.csv')

    create_clusterization_results(f'{results_dir}/Interpolation')
    save_to_csv(predicted_df, file_path)
