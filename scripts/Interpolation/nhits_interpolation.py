import torch
import torch.nn as nn
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
    results_folder
) 

# python3 12_nhits_interpolation_2.py 94 '1/22/15 17:31' '7/31/15 17:31'

# Define the NHiTSBlock with hierarchical time series forecasting mechanism
class NHiTSBlock(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_hierarchies):
        super(NHiTSBlock, self).__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, hidden_dim)  # Output is hidden_dim (not scalar)
        
        # Optional: If you want residual connections, ensure they match input/output dimensions
        self.fc_res = nn.Linear(hidden_dim, hidden_dim)  # Fix the dimension mismatch here
        
        self.num_hierarchies = num_hierarchies  # Number of hierarchical levels

    def forward(self, x):
        # Initial forecast (without residual connection)
        forecast = torch.relu(self.fc1(x))  # Shape: [batch_size, hidden_dim]
        forecast = torch.relu(self.fc2(forecast))  # Shape: [batch_size, hidden_dim]
        forecast = self.fc3(forecast)  # Shape: [batch_size, hidden_dim]

        # Initialize forecast_residual as forecast (first step)
        forecast_residual = forecast  # The initial residual is just the forecast itself

        # Apply residual connections (hierarchical forecasting)
        hierarchical_forecasts = [forecast]  # List to store forecasts at different levels
        for _ in range(self.num_hierarchies - 1):
            forecast_residual = self.fc_res(forecast_residual)  # Shape: [batch_size, hidden_dim]
            forecast_residual = torch.relu(forecast_residual)  # Ensure activation for residuals
            forecast_residual = self.fc3(forecast_residual)  # Apply output layer to residual forecast
            hierarchical_forecasts.append(forecast_residual)
        
        return hierarchical_forecasts  # Return hierarchical forecasts


# Define the NHiTS model
class NHiTS(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_blocks, num_hierarchies):
        super(NHiTS, self).__init__()
        self.blocks = nn.ModuleList([NHiTSBlock(input_dim, hidden_dim, num_hierarchies) for _ in range(num_blocks)])
        
        # Separate output layers for time difference, longitude, and latitude
        self.fc_time = nn.Linear(hidden_dim, 1)  # For time difference
        self.fc_lon = nn.Linear(hidden_dim, 1)  # For longitude
        self.fc_lat = nn.Linear(hidden_dim, 1)  # For latitude

    def forward(self, x):
        forecasts = []
        for block in self.blocks:
            block_forecasts = block(x)  # Get hierarchical forecasts from each block
            forecasts.append(block_forecasts)

        # Aggregate all forecasts from each block and hierarchy level
        aggregated_forecasts = [torch.mean(torch.stack([forecast[i] for forecast in forecasts]), dim=0)
                                for i in range(len(forecasts[0]))]

        # The aggregated forecast for each hierarchy is expected to be of shape [batch_size, hidden_dim]
        # We take the first forecast from the first block for simplicity, which should have shape [batch_size, hidden_dim]
        aggregated_forecast = aggregated_forecasts[0]

        # Separate predictions for time difference, longitude, and latitude
        time_diff = self.fc_time(aggregated_forecast)  # Output shape: [batch_size, 1]
        longitude = self.fc_lon(aggregated_forecast)  # Output shape: [batch_size, 1]
        latitude = self.fc_lat(aggregated_forecast)  # Output shape: [batch_size, 1]

        return time_diff.view(-1), longitude.view(-1), latitude.view(-1)


# Load your CSV data here
def load_data(filename, mask):
    df = pd.read_csv(filename, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
    return df

def load_trained_model(current_animal, file_rawdata_name):
 
    script_dir = os.path.dirname(os.path.abspath(__file__))
    hyperparam_path = os.path.join(script_dir, 'hyperparameters.json')

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
    #model = NHiTS(input_dim=3, hidden_dim=64, num_blocks=4, num_hierarchies=3)
    model = NHiTS(input_dim, hidden_dim, num_blocks, num_hierarchies)

    print(f'model_path >>> {model_path}')

    # Load trained weights
    #checkpoint = torch.load(model_path)
    checkpoint = torch.load(model_path, weights_only=False)

    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()

    return model

# Function to predict between two dates
def predict_between_dates(start_date, end_date, df, model, mask, num_steps=10):
    current_timestamp = start_date
    new_data = []

    # Get the last known row (most recent data point)
    last_row = df.iloc[-1]

    # Feature: Prev Time Difference (hours), Longitude, Latitude
    prev_time_diff = (current_timestamp - last_row['Timestamp']).total_seconds() / 3600.0  # in hours
    
    # Directly accessing values for Longitude and Latitude as scalars
    longitude = last_row['Longitude']
    latitude = last_row['Latitude']

    # Prepare the tensor for model input
    last_features = torch.tensor([[prev_time_diff, longitude, latitude]], dtype=torch.float32)

    for step in range(num_steps):
        time_diff_forecast, lon_forecast, lat_forecast = model(last_features)  # Get the forecast from the model
        
        # Extract forecasted values
        predicted_time_diff = time_diff_forecast.item()
        predicted_longitude = lon_forecast.item()
        predicted_latitude = lat_forecast.item()

        # Print the predicted values for debugging
        print(f"Step {step+1}: Predicted time difference: {predicted_time_diff} hours")
        print(f"Predicted Longitude: {predicted_longitude}, Predicted Latitude: {predicted_latitude}")

        # If the predicted time difference is too small, we could skip this step
        if predicted_time_diff < 0.1:  # Adjust this threshold based on your needs
            print("Predicted time difference too small. Stopping predictions.")
            break

        # Calculate the next timestamp using the predicted time difference
        new_timestamp = current_timestamp + timedelta(hours=predicted_time_diff)

        # Save the forecast data point
        #new_data.append([last_row['ID'], new_timestamp.strftime('%m/%d/%y %H:%M'), predicted_longitude, predicted_latitude])
        new_data.append([last_row['ID'], new_timestamp.strftime(mask), predicted_longitude, predicted_latitude])
        

        # Update current timestamp and features for the next iteration
        current_timestamp = new_timestamp
        
        # Update the input features for the next iteration using the last predicted values
        prev_time_diff = predicted_time_diff  # New previous time difference for the next step
        longitude = predicted_longitude  # New predicted longitude
        latitude = predicted_latitude   # New predicted latitude

        # Update the input features for the model
        last_features = torch.tensor([[prev_time_diff, longitude, latitude]], dtype=torch.float32)

    return pd.DataFrame(new_data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude'])


# Save the results to CSV
def save_to_csv(df, filename):
    #df.to_csv(filename, index=False, header=True)
    df.to_csv(filename, index=False, header=False)
    print(f"Results saved to {filename}")

def getDataFromCSV( current_animal, file_rawdata_name ):
    # Read the CSV file into a DataFrame

    results_dir = results_folder( file_rawdata_name )
    
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    
    df = pd.read_csv( file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])

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
    #'1/22/15 17:31'
    #start_date_str = sys.argv [2]
    #end_date_str = sys.argv [3]
    run( current_animal )

def run(    current_animal, 
            len_animal, 
            start_date, 
            end_date, 
            file_rawdata_name, 
            file_rawdata_columns ):

    df = getDataFromCSV( current_animal, file_rawdata_name )

    # Define your date range and input file

    start_date_str = start_date
    end_date_str = end_date

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

    start_date = pd.to_datetime(start_date_str, format=mask)
    end_date = pd.to_datetime(end_date_str, format=mask)

    # Load the dataset
    results_dir = results_folder( file_rawdata_name )

    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')

    df = load_data( file_path, mask )  # Make sure this file exists

    len_animal = int(len_animal)
    predicted_df = pd.DataFrame()

    trainned_model = load_trained_model(current_animal, file_rawdata_name)

    while len(predicted_df) < len_animal:

        # Predict between the two dates
        #new_predictions = predict_between_dates(start_date, end_date, df, model, mask, num_steps=20)
        new_predictions = predict_between_dates(start_date, end_date, df, trainned_model, mask, num_steps=20)

        # Concatenate the new predictions to the existing predicted_df
        predicted_df = pd.concat([predicted_df, new_predictions], ignore_index=True)

        #start_date = predicted_df['date_column'].iloc[-1]  # Replace 'date_column' with the actual column name for dates


    # Save the predictions to a CSV file
    results_dir = results_folder( file_rawdata_name )

    file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nhits.csv')

    save_to_csv(predicted_df, file_path)
