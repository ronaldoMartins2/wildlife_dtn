import pandas as pd
import torch
import torch.nn as nn
from datetime import timedelta
import sys
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField

from Common.utils import (
    create_clusterization_results,
    results_folder,
)

from Interpolation.nbeat_trainer import train_nbeats_model

from Interpolation.nbeat_model import NBeats

from Common.utils import (
    read_field_from_json,
    TRAINNING_SET
)

from Data_preparation.clear_outtliers import (
    run as run_clear_outliers
)

def getDataFromCSV( current_animal, file_rawdata_name ):
    # Read the CSV file into a DataFrame
    
    results_dir = results_folder( file_rawdata_name )

    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')

    df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    
    if 'jaguar' in file_rawdata_name:
        # df = remove_null_data(df, current_animal, file_rawdata_name, dataset_name="Jaguar")
        df = run_clear_outliers( df, current_animal, file_rawdata_name, dataset_name="Jaguar" )
    else:
        # df = remove_null_data(df, current_animal, file_rawdata_name, dataset_name="Tangará")
        df = run_clear_outliers( df, current_animal, file_rawdata_name, dataset_name="Tangará", exclude_cols=["manually-marked-outlier"] )

    columns_to_save = ['ID', 'Timestamp', 'Longitude', 'Latitude']
    file_path = os.path.join(results_dir, f'map_{current_animal}_outliers_less.csv')
    df[columns_to_save].to_csv( file_path, index=False, header=False)

    # Limita a 80% do número de registros
    limit = int(TRAINNING_SET * len(df))
    df = df.iloc[:limit]

    hiper_content = []

    hiper_content.append( f"Trainning nbeat animal {current_animal} 80% {limit}" )

    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')

    file_path = os.path.join(results_dir, f'map_{current_animal}_outliers_less_test_only.csv')
    df[columns_to_save].to_csv( file_path, index=False, header=False)

    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    return df

def load_trained_nbeats_model(file_rawdata_name):

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')  # Navigate to the parent directory and into 'Results'

    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    input_dim = 3
    output_dim = read_field_from_json(hyperparam_path, "output_dim_nbeat")
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat")
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat")
    
    # Novos parâmetros de otimização: Batch Normalization e Dropout
    dropout_rate = read_field_from_json(hyperparam_path, 'dropout_rate_nbeat')
    use_batch_norm = read_field_from_json(hyperparam_path, 'use_batch_norm_nbeat')
    
    # Valores padrão caso não encontrados
    if dropout_rate is None:
        dropout_rate = 0.1
    if use_batch_norm is None:
        use_batch_norm = True

    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks, dropout_rate=dropout_rate, use_batch_norm=use_batch_norm)

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Interpolation')  # Navigate to the parent directory and into 'Results'

    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    model_path = os.path.join(data_prep_dir, f'nbeats_model_general_{filename}.pth')

    print(f'Loading trained model from {model_path}')
    checkpoint = torch.load(model_path, map_location=torch.device('cpu'))  # Add map_location if needed
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()
    return model

def run(    current_animal, 
            number_of_predictions, 
            #len_animal, 
            file_rawdata_name, 
            file_rawdata_columns ):

    print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")

    df = getDataFromCSV( current_animal, file_rawdata_name )

    len_animal_outliers_less = len(df)

    if df.empty:
        print(f'df is empty {current_animal}-{file_rawdata_name}')
        return 

    '''
    if 'jaguar' in file_rawdata_name:
        df = run_clear_outliers( df, current_animal, file_rawdata_name, dataset_name="Jaguar" )
    else:
        df = run_clear_outliers( df, current_animal, file_rawdata_name, dataset_name="Tangará", exclude_cols=["manually-marked-outlier"] )

    results_dir = results_folder(file_rawdata_name)
    columns_to_save = ['ID', 'Timestamp', 'Longitude', 'Latitude']
    file_path = os.path.join(results_dir, f'map_{current_animal}_outliers_less.csv')
    df[columns_to_save].to_csv( file_path, index=False, header=False)
    '''

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

    # Convert the 'Timestamp' column to datetime objects
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)

    # Calculate the time differences between consecutive timestamps in hours
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600

    # Drop the first row since it will have a NaN value for 'Time Difference (hours)'
    df = df.dropna(subset=['Time Difference (hours)'])

    # Let's use the time differences as the target and the latitude, longitude, and previous time differences as features.
    df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)

    df = df.dropna(subset=['Prev Time Difference (hours)'])

    features = ['Prev Time Difference (hours)', 'Longitude', 'Latitude']
    target = 'Time Difference (hours)'

    X = df[features].values
    y = df[target].values

    X_tensor = torch.tensor(X, dtype=torch.float32)
    y_tensor = torch.tensor(y, dtype=torch.float32)


    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')  # Navigate to the parent directory and into 'Results'
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    # Hyperparameters
    input_dim = X_tensor.shape[1]  # Number of features (Prev Time Difference, Longitude, Latitude)
    output_dim = read_field_from_json(hyperparam_path, "output_dim_nbeat")  # Output: predict multiple future time steps
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat")  # Hidden layer size
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat")  # Number of N-BEATS blocks
    
    # Novos parâmetros de otimização: Batch Normalization e Dropout
    dropout_rate = read_field_from_json(hyperparam_path, 'dropout_rate_nbeat')
    use_batch_norm = read_field_from_json(hyperparam_path, 'use_batch_norm_nbeat')
    
    # Valores padrão caso não encontrados
    if dropout_rate is None:
        dropout_rate = 0.1
    if use_batch_norm is None:
        use_batch_norm = True

    # Create the model
    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks, dropout_rate=dropout_rate, use_batch_norm=use_batch_norm)

    # Training loop (for demonstration)
    criterion = nn.MSELoss()  # Mean Squared Error Loss
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    # Ensure the target tensor is reshaped correctly to have the same shape as the forecast
    y_tensor = y_tensor.view(-1, 1)  # Reshape to (12, 1) if the model is predicting single values    
    
    hiper_content = []
    hiper_content.append( f"Hyper nbeats input_dim {input_dim}" )
    hiper_content.append( f"Hyper nbeats output_dim {output_dim}" )
    hiper_content.append( f"Hyper nbeats hidden_dim {hidden_dim}" )
    hiper_content.append( f"Hyper nbeats num_blocks {num_blocks}" )
    #hiper_content.append( f"Hyper nbeats num_hierarchies {num_hierarchies}" )

    results_dir = results_folder(file_rawdata_name)
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')

    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Interpolation/models')  # Navigate to the parent directory and into 'Results'

    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    model_path = os.path.join(data_prep_dir, f'nbeats_model_general_{filename}.pth')

    if not os.path.exists(model_path):
        print("Model not found for nbeat, training...")
        # train_nbeats_model(current_animal, file_rawdata_name, file_rawdata_columns)
        print('need first generate trainning model')
        sys.exit()

    print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& 22222222222222")

    '''
    # Function to predict values between dates
    def predict_between_dates(start_date, end_date, df, file_rawdata_columns, model, num_steps=5):
        new_data = []
        current_timestamp = start_date

        while current_timestamp <= end_date:

            # Prepare the input for the model (use the last known values from the previous row)
            last_row = df.iloc[-1]
            last_features = torch.tensor([[last_row['Prev Time Difference (hours)'], last_row['Longitude'], last_row['Latitude']]], dtype=torch.float32)

            # Predict the next time difference (forecasting multiple steps)
            forecast = model(last_features)

            # We can choose how to use the forecast vector. Here we use the first predicted time difference.
            #predicted_time_diff = forecast[0].item()  # Use the first predicted time difference as a scalar
            predicted_time_diff = forecast.item()  # Use the first predicted time difference as a scalar

            #print(".")

            if predicted_time_diff <= 0:
                print("Predicted time difference is non-positive, breaking loop.")
                break

            # Calculate the next timestamp using the predicted time difference
            new_timestamp = current_timestamp + timedelta(hours=predicted_time_diff)

            # Append the new entry to the data with the correct number of columns
            mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

            #new_data.append([current_animal, new_timestamp.strftime('%m/%d/%y %H:%M'), last_row['Longitude'], last_row['Latitude'],
            new_data.append([current_animal, new_timestamp.strftime(mask), last_row['Longitude'], last_row['Latitude'],
                            last_row['Time Difference (hours)'], last_row['Prev Time Difference (hours)']])

            # Update current_timestamp and last_row for the next iteration
            current_timestamp = new_timestamp
            df = pd.concat([df, pd.DataFrame([new_data[-1]], columns=df.columns)], ignore_index=True)

        return pd.DataFrame(new_data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude', 'Time Difference (hours)', 'Prev Time Difference (hours)'])
    '''

    def predict_between_dates(start_date, end_date, df, file_rawdata_columns, model, num_steps=5, max_rows=None):
        new_data = []
        current_timestamp = start_date
        rows_generated = 0

        while current_timestamp <= end_date:
            # Check if we've reached the maximum rows limit
            if max_rows is not None and rows_generated >= max_rows:
                break

            # Prepare the input for the model (use the last known values from the previous row)
            last_row = df.iloc[-1]
            last_features = torch.tensor([[last_row['Prev Time Difference (hours)'], last_row['Longitude'], last_row['Latitude']]], dtype=torch.float32)

            # Predict the next time difference (forecasting multiple steps)
            forecast = model(last_features)
            predicted_time_diff = forecast.item()

            if predicted_time_diff <= 0:
                print("Predicted time difference is non-positive, breaking loop.")
                break

            # Calculate the next timestamp using the predicted time difference
            new_timestamp = current_timestamp + timedelta(hours=predicted_time_diff)

            # Append the new entry to the data with the correct number of columns
            mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
            new_data.append([current_animal, new_timestamp.strftime(mask), last_row['Longitude'], last_row['Latitude'],
                            last_row['Time Difference (hours)'], last_row['Prev Time Difference (hours)']])

            # Update current_timestamp and last_row for the next iteration
            current_timestamp = new_timestamp
            df = pd.concat([df, pd.DataFrame([new_data[-1]], columns=df.columns)], ignore_index=True)
            rows_generated += 1

        return pd.DataFrame(new_data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude', 'Time Difference (hours)', 'Prev Time Difference (hours)'])

    def find_min_max_dates(current_animal, file_rawdata_columns):
        """
        Reads a CSV file and identifies the earliest and latest dates in the 'Datetime' column.

        Args:
            file_path (str): Path to the CSV file.

        Returns:
            tuple: A tuple containing the earliest and latest dates.
        """

        mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

        results_dir = results_folder( file_rawdata_name )

        file_path = os.path.join(results_dir, f'map_{current_animal}.csv')

        data = pd.read_csv(file_path, header=None)

        # Rename columns for clarity (modify as per actual column names)
        data.columns = ['ID', 'Datetime', 'Longitude', 'Latitude']

        # Display the first few rows of the 'Datetime' column for validation
        print("Sample of 'Datetime' column:")
        print(data['Datetime'].head())

        # Convert the 'Datetime' column to datetime format
        data['Datetime'] = pd.to_datetime(data['Datetime'], format=mask, errors='coerce')


        # Check for rows with invalid or missing dates
        invalid_dates = data[data['Datetime'].isna()]
        if not invalid_dates.empty:
            print("Warning: Some rows have invalid or missing dates:")
            print(invalid_dates)

        # Drop rows with invalid dates
        data = data.dropna(subset=['Datetime'])

        # Find the minimum and maximum dates
        min_date = data['Datetime'].min().strftime( mask )
        max_date = data['Datetime'].max().strftime( mask )

        return min_date, max_date
    # Define start and end dates

    start_date_str, end_date_str = find_min_max_dates( current_animal, file_rawdata_columns )

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

    start_date = datetime.strptime(start_date_str, mask)

    # Add 2 months to start_date
    end_date = start_date + relativedelta(months=+2)

    end_date_str = end_date.strftime(mask)

    start_date = pd.to_datetime( start_date_str, format=mask)
    end_date = pd.to_datetime(end_date_str, format=mask)

    print(f'>>>>>>>>>>>>>>>>>>>>>>>>> len_animal_outliers_less {len_animal_outliers_less} current_animal {current_animal} nbeat')

    predicted_df = pd.DataFrame()

    if not df.empty:
        # Generate exactly the number of rows needed
        predicted_df = predict_between_dates(
            start_date, end_date, df, file_rawdata_columns, model, 
            max_rows=len_animal_outliers_less
        )
    else:
        print("Warning: DataFrame is empty.")

    '''
    while len(predicted_df) < len_animal_outliers_less:
        # Call the function to predict data between the given dates
        if not df.empty:
            new_predictions = predict_between_dates(start_date, end_date, df, file_rawdata_columns, model, max_rows=len_animal_outliers_less)
            
            # Check if new predictions were actually generated
            if new_predictions.empty:
                print("Warning: No new predictions generated. Breaking loop to prevent infinite iteration.")
                break
                
            # Concatenate the new predictions to the existing predicted_df
            predicted_df = pd.concat([predicted_df, new_predictions], ignore_index=True)
            
            # Optional: Add a counter to prevent infinite loops
            # iteration_count += 1
            # if iteration_count > max_iterations:
            #     print(f"Warning: Maximum iterations ({max_iterations}) reached.")
            #     break
        else:
            print("Warning: DataFrame is empty. Breaking loop.")
            break
    '''

    '''
    while len(predicted_df) < len_animal:

        # Call the function to predict data between the given dates

        if not df.empty:

            new_predictions = predict_between_dates(start_date, end_date, df, file_rawdata_columns, model)

            # Concatenate the new predictions to the existing predicted_df
            predicted_df = pd.concat([predicted_df, new_predictions], ignore_index=True)
    '''

    print(f'############################### len(predicted_df)  {len(predicted_df)}')

    hiper_content = []
    hiper_content.append( f"Number of nbeat interpolations {len_animal_outliers_less} animal {current_animal}" )

    results__rawdataset_dir = results_folder(file_rawdata_name)
    hiper_path = os.path.join(results__rawdataset_dir, f'hiperparameters.txt')

    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    columns_to_save = ['ID', 'Timestamp', 'Longitude', 'Latitude']

    create_clusterization_results(f'{results__rawdataset_dir}/Interpolation')
    file_path = os.path.join(results__rawdataset_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats.csv')

    predicted_df[columns_to_save].to_csv( file_path, index=False, header=False)


def run_mock( ):

    current_animal = sys.argv [1]
    number_of_predictions = sys.argv[2]

    run( current_animal, number_of_predictions )