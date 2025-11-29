import pandas as pd
import torch
import torch.nn as nn
import numpy as np
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
    remove_nan_data
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
    output_dim = read_field_from_json(hyperparam_path, "output_dim")
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim")
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks")

    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks)

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

    if len(df) == 0:
        print(f'df is empty {current_animal}-{file_rawdata_name}')
        return None

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
    df['Timestamp'] = df['Timestamp'].astype(str).str.strip()

    # Normaliza coluna como string e remove possíveis linhas de cabeçalho ("timestamp")
    df['Timestamp'] = df['Timestamp'].astype(str).str.strip()
    header_mask = df['Timestamp'].str.lower() == 'timestamp'
    if header_mask.any():
        print(f"[NBEATS] Removed {header_mask.sum()} header-like rows from Timestamp column")
        df = df.loc[~header_mask].reset_index(drop=True)

    # Tenta parse com o mask informado; em falha, faz fallback para infer/coerce
    if mask:
        try:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
        except Exception as e:
            print(f"[NBEATS] Warning: parsing with mask '{mask}' failed: {e}. Falling back to infer/coerce.")
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce', infer_datetime_format=True)
    else:
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce', infer_datetime_format=True)

    # Remove timestamps inválidos antes de calcular diferenças
    n_invalid = df['Timestamp'].isna().sum()
    if n_invalid > 0:
        print(f"[NBEATS] Dropping {n_invalid} rows with invalid timestamps")
        df = df.dropna(subset=['Timestamp']).reset_index(drop=True)

    # Calculate the time differences between consecutive timestamps in hours
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600

    # Drop the first row since it will have a NaN value for 'Time Difference (hours)'
    df = df.dropna(subset=['Time Difference (hours)'])

    # Let's use the time differences as the target and the latitude, longitude, and previous time differences as features.
    df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)

    df = df.dropna(subset=['Prev Time Difference (hours)'])

    features = ['Prev Time Difference (hours)', 'Longitude', 'Latitude']
    target = 'Time Difference (hours)'

    # Coerce feature/target columns to numeric, remover linhas inválidas e converter para float32
    df[features] = df[features].apply(pd.to_numeric, errors='coerce')
    df[target] = pd.to_numeric(df[target], errors='coerce')

    # Remover linhas com NaN/inf em features ou target
    valid_mask = ~(df[features].isna().any(axis=1) | df[features].isin([np.inf, -np.inf]).any(axis=1) | df[target].isna() | df[target].isin([np.inf, -np.inf]))
    if not valid_mask.all():
        removed = (~valid_mask).sum()
        print(f"[NBEATS] Removed {removed} rows with non-numeric/invalid feature/target values")
        df = df.loc[valid_mask].reset_index(drop=True)

    if df.empty:
        print("[NBEATS] No valid rows after numeric coercion. Aborting interpolation.")
        return None

    X = df[features].values.astype(np.float32)
    y = df[target].values.astype(np.float32).reshape(-1, 1)

    print(f"[NBEATS] Prepared X shape: {X.shape}, y shape: {y.shape}")

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

    # Create the model
    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks)

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
    models_dir = os.path.join(script_dir, '..', 'Interpolation/models')  # Navigate to the parent directory and into 'Results'

    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    model_path = os.path.join(models_dir, f'nbeats_model_general_{filename}.pth')

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

    #Funcionando
    def predict_between_dates(start_date, end_date, df, model, trainer=None, mask=None, num_steps=5, max_rows=None):
        """
        Predict rows between start_date and end_date.
        - trainer, mask are optional (kept for compatibility).
        - max_rows limits total generated rows; if provided, overrides num_steps.
        """
        new_rows = []
        current_timestamp = start_date
        rows_generated = 0
        limit = int(max_rows) if max_rows is not None else int(num_steps)

        # determine device from model
        try:
            model_device = next(model.parameters()).device
        except Exception:
            model_device = torch.device('cpu')

        while current_timestamp <= end_date and rows_generated < limit:
            last_row = df.iloc[-1]

            last_feat = np.array([[last_row['Prev Time Difference (hours)'],
                                    last_row['Longitude'],
                                    last_row['Latitude']]], dtype=np.float32)
            last_tensor = torch.tensor(last_feat, dtype=torch.float32).to(model_device)

            model.eval()
            with torch.no_grad():
                forecast = model(last_tensor)

            # Normaliza forecast para vetor numpy 1D
            if isinstance(forecast, torch.Tensor):
                out = forecast.detach().cpu().numpy().flatten()
            else:
                out = np.array(forecast).flatten()

            # Tratamento de diferentes formatos de saída
            if out.size >= 3:
                predicted_time_diff = float(out[0])
                predicted_lon = float(out[1])
                predicted_lat = float(out[2])
            elif out.size == 1:
                # Modelo retorna apenas time_diff — usar lon/lat do último registro como fallback
                predicted_time_diff = float(out[0])
                predicted_lon = float(last_row['Longitude'])
                predicted_lat = float(last_row['Latitude'])
                print("⚠️ Model returned single value. Using last known lon/lat as fallback for coordinates.")
            else:
                raise RuntimeError(f"[NBEATS] Unexpected forecast size: {out.shape}")

            # Validate predicted_time_diff
            if not np.isfinite(predicted_time_diff) or predicted_time_diff <= 0:
                fallback_td = getattr(trainer, 'median_time_diff', 6.0)
                print(f"   ⚠️ Time diff inválido: {predicted_time_diff}, aplicando fallback {fallback_td}h e continuando")
                predicted_time_diff = fallback_td

            new_timestamp = current_timestamp + timedelta(hours=predicted_time_diff)

            new_row = {
                'ID': last_row['ID'],
                'Timestamp': new_timestamp,
                'Longitude': predicted_lon,
                'Latitude': predicted_lat,
                'Time Difference (hours)': predicted_time_diff,
                'Prev Time Difference (hours)': last_row.get('Time Difference (hours)', predicted_time_diff)
            }
            new_rows.append(new_row)

            # atualiza df para próxima iteração
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            current_timestamp = new_timestamp
            rows_generated += 1

        predicted_df = pd.DataFrame(new_rows)
        return predicted_df

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
            start_date, end_date, df, model, 
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