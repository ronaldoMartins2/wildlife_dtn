# nbeat_trainer.py
import os
import torch
import torch.nn as nn
import pandas as pd

from Common.utils import results_folder, read_field_from_json, TRAINNING_SET
from Data_preparation.clear_outtliers import run as run_clear_outliers
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
#from Interpolation.nbeat_interpolation import NBeats
from Interpolation.nbeat_model import NBeats

def prepare_training_data(  #current_animal, 
                            df,
                            file_rawdata_name,
                            file_rawdata_columns):

    limit = int(TRAINNING_SET * len(df))
    df = df.iloc[:limit]

    if df.empty:
        return None, None

    df = run_clear_outliers(df, dataset_name="Tangará", exclude_cols=["manually-marked-outlier"])

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600
    df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)

    df = df.dropna(subset=['Time Difference (hours)', 'Prev Time Difference (hours)'])

    features = ['Prev Time Difference (hours)', 'Longitude', 'Latitude']
    target = 'Time Difference (hours)'

    X = df[features].values
    y = df[target].values

    return torch.tensor(X, dtype=torch.float32), torch.tensor(y, dtype=torch.float32)

def train_nbeats_model_list(
                        animal_list,
                        file_rawdata_name, 
                        file_rawdata_columns,
                        ):
    results_dir = results_folder(file_rawdata_name)
    
    combined_df_list = []

    for current_animal in animal_list:
        #df = load_data_for_training(current_animal, file_rawdata_name, file_rawdata_columns)
        file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
        df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
        combined_df_list.append(df)

    combined_df = pd.concat(combined_df_list, ignore_index=True)

    train_nbeats_model( #current_animal,
                        combined_df,
                        file_rawdata_name, 
                        file_rawdata_columns)
                        
def train_nbeats_model_single(
                            current_animal,
                            file_rawdata_name, 
                            file_rawdata_columns, 
                            ):

    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')

    df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])

    train_nbeats_model( #current_animal,
                        df,
                        file_rawdata_name, 
                        file_rawdata_columns)

def train_nbeats_model( #current_animal, 
                        df,
                        file_rawdata_name, 
                        file_rawdata_columns, 
                        epochs=100, 
                        lr=0.001):

    #X_tensor, y_tensor = prepare_training_data(current_animal, file_rawdata_name, file_rawdata_columns)
    X_tensor, y_tensor = prepare_training_data(df, file_rawdata_name, file_rawdata_columns)

    if X_tensor is None or y_tensor is None:
        print("Training data is empty. Skipping training.")
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')  # Navigate to the parent directory and into 'Results'

    #script_dir = os.path.dirname(os.path.abspath(__file__))
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    input_dim = X_tensor.shape[1]
    output_dim = read_field_from_json(hyperparam_path, "output_dim")
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim")
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks")

    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    y_tensor = y_tensor.view(-1, 1)

    filename = file_rawdata_name.split('/')[-1].split('.')[0]

    log_file_path = "training_log_nbeat.txt"

    final_loss = 0

    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        forecast = model(X_tensor)
        loss = criterion(forecast, y_tensor)
        loss.backward()
        optimizer.step()
        if epoch % 10 == 0:
            #print(f"[{current_animal}] Epoch {epoch}, Loss: {loss.item():.4f}")
            #print(f"[{filename}] Epoch {epoch}, Loss: {loss.item():.4f}")
            log_msg = f"[{filename}] Epoch {epoch}, Loss: {loss.item():.4f}"
            print(log_msg)

            final_loss = loss.item()

            # Write log to file
            with open(log_file_path, "a") as f:
                f.write(log_msg + "\n")

    results_dir = results_folder(file_rawdata_name)

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Interpolation/models')  # Navigate to the parent directory and into 'Results'

    model_path = os.path.join(data_prep_dir, f'nbeats_model_general_{filename}.pth')
    torch.save({'model_state_dict': model.state_dict()}, model_path)
    print(f"Model saved to {model_path}")

    hiper_content = []
 
    hiper_content.append( f"Hyper nbeat input_dim {input_dim}" )
    hiper_content.append( f"Hyper nbeat output_dim {output_dim}" )
    hiper_content.append( f"Hyper nbeat hidden_dim {hidden_dim}" )
    hiper_content.append( f"Hyper nbeat num_blocks {num_blocks}" )
    hiper_content.append( f"Hyper nbeat loss {final_loss}" )
    hiper_content.append( f"Hyper nbeat epochs {epochs}" )

    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')

    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

if __name__ == "__main__":
    import sys
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    
    # You need to load this from somewhere; this is placeholder
    from Common.utils import load_columns_config
    file_rawdata_columns = load_columns_config(file_rawdata_name)  # implement if missing

    train_nbeats_model(current_animal, file_rawdata_name, file_rawdata_columns)