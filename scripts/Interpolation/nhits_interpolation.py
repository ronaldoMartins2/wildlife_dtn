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

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    # Ler hiperparâmetros treinados (fallbacks seguros)
    import json
    try:
        with open(hyperparam_path, 'r') as hf:
            hp = json.load(hf)
    except Exception as e:
        hp = {}
        print(f"[NHITS] aviso: não foi possível ler {hyperparam_path}: {e}")

    input_dim = hp.get("input_dim_nhits", 3)
    hidden_dim = hp.get("hidden_dim_nhits", 32)
    num_blocks = hp.get("num_blocks_nhits", 3)
    num_hierarchies = hp.get("num_hierarchies_nhits", 1)
    dropout_rate = hp.get("dropout_rate_nhits", 0.0)
    use_batch_norm = hp.get("use_batch_norm_nhits", False)

    hiper_content = []
    hiper_content.append( f"Hyper nhits input_dim {input_dim}" )
    hiper_content.append( f"Hyper nhits hidden_dim {hidden_dim}" )
    hiper_content.append( f"Hyper nhits num_blocks {num_blocks}" )
    hiper_content.append( f"Hyper nhits num_hierarchies {num_hierarchies}" )
    hiper_content.append( f"Hyper nhits dropout_rate {dropout_rate}" )
    hiper_content.append( f"Hyper nhits use_batch_norm {use_batch_norm}" )

    results_dir = results_folder(file_rawdata_name)
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')

    with open(hiper_path, "a") as file:
        for l in hiper_content:
            file.write(l + "\n")

    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    model_path = os.path.join(results_dir, f'nhits_model_general_{filename}.pth')

    # criar modelo com os hiperparâmetros do treino
    model = NHits(input_dim, hidden_dim, num_blocks, num_hierarchies,
                  dropout_rate=dropout_rate, use_batch_norm=use_batch_norm)

    print(f'[NHITS] tentando carregar checkpoint em: {model_path} (exists={os.path.exists(model_path)})')
    if not os.path.exists(model_path):
        alt = os.path.join(os.path.dirname(__file__), 'models', os.path.basename(model_path))
        if os.path.exists(alt):
            model_path = alt
            print(f'[NHITS] usando modelo alternativo: {model_path}')
        else:
            print(f"[NHITS] Modelo NHITS não encontrado: {model_path}")
            return None

    import torch
    checkpoint = torch.load(model_path, map_location='cpu')
    try:
        model.load_state_dict(checkpoint['model_state_dict'])
    except RuntimeError as err:
        # Falha clara: provavelmente hiperparâmetros diferentes entre treino e inferência
        raise RuntimeError(
            "Falha ao carregar state_dict do NHits. Verifique se os hiperparâmetros em "
            "Data_preparation/hyperparameters.json batem com os usados no treino. "
            f"Erro original: {err}"
        ) from err

    model.eval()
    return model

def predict_between_dates(start_date, end_date, df, model, trainer, mask, num_steps=1):
    """
    Predict future points using a trained model with proper scaling.
    Handles model outputs that may be:
     - a single tensor (possibly batched),
     - a tuple/list of tensors,
     - plain python numbers.
    """
    print(f"[NHITS] predict_between_dates start={start_date} end={end_date} num_steps={num_steps}")
    current_timestamp = start_date
    new_data = []

    # Get the last known row (most recent data point)
    last_row = df.iloc[-1]
    print(f"[NHITS] last_row timestamp={last_row['Timestamp']} lon={last_row['Longitude']} lat={last_row['Latitude']}")

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

        # Ensure model and batchnorm are in eval for single-sample inference
        model.eval()
        for m in model.modules():
            if isinstance(m, torch.nn.modules.batchnorm._BatchNorm):
                m.eval()

        with torch.no_grad():
            if input_tensor.dim() == 1:
                input_tensor = input_tensor.unsqueeze(0)
            raw_out = model(input_tensor)
            print(f"[NHITS] raw_out type={type(raw_out)} raw_out={raw_out}")
            # Normalize output into a flat Python list of floats
            flat_vals = []
            if isinstance(raw_out, (tuple, list)):
                for elem in raw_out:
                    if isinstance(elem, torch.Tensor):
                        e = elem.detach().squeeze()
                        if e.numel() == 0:
                            continue
                        flat_vals.extend(e.reshape(-1).cpu().numpy().tolist())
                    else:
                        flat_vals.append(float(elem))
            elif isinstance(raw_out, torch.Tensor):
                t = raw_out.detach().squeeze()
                if t.numel() == 0:
                    flat_vals = []
                else:
                    flat_vals = t.reshape(-1).cpu().numpy().tolist()
            else:
                # scalar python number
                flat_vals = [float(raw_out)]

            print(f"[NHITS] flat_vals={flat_vals}")

            if len(flat_vals) == 0:
                print("Warning: model returned no values. Stopping.")
                break

            # Assign predictions (safely handle missing values)
            time_diff_pred = float(flat_vals[0])
            lon_pred = float(flat_vals[1]) if len(flat_vals) > 1 else None
            lat_pred = float(flat_vals[2]) if len(flat_vals) > 2 else None

        # Prepare output for inverse scaling (fill missing with zeros)
        out_for_scaler = [
            time_diff_pred,
            lon_pred if lon_pred is not None else 0.0,
            lat_pred if lat_pred is not None else 0.0
        ]
        output_scaled = np.array([out_for_scaler])
        output_unscaled = trainer.scaler_targets.inverse_transform(output_scaled)

        predicted_time_diff = output_unscaled[0][0]
        predicted_longitude = output_unscaled[0][1]
        predicted_latitude = output_unscaled[0][2]

        # Validate predictions
        if not (-90 <= predicted_latitude <= 90) or not (-180 <= predicted_longitude <= 180):
            print(f"[NHITS] Predição inválida: latitude {predicted_latitude}, longitude {predicted_longitude}. Interrompendo predições.")
            break

        if predicted_time_diff <= 0:
            print("Predicted time difference too small or negative. Stopping predictions.")
            break

        new_timestamp = current_timestamp + timedelta(hours=predicted_time_diff)

        new_data.append([
            last_row['ID'],
            new_timestamp.strftime(mask),
            predicted_longitude,
            predicted_latitude
        ])

        # Update for next iteration
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
    print(f"[NHITS] run current_animal={current_animal} file={file_rawdata_name} dates=({start_date},{end_date})")
    df = getDataFromCSV( current_animal, file_rawdata_name )
    print(f"[NHITS] loaded df shape: {None if df is None else df.shape}")
    if df.empty:
        print(f"[NHITS] DataFrame is empty for animal {current_animal}. Skipping NHITS interpolation.")
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
    print(f"[NHITS] model_path = {model_path} exists={os.path.exists(model_path)}")

    # Fallback: se o path retornado não existir, tentar procurar em ./models/<basename>
    if not os.path.exists(model_path):
        alt_model_path = os.path.join(os.path.dirname(__file__), 'models', os.path.basename(model_path))
        print(f"[NHITS] model_path não encontrado, tentando alternativa: {alt_model_path} exists={os.path.exists(alt_model_path)}")
        if os.path.exists(alt_model_path):
            model_path = alt_model_path
            print(f"[NHITS] Usando modelo alternativo: {model_path}")
        else:
            print(f"Modelo NHITS não encontrado: {model_path}\nGere o treino primeiro ou verifique o caminho.")
            return

    trainer = NHiTSTrainer(model)
    trainer.load_model(model_path)
    print(f"[NHITS] trainer loaded. has scaler_features={hasattr(trainer,'scaler_features')}, has scaler_targets={hasattr(trainer,'scaler_targets')}")
    
    # Verificações básicas pós-load
    if not hasattr(trainer, 'scaler_features') or trainer.scaler_features is None:
        print("Aviso: scaler_features não encontrado no trainer (checkpoint pode estar incompleto).")
        return
    if not hasattr(trainer, 'scaler_targets') or trainer.scaler_targets is None:
        print("Aviso: scaler_targets não encontrado no trainer (checkpoint pode estar incompleto).")
        return
    
    if start_date <= df.iloc[-1]['Timestamp']:
        print("Adjusting start_date to just after last known timestamp.")
        start_date = df.iloc[-1]['Timestamp'] + timedelta(hours=1)
    
    print(f'********************************** len_animal {len_animal}  current_animal {current_animal} nhits')

    while len(predicted_df) < len_animal:
        remaining_predictions = len_animal - len(predicted_df)
        num_steps = min(remaining_predictions, 1)
        new_predictions = predict_between_dates(start_date, end_date, df, model, trainer, mask, num_steps=num_steps)
        print(f"[NHITS] new_predictions returned: {len(new_predictions)}")
        if not new_predictions:
            print("[NHITS] No valid predictions returned. Exiting interpolation loop.")
            break

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

    print(f"[NHITS] final predicted_df shape: {predicted_df.shape}")
    create_clusterization_results(f'{results_dir}/Interpolation')
    save_to_csv(predicted_df, file_path)
