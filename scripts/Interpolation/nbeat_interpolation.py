import pandas as pd
import torch
import numpy as np
import joblib
from datetime import timedelta
import sys
import os
import math

from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
from Common.utils import results_folder, remove_nan_data, read_field_from_json
from Data_preparation.clear_outtliers import run as run_clear_outliers
from Interpolation.nbeat_model import NBeats

def get_device():
    return torch.device('cuda' if torch.cuda.is_available() else 'cpu')

def getDataFromCSV(current_animal, file_rawdata_name):
    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    if not os.path.exists(file_path): return pd.DataFrame()
    df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    df = remove_nan_data(df, current_animal)
    if len(df) > 0:
        if 'jaguar' in file_rawdata_name.lower():
            df = run_clear_outliers(df, current_animal, file_rawdata_name, dataset_name="Jaguar")
        else:
            df = run_clear_outliers(df, current_animal, file_rawdata_name, dataset_name="Tangará", exclude_cols=["manually-marked-outlier"])
    return df

def load_model_and_scaler(file_rawdata_name):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    models_dir = os.path.join(script_dir, '..', 'Interpolation/models')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    
    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    model_path = os.path.join(models_dir, f'nbeats_model_general_{filename}.pth')
    scaler_path = os.path.join(models_dir, f'nbeats_scaler_{filename}.pkl')

    if not os.path.exists(model_path) or not os.path.exists(scaler_path):
        raise FileNotFoundError("Model or Scaler not found. Please retrain.")

    # Carrega Scaler (RobustScaler agora)
    scaler_X, scaler_y = joblib.load(scaler_path)

    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat")
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat")
    dropout_rate = read_field_from_json(hyperparam_path, "dropout_rate_nbeat") or 0.1
    
    device = get_device()
    model = NBeats(3, 3, hidden_dim, num_blocks, dropout=dropout_rate)
    checkpoint = torch.load(model_path, map_location=device)
    model.load_state_dict(checkpoint['model_state_dict'])
    model.to(device)
    model.eval()
    
    return model, scaler_X, scaler_y

def predict_between_dates(start_date, end_date, df_history, file_rawdata_columns, model, scalers, current_animal, device, target_lon, target_lat):
    scaler_X, scaler_y = scalers
    new_data = []
    current_timestamp = start_date
    
    # Estado Inicial
    if len(df_history) < 2:
        last_time_diff, last_delta_long, last_delta_lat = 1.0, 0.0, 0.0
        current_long, current_lat = df_history.iloc[-1]['Longitude'], df_history.iloc[-1]['Latitude']
    else:
        last_row = df_history.iloc[-1]
        second_last_row = df_history.iloc[-2]
        # Importante: Se a última diferença for muito grande (gap), não usamos ela como input inicial
        # Usamos uma média segura para começar
        raw_time_diff = (last_row['Timestamp'] - second_last_row['Timestamp']).total_seconds() / 3600.0
        last_time_diff = min(raw_time_diff, 4.0) # Capa o input inicial em 4h

        last_delta_long = last_row['Longitude'] - second_last_row['Longitude']
        last_delta_lat = last_row['Latitude'] - second_last_row['Latitude']
        current_long, current_lat = last_row['Longitude'], last_row['Latitude']

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    
    # LOOP DE PREDIÇÃO
    while current_timestamp < end_date:
        # 1. Normalizar Input
        # Se o delta anterior foi muito grande (erro), clampamos antes de normalizar
        # para não assustar o modelo
        safe_dlon_in = max(-0.5, min(0.5, last_delta_long))
        safe_dlat_in = max(-0.5, min(0.5, last_delta_lat))
        
        raw_input = np.array([[last_time_diff, safe_dlon_in, safe_dlat_in]])
        scaled_input = scaler_X.transform(raw_input)
        input_tensor = torch.tensor(scaled_input, dtype=torch.float32).to(device)

        # 2. Previsão
        with torch.no_grad():
            forecast = model(input_tensor)
        
        # 3. Desnormalizar
        scaled_prediction = forecast.cpu().numpy()
        if scaled_prediction.ndim == 1: scaled_prediction = scaled_prediction.reshape(1, -1)
        raw_prediction = scaler_y.inverse_transform(scaled_prediction).flatten()
        
        pred_time = float(raw_prediction[0])
        pred_dlon = float(raw_prediction[1])
        pred_dlat = float(raw_prediction[2])

        # --- 4. TRAVA DE SEGURANÇA (SAFETY CLAMP) ---
        # Isso impede que coordenadas impossíveis sejam geradas
        
        # Tempo: entre 6min e 3h
        pred_time = max(0.1, min(3.0, pred_time))
        
        # Distância: Máximo 0.1 grau (aprox 11km) por passo. 
        # Se o modelo surtar e disser 10 graus, cortamos para 0.1.
        MAX_DEGREE_STEP = 0.1
        pred_dlon = max(-MAX_DEGREE_STEP, min(MAX_DEGREE_STEP, pred_dlon))
        pred_dlat = max(-MAX_DEGREE_STEP, min(MAX_DEGREE_STEP, pred_dlat))
        
        # --- 5. ATRAÇÃO AO DESTINO (Weighted Bridge) ---
        next_timestamp = current_timestamp + timedelta(hours=pred_time)
        if next_timestamp >= end_date: break

        dist_lon = target_lon - current_long
        dist_lat = target_lat - current_lat
        time_rem = (end_date - current_timestamp).total_seconds() / 3600.0
        if time_rem < 0.1: time_rem = 0.1
        
        ideal_dlon = (dist_lon / time_rem) * pred_time
        ideal_dlat = (dist_lat / time_rem) * pred_time
        
        # Limitamos também a velocidade "ideal" para não forçar pulos gigantes no final
        ideal_dlon = max(-MAX_DEGREE_STEP, min(MAX_DEGREE_STEP, ideal_dlon))
        ideal_dlat = max(-MAX_DEGREE_STEP, min(MAX_DEGREE_STEP, ideal_dlat))

        # Quanto mais perto do fim, mais atração
        total_dur = (end_date - start_date).total_seconds()
        curr_dur = (current_timestamp - start_date).total_seconds()
        prog = curr_dur / total_dur if total_dur > 0 else 1
        
        alpha = 0.1 + (0.8 * prog) # 10% a 90% de atração
        
        final_dlon = (pred_dlon * (1-alpha)) + (ideal_dlon * alpha)
        final_dlat = (pred_dlat * (1-alpha)) + (ideal_dlat * alpha)

        current_long += final_dlon
        current_lat += final_dlat
        current_timestamp = next_timestamp
        
        new_data.append([current_animal, current_timestamp.strftime(mask), current_long, current_lat])
        
        last_time_diff = pred_time
        last_delta_long = final_dlon
        last_delta_lat = final_dlat
        
        if len(new_data) > 5000: break

    new_data.append([current_animal, end_date.strftime(mask), target_lon, target_lat])
    return pd.DataFrame(new_data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude'])

def run(current_animal, number_of_predictions, file_rawdata_name, file_rawdata_columns):
    print(f"Running Interpolation (Robust Scaled + Safety) for Animal {current_animal}...")
    device = get_device()
    
    df = getDataFromCSV(current_animal, file_rawdata_name)
    if len(df) == 0: return

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
    df = df.sort_values('Timestamp').reset_index(drop=True)
    
    # Remoção preventiva de outliers no dado histórico para não confundir o ponto de partida
    df = df[(df['Latitude'] >= -90) & (df['Latitude'] <= 90)]

    try:
        model, scaler_X, scaler_y = load_model_and_scaler(file_rawdata_name)
    except Exception as e:
        print(f"Error: {e}")
        return

    df['Time Diff'] = df['Timestamp'].diff().dt.total_seconds() / 3600.0
    
    if len(df) > 1:
        valid_gaps = df['Time Diff'].dropna()
        if valid_gaps.empty: return
        max_gap_idx = valid_gaps.idxmax()
        
        gap_start = df.loc[max_gap_idx-1, 'Timestamp']
        gap_end = df.loc[max_gap_idx, 'Timestamp']
        target_lon = df.loc[max_gap_idx, 'Longitude']
        target_lat = df.loc[max_gap_idx, 'Latitude']
        
        print(f"Gap: {gap_start} -> {gap_end}")
        
        df_interp = predict_between_dates(
            gap_start, gap_end, df.loc[:max_gap_idx-1], 
            file_rawdata_columns, model, (scaler_X, scaler_y), 
            current_animal, device, target_lon, target_lat
        )
        
        results_dir = results_folder(file_rawdata_name)
        interpolation_dir = os.path.join(results_dir, 'Interpolation')
        out_path = os.path.join(interpolation_dir, f'map_{current_animal}_interpolation_nbeats.csv')
        df_interp.to_csv(out_path, index=False, header=False)
        print(f"Saved to {out_path}")