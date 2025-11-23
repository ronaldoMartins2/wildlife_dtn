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
from Interpolation.nhits_model import NHITS

# ==========================================
# 1. Configurações
# ==========================================

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
    model_path = os.path.join(models_dir, f'nhits_model_general_{filename}.pth')
    scaler_path = os.path.join(models_dir, f'nhits_scaler_{filename}.pkl')

    if not os.path.exists(model_path) or not os.path.exists(scaler_path):
        raise FileNotFoundError("NHITS Model or Scaler not found. Please retrain.")

    scaler_X, scaler_y = joblib.load(scaler_path)

    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nhits") or 64
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nhits") or 2
    dropout_rate = read_field_from_json(hyperparam_path, "dropout_rate_nhits") or 0.1
    
    device = get_device()
    model = NHITS(3, 3, hidden_dim, num_blocks, dropout=dropout_rate)
    checkpoint = torch.load(model_path, map_location=device)
    model.load_state_dict(checkpoint['model_state_dict'])
    model.to(device)
    model.eval()
    
    return model, scaler_X, scaler_y

# ==========================================
# 2. Lógica de Interpolação Blindada e Adaptativa
# ==========================================

def predict_between_dates(start_date, end_date, df_history, file_rawdata_columns, model, scalers, current_animal, device, target_lon, target_lat):
    scaler_X, scaler_y = scalers
    new_data = []
    current_timestamp = start_date
    
    if df_history.empty:
        print("Error: History is empty.")
        return pd.DataFrame(columns=['ID', 'Timestamp', 'Longitude', 'Latitude'])

    # --- LÓGICA ADAPTATIVA DE TEMPO ---
    total_gap_hours = (end_date - start_date).total_seconds() / 3600.0
    
    # Define um passo mínimo dinâmico para não gerar milhões de pontos
    # Alvo: Gerar aprox 2000 pontos para o intervalo inteiro
    target_num_points = 2000
    dynamic_min_step = max(0.1, total_gap_hours / target_num_points)
    
    print(f"Gap Duration: {total_gap_hours:.1f} hours. Using dynamic min step: {dynamic_min_step:.4f} hours")
    # ----------------------------------

    # Inicialização
    if len(df_history) < 2:
        last_time_diff, last_delta_long, last_delta_lat = 1.0, 0.0, 0.0
        current_long, current_lat = df_history.iloc[-1]['Longitude'], df_history.iloc[-1]['Latitude']
    else:
        last_row = df_history.iloc[-1]
        second_last_row = df_history.iloc[-2]
        
        raw_time_diff = (last_row['Timestamp'] - second_last_row['Timestamp']).total_seconds() / 3600.0
        last_time_diff = min(raw_time_diff, 4.0)
        
        last_delta_long = last_row['Longitude'] - second_last_row['Longitude']
        last_delta_lat = last_row['Latitude'] - second_last_row['Latitude']
        current_long, current_lat = last_row['Longitude'], last_row['Latitude']

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    
    # Aumentamos o limite de segurança baseado no target
    safety_limit = target_num_points * 2 

    while current_timestamp < end_date:
        # 1. Normalizar Input
        safe_dlon_in = max(-0.5, min(0.5, last_delta_long))
        safe_dlat_in = max(-0.5, min(0.5, last_delta_lat))
        
        raw_input = np.array([[last_time_diff, safe_dlon_in, safe_dlat_in]])
        scaled_input = scaler_X.transform(raw_input)
        input_tensor = torch.tensor(scaled_input, dtype=torch.float32).to(device)

        with torch.no_grad():
            forecast = model(input_tensor)
        
        scaled_pred = forecast.cpu().numpy()
        if scaled_pred.ndim == 1: scaled_pred = scaled_pred.reshape(1, -1)
        raw_pred = scaler_y.inverse_transform(scaled_pred).flatten()
        
        pred_time = float(raw_pred[0])
        pred_dlon = float(raw_pred[1])
        pred_dlat = float(raw_pred[2])

        # 2. Clamp Dinâmico de Tempo
        # O passo não pode ser menor que o dinâmico calculado (para longos gaps)
        # e não maior que 4x o dinâmico (para manter resolução)
        pred_time = max(dynamic_min_step, min(dynamic_min_step * 5, pred_time))
        
        # 3. Clamp de Espaço
        MAX_DEGREE_STEP = 0.1 
        pred_dlon = max(-MAX_DEGREE_STEP, min(MAX_DEGREE_STEP, pred_dlon))
        pred_dlat = max(-MAX_DEGREE_STEP, min(MAX_DEGREE_STEP, pred_dlat))
        
        # 4. Atração ao Destino
        next_timestamp = current_timestamp + timedelta(hours=pred_time)
        if next_timestamp >= end_date: break

        dist_lon = target_lon - current_long
        dist_lat = target_lat - current_lat
        time_rem = (end_date - current_timestamp).total_seconds() / 3600.0
        if time_rem < 0.1: time_rem = 0.1
        
        ideal_dlon = (dist_lon / time_rem) * pred_time
        ideal_dlat = (dist_lat / time_rem) * pred_time
        
        ideal_dlon = max(-MAX_DEGREE_STEP, min(MAX_DEGREE_STEP, ideal_dlon))
        ideal_dlat = max(-MAX_DEGREE_STEP, min(MAX_DEGREE_STEP, ideal_dlat))

        # Progressão
        total_dur = (end_date - start_date).total_seconds()
        curr_dur = (current_timestamp - start_date).total_seconds()
        prog = curr_dur / total_dur if total_dur > 0 else 1
        
        # Se o gap for muito grande, aumentamos a atração base para evitar drift inicial
        base_alpha = 0.1 if total_gap_hours < 240 else 0.3 
        alpha = base_alpha + ((0.9 - base_alpha) * prog)
        
        final_dlon = (pred_dlon * (1-alpha)) + (ideal_dlon * alpha)
        final_dlat = (pred_dlat * (1-alpha)) + (ideal_dlat * alpha)

        current_long += final_dlon
        current_lat += final_dlat
        current_timestamp = next_timestamp
        
        new_data.append([current_animal, current_timestamp.strftime(mask), current_long, current_lat])
        
        last_time_diff = pred_time
        last_delta_long = final_dlon
        last_delta_lat = final_dlat
        
        if len(new_data) > safety_limit: 
            print(f"Safety limit reached ({safety_limit} points). Stopping.")
            break

    new_data.append([current_animal, end_date.strftime(mask), target_lon, target_lat])
    return pd.DataFrame(new_data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude'])

# ==========================================
# 3. Execução Principal
# ==========================================

def run(current_animal, input_start_date, input_end_date, file_rawdata_name, file_rawdata_columns):
    print(f"Running NHITS Interpolation for Animal {current_animal}...")
    device = get_device()
    
    df = getDataFromCSV(current_animal, file_rawdata_name)
    if len(df) == 0: return

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
    df = df.sort_values('Timestamp').reset_index(drop=True)
    
    df = df[(df['Latitude'] >= -90) & (df['Latitude'] <= 90)]

    try:
        model, scaler_X, scaler_y = load_model_and_scaler(file_rawdata_name)
    except Exception as e:
        print(f"Error loading model: {e}")
        return

    gap_start, gap_end = None, None
    target_lon, target_lat = None, None
    history_subset = pd.DataFrame()

    if input_start_date and input_end_date:
        try:
            s_date = pd.to_datetime(input_start_date)
            e_date = pd.to_datetime(input_end_date)
            print(f"Using provided dates: {s_date} -> {e_date}")
            
            closest_end_idx = (df['Timestamp'] - e_date).abs().idxmin()
            target_lon = df.loc[closest_end_idx, 'Longitude']
            target_lat = df.loc[closest_end_idx, 'Latitude']
            
            history_subset = df[df['Timestamp'] <= s_date]
            if history_subset.empty:
                print(f"Warning: Using first record as anchor.")
                history_subset = df.iloc[:1]
                s_date = df.iloc[0]['Timestamp']
            
            gap_start = s_date
            gap_end = e_date
        except Exception as e:
            print(f"Fallback to auto-detection due to: {e}")
            gap_start = None 

    if gap_start is None:
        df['Time Diff'] = df['Timestamp'].diff().dt.total_seconds() / 3600.0
        if len(df) > 1:
            valid_gaps = df['Time Diff'].dropna()
            if not valid_gaps.empty:
                max_gap_idx = valid_gaps.idxmax()
                gap_start = df.loc[max_gap_idx-1, 'Timestamp']
                gap_end = df.loc[max_gap_idx, 'Timestamp']
                target_lon = df.loc[max_gap_idx, 'Longitude']
                target_lat = df.loc[max_gap_idx, 'Latitude']
                history_subset = df.loc[:max_gap_idx-1]
                print(f"Auto-detected Gap: {gap_start} -> {gap_end}")

    if gap_start and gap_end and not history_subset.empty:
        print(f"Interpolating from {gap_start} to {gap_end}")
        print(f"Target: Lat {target_lat}, Lon {target_lon}")
        
        df_interp = predict_between_dates(
            gap_start, gap_end, history_subset, 
            file_rawdata_columns, model, (scaler_X, scaler_y), 
            current_animal, device, target_lon, target_lat
        )
        
        results_dir = results_folder(file_rawdata_name)
        interpolation_dir = os.path.join(results_dir, 'Interpolation')
        out_path = os.path.join(interpolation_dir, f'map_{current_animal}_interpolation_nhits.csv')
        df_interp.to_csv(out_path, index=False)
        print(f"Saved to {out_path}")
    else:
        print("Could not determine valid interpolation interval.")