# nbeat_interpolation.py
import pandas as pd
import torch
import numpy as np
import joblib
import os
import sys
from datetime import timedelta
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
from Common.utils import results_folder, remove_nan_data
from Interpolation.nbeat_model import NBeats
from Common.utils import read_field_from_json, TRAINNING_SET

def predict_between_dates(start_date, end_date, df, model, scaler_X, scaler_y, max_rows=None):
    new_rows = []
    current_timestamp = start_date
    rows_generated = 0
    
    # Pegar último registro válido
    last_row = df.iloc[-1]
    
    # Inicializar estado atual com valores absolutos
    curr_lon = last_row['Longitude']
    curr_lat = last_row['Latitude']
    
    # Tentar recuperar o último TimeDiff, se não existir (inicio), chutar média
    if 'Time Difference (hours)' in last_row:
        prev_time_diff = last_row['Time Difference (hours)']
    else:
        prev_time_diff = 0.5 # Chute inicial seguro (30 min)

    device = next(model.parameters()).device

    while current_timestamp <= end_date:
        if max_rows and rows_generated >= max_rows: break

        # Preparar Input: [PrevTimeDiff, PrevLon, PrevLat]
        # PrevLon e PrevLat são ABSOLUTOS aqui, pois o Scaler X cuida de normalizá-los
        input_feat = np.array([[prev_time_diff, curr_lon, curr_lat]])
        
        # Escalar
        input_scaled = scaler_X.transform(input_feat)
        input_tensor = torch.tensor(input_scaled, dtype=torch.float32).to(device)
        
        # Prever Delta
        model.eval()
        with torch.no_grad():
            pred_scaled = model(input_tensor).cpu().numpy()
        
        # Desescalar Delta
        pred_deltas = scaler_y.inverse_transform(pred_scaled).flatten()
        
        # Extrair Deltas previstos
        # pred_deltas = [TimeDiff, LonDiff, LatDiff]
        pred_time_diff = float(pred_deltas[0])
        pred_lon_diff = float(pred_deltas[1])
        pred_lat_diff = float(pred_deltas[2])

        # Garantir tempo positivo (mínimo 1 minuto = 0.016h)
        if pred_time_diff <= 0.01: 
            pred_time_diff = 0.1 # Fallback suave
            
        # ATUALIZAR ESTADO (Soma o Delta à posição anterior)
        new_timestamp = current_timestamp + timedelta(hours=pred_time_diff)
        curr_lon = curr_lon + pred_lon_diff
        curr_lat = curr_lat + pred_lat_diff
        prev_time_diff = pred_time_diff # Atualiza para o próximo loop

        # Salvar
        new_row = {
            'ID': last_row['ID'],
            'Timestamp': new_timestamp,
            'Longitude': curr_lon,
            'Latitude': curr_lat
        }
        new_rows.append(new_row)
        
        current_timestamp = new_timestamp
        rows_generated += 1

    return pd.DataFrame(new_rows)

def run(current_animal, number_of_predictions, file_rawdata_name, file_rawdata_columns):
    # Carregar dados iniciais
    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    
    # Preparar timestamp para pegar último ponto
    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask, errors='coerce')
    df = df.dropna(subset=['Timestamp']).sort_values('Timestamp')
    
    # Calcular TimeDiff inicial para o dataframe histórico (necessário para o primeiro input)
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600
    df = df.dropna().reset_index(drop=True)

    # Carregar Modelo e Scalers
    script_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = os.path.join(script_dir, 'models')
    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    
    model_path = os.path.join(models_dir, f'nbeats_model_general_{filename}.pth')
    scaler_x_path = os.path.join(models_dir, f'scaler_x_{filename}.pkl')
    scaler_y_path = os.path.join(models_dir, f'scaler_y_{filename}.pkl')

    if not os.path.exists(model_path):
        print("Model/Scalers not found. Run training first.")
        return

    # Instanciar Modelo
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat") or 64
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat") or 2
    
    model = NBeats(3, 3, hidden_dim, num_blocks) # 3 in, 3 out
    model.load_state_dict(torch.load(model_path, map_location='cpu')['model_state_dict'])
    
    scaler_X = joblib.load(scaler_x_path)
    scaler_y = joblib.load(scaler_y_path)

    # Definir datas
    start_date = df.iloc[-1]['Timestamp']
    end_date = start_date + timedelta(days=60) # Exemplo: interpolar 2 meses

    # Prever
    print(f"Interpolating for animal {current_animal}...")
    pred_df = predict_between_dates(start_date, end_date, df, model, scaler_X, scaler_y, max_rows=2000)
    
    # Salvar
    out_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats.csv')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    
    # Formatar para salvar (ID, Timestamp, Lon, Lat)
    pred_df['Timestamp'] = pred_df['Timestamp'].dt.strftime(mask if mask else '%Y-%m-%d %H:%M:%S')
    pred_df[['ID', 'Timestamp', 'Longitude', 'Latitude']].to_csv(out_path, index=False, header=False)
    print(f"Saved to {out_path}")

def run_mock():
    if len(sys.argv) >= 3:
        run(sys.argv[1], 0, sys.argv[2], sys.argv[3])