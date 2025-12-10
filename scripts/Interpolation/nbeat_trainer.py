# nbeat_trainer.py
import os
import torch
import torch.nn as nn
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error

from Common.utils import results_folder, read_field_from_json, TRAINNING_SET
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
from Interpolation.nbeat_model import NBeats

# =============================================================================
# PREPARAÇÃO DE DADOS (USANDO DELTAS)
# =============================================================================
def prepare_training_data(df, file_rawdata_name, file_rawdata_columns):
    if df is None or len(df) == 0:
        return None, None

    # Normalizar nomes
    df.columns = [str(c) for c in df.columns]

    # Encontrar Timestamp
    timestamp_col = None
    for col in df.columns:
        if any(k in col.lower() for k in ('timestamp', 'date', 'datetime', 'time')):
            timestamp_col = col
            break
    if timestamp_col and timestamp_col != 'Timestamp':
        df = df.rename(columns={timestamp_col: 'Timestamp'})

    # Converter Timestamp
    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce') # Força coerce para garantir
    df = df.dropna(subset=['Timestamp']).sort_values('Timestamp').reset_index(drop=True)

    if df.empty: return None, None

    # --- ENGENHARIA DE FEATURES (DELTAS) ---
    # Calcular diferenças de tempo e posição
    df['TimeDiff'] = df['Timestamp'].diff().dt.total_seconds() / 3600 # Horas
    df['LonDiff'] = df['Longitude'].diff()
    df['LatDiff'] = df['Latitude'].diff()
    
    # Feature anterior (Input do modelo)
    df['PrevTimeDiff'] = df['TimeDiff'].shift(1)
    df['PrevLonDiff'] = df['LonDiff'].shift(1) # Opcional: usar delta anterior ajuda a manter inércia
    df['PrevLon'] = df['Longitude'].shift(1)     # Posição absoluta anterior (contexto)
    df['PrevLat'] = df['Latitude'].shift(1)      # Posição absoluta anterior (contexto)

    # Remover NaNs gerados pelos shifts (primeiras 2 linhas)
    df = df.dropna().reset_index(drop=True)
    
    # Features (Entrada): Contexto absoluto + Velocidade anterior
    # X = [PrevTimeDiff, PrevLon, PrevLat]
    features_cols = ['PrevTimeDiff', 'PrevLon', 'PrevLat']
    
    # Targets (Saída): O quanto variou (Delta)
    # y = [TimeDiff, LonDiff, LatDiff]
    targets_cols = ['TimeDiff', 'LonDiff', 'LatDiff']

    # Validar numéricos
    for c in features_cols + targets_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    
    df = df.dropna(subset=features_cols + targets_cols)
    if df.empty: return None, None

    X = df[features_cols].values.astype(np.float32)
    y = df[targets_cols].values.astype(np.float32)

    return X, y

# =============================================================================
# TREINAMENTO
# =============================================================================
def train_nbeats_model(df_train, df_eval, file_rawdata_name, file_rawdata_columns, epochs=150, lr=0.0001):
    
    print("[NBEATS] Preparing Training Data (Deltas)...")
    X_train, y_train = prepare_training_data(df_train, file_rawdata_name, file_rawdata_columns)
    
    if X_train is None:
        print("Training data empty.")
        return

    # Usar StandardScaler (melhor para Deltas que variam em torno de 0)
    scaler_X = StandardScaler()
    scaler_y = StandardScaler()

    X_train_scaled = scaler_X.fit_transform(X_train)
    y_train_scaled = scaler_y.fit_transform(y_train)

    # Preparar Eval se existir
    if df_eval is not None and len(df_eval) > 0:
        X_eval, y_eval = prepare_training_data(df_eval, file_rawdata_name, file_rawdata_columns)
        if X_eval is not None:
            X_eval_scaled = scaler_X.transform(X_eval)
            # Não precisamos escalar y_eval para o loop de validação simples aqui, mas ajuda se quiser calcular loss
        else:
            X_eval_scaled = None
    else:
        X_eval_scaled = None

    # Tensores
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"[NBEATS] Device: {device}")
    
    X_tensor = torch.tensor(X_train_scaled, dtype=torch.float32).to(device)
    y_tensor = torch.tensor(y_train_scaled, dtype=torch.float32).to(device)

    # Modelo
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    input_dim = X_train.shape[1] # 3
    output_dim = y_train.shape[1] # 3
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat") or 64
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat") or 2
    
    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks).to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    # Loop
    print(f"[NBEATS] Starting training for {epochs} epochs...")
    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        output = model(X_tensor)
        loss = criterion(output, y_tensor)
        loss.backward()
        optimizer.step()

        if epoch % 20 == 0:
            print(f"Epoch {epoch} | Loss: {loss.item():.6f}")

    # Salvar
    models_dir = os.path.join(script_dir, 'models')
    os.makedirs(models_dir, exist_ok=True)
    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    
    torch.save({'model_state_dict': model.state_dict()}, os.path.join(models_dir, f'nbeats_model_general_{filename}.pth'))
    joblib.dump(scaler_X, os.path.join(models_dir, f'scaler_x_{filename}.pkl'))
    joblib.dump(scaler_y, os.path.join(models_dir, f'scaler_y_{filename}.pkl'))
    
    print("✅ Model and Delta-Scalers saved.")

# Wrappers para compatibilidade
def get_train_eval(df_full):
    df_full = df_full.sort_values(by='Timestamp').reset_index(drop=True)
    split_index = int(TRAINNING_SET * len(df_full))
    return df_full.iloc[:split_index].copy(), df_full.iloc[split_index:].copy()

def train_nbeats_model_single(current_animal, file_rawdata_name, file_rawdata_columns):
    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    try:
        df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
        df_train, df_eval = get_train_eval(df)
        train_nbeats_model(df_train, df_eval, file_rawdata_name, file_rawdata_columns)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 4:
        train_nbeats_model_single(sys.argv[1], sys.argv[2], sys.argv[3])