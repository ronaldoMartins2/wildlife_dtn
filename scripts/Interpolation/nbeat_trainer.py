import os
import torch
import torch.nn as nn
import pandas as pd
import numpy as np
import joblib
from torch.utils.data import TensorDataset, DataLoader
from sklearn.preprocessing import RobustScaler  # MUDANÇA: RobustScaler em vez de StandardScaler

from Common.utils import results_folder, read_field_from_json, TRAINNING_SET, remove_nan_data
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
from Interpolation.nbeat_model import NBeats

def prepare_training_data_scaled(df, file_rawdata_name, file_rawdata_columns, scaler=None, is_training=True):
    df = remove_nan_data(df, file_rawdata_columns)
    
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.sort_values(by='Timestamp')

    # --- 1. LIMPEZA PRÉVIA DE OUTLIERS FÍSICOS ---
    # Antes mesmo de calcular deltas, removemos coordenadas que não existem
    # Latitudes devem estar entre -90 e 90. Longitudes entre -180 e 180.
    df = df[
        (df['Latitude'] >= -90) & (df['Latitude'] <= 90) & 
        (df['Longitude'] >= -180) & (df['Longitude'] <= 180)
    ]

    # 2. Calcular DELTAS
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600.0
    df['Delta Longitude'] = df['Longitude'].diff()
    df['Delta Latitude'] = df['Latitude'].diff()

    # --- 2. FILTRO DE VELOCIDADE IMPOSSÍVEL NO TREINO ---
    # Se o animal "andou" mais de 1 grau (aprox 111km) em um passo, removemos essa linha do treino.
    # Isso impede o modelo de aprender teletransportes.
    df = df[
        (df['Delta Longitude'].abs() < 1.0) & 
        (df['Delta Latitude'].abs() < 1.0) &
        (df['Time Difference (hours)'] > 0) # Remove tempos negativos ou zero
    ]

    # 3. Shift para Input
    df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)
    df['Prev Delta Longitude'] = df['Delta Longitude'].shift(1)
    df['Prev Delta Latitude'] = df['Delta Latitude'].shift(1)

    features = ['Prev Time Difference (hours)', 'Prev Delta Longitude', 'Prev Delta Latitude']
    target = ['Time Difference (hours)', 'Delta Longitude', 'Delta Latitude'] 

    df = df.dropna(subset=features + target)

    X = df[features].values
    y = df[target].values 
    
    # --- 3. USANDO ROBUST SCALER ---
    # O RobustScaler usa quartis, então outliers extremos não estragam a escala.
    if is_training:
        scaler_X = RobustScaler()
        scaler_y = RobustScaler()
        X_scaled = scaler_X.fit_transform(X)
        y_scaled = scaler_y.fit_transform(y)
        return torch.tensor(X_scaled, dtype=torch.float32), torch.tensor(y_scaled, dtype=torch.float32), scaler_X, scaler_y
    else:
        if scaler is None: raise ValueError("Scaler required")
        scaler_X, scaler_y = scaler
        X_scaled = scaler_X.transform(X)
        y_scaled = scaler_y.transform(y)
        return torch.tensor(X_scaled, dtype=torch.float32), torch.tensor(y_scaled, dtype=torch.float32)

def train_nbeats_model(df_train, df_eval, file_rawdata_name, file_rawdata_columns, epochs=100, lr=0.001):
    # Garante reset dos índices
    df_train = df_train.reset_index(drop=True)
    
    X_tensor, y_tensor, scaler_X, scaler_y = prepare_training_data_scaled(df_train, file_rawdata_name, file_rawdata_columns, is_training=True)

    if len(X_tensor) < 10:
        print("Not enough clean data for training.")
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    models_dir = os.path.join(script_dir, '..', 'Interpolation/models')
    if not os.path.exists(models_dir): os.makedirs(models_dir)
    
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    input_dim = X_tensor.shape[1]
    output_dim = y_tensor.shape[1]
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat")
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat")
    dropout_rate = read_field_from_json(hyperparam_path, "dropout_rate_nbeat") or 0.1
    batch_size = read_field_from_json(hyperparam_path, "batch_size_nbeat") or 32
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks, dropout=dropout_rate)
    model.to(device)
    
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    
    dataset = TensorDataset(X_tensor.to(device), y_tensor.to(device))
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)
    
    print(f"Training on {device} with RobustScaler (Clean Data)...")

    for epoch in range(epochs):
        model.train()
        epoch_loss = 0
        for batch_X, batch_y in dataloader:
            optimizer.zero_grad()
            output = model(batch_X)
            loss = criterion(output, batch_y)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
        
        if epoch % 10 == 0:
            print(f"Epoch {epoch} | Loss: {epoch_loss / len(dataloader):.6f}")

    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    model_path = os.path.join(models_dir, f'nbeats_model_general_{filename}.pth')
    scaler_path = os.path.join(models_dir, f'nbeats_scaler_{filename}.pkl')
    
    torch.save({'model_state_dict': model.state_dict()}, model_path)
    joblib.dump((scaler_X, scaler_y), scaler_path)
    
    print(f"Model saved to {model_path}")

def train_nbeats_model_list(list_animals, file_rawdata, file_rawdata_columns):
    results_dir = results_folder(file_rawdata)
    all_dfs = []
    for animal in list_animals:
        p = os.path.join(results_dir, f'map_{animal}.csv')
        if os.path.exists(p):
            try:
                # Tenta ler ignorando linhas podres se houver
                df = pd.read_csv(p, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'], on_bad_lines='skip')
                all_dfs.append(df)
            except Exception as e:
                print(f"Skipping bad file {p}: {e}")

    if not all_dfs: return
    full_df = pd.concat(all_dfs, ignore_index=True)
    train_nbeats_model(full_df, None, file_rawdata, file_rawdata_columns)