# nbeat_trainer.py
import os
import json
import torch
import torch.nn as nn
import pandas as pd
import numpy as np
import joblib
import random
from pyproj import Transformer
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
from torch.utils.data import TensorDataset, DataLoader
from torch.nn.utils import clip_grad_norm_

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

    # --- PROJEÇÃO GEOESPACIAL E ENGENHARIA DE FEATURES (DELTAS EM METROS) ---
    # Calcular diferenças de tempo
    df['TimeDiff'] = df['Timestamp'].diff().dt.total_seconds() / 3600.0 # Horas

    # Converter para coordenadas UTM (zone centrado na série)
    # Usamos a mediana para definir zona UTM/hemisfério
    median_lon = df['Longitude'].median()
    median_lat = df['Latitude'].median()
    try:
        zone = int((median_lon + 180) / 6) + 1
    except Exception:
        zone = 23
    is_northern = True if median_lat >= 0 else False
    utm_epsg = 32600 + zone if is_northern else 32700 + zone
    transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{utm_epsg}", always_xy=True)

    # Criar colunas E, N (metros)
    e, n = transformer.transform(df['Longitude'].values, df['Latitude'].values)
    df['E'] = e
    df['N'] = n

    # Deltas em metros
    df['EDiff'] = df['E'].diff()
    df['NDiff'] = df['N'].diff()

    # Features anteriores (Inputs do modelo)
    df['PrevTimeDiff'] = df['TimeDiff'].shift(1)
    df['PrevE'] = df['E'].shift(1)
    df['PrevN'] = df['N'].shift(1)

    # Remover NaNs gerados pelos shifts (primeiras linhas)
    df = df.dropna().reset_index(drop=True)

    # Features (Entrada): [PrevTimeDiff, PrevE, PrevN]
    features_cols = ['PrevTimeDiff', 'PrevE', 'PrevN']

    # Targets (Saída): [TimeDiff, EDiff, NDiff]
    targets_cols = ['TimeDiff', 'EDiff', 'NDiff']

    # Validar numéricos
    for c in features_cols + targets_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    
    df = df.dropna(subset=features_cols + targets_cols)
    if df.empty: return None, None

    X = df[features_cols].values.astype(np.float32)
    y = df[targets_cols].values.astype(np.float32)

    # Anexar info de projeção para uso posterior
    try:
        df.attrs['utm_epsg'] = int(utm_epsg)
    except Exception:
        pass

    return X, y

# =============================================================================
# TREINAMENTO
# =============================================================================
def train_nbeats_model(df_train, df_eval, file_rawdata_name, file_rawdata_columns, epochs=None, lr=None, seed=42):

    print("[NBEATS] Preparing Training Data (Deltas) and splits (70/15/15)...")

    # Carregar hyperparams
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    if epochs is None:
        epochs = read_field_from_json(hyperparam_path, 'epochs_nbeats') or 150
    if lr is None:
        lr = read_field_from_json(hyperparam_path, 'lr_nbeats') or 1e-4

    batch_size = read_field_from_json(hyperparam_path, 'batch_size_nbeat') or 32
    weight_decay = read_field_from_json(hyperparam_path, 'weight_decay_nbeat') or 0.0
    beta1 = read_field_from_json(hyperparam_path, 'beta1_nbeats') or 0.9
    beta2 = read_field_from_json(hyperparam_path, 'beta2_nbeats') or 0.999
    scheduler_factor = read_field_from_json(hyperparam_path, 'scheduler_factor_nbeats') or 0.5
    scheduler_patience = read_field_from_json(hyperparam_path, 'scheduler_patience_nbeats') or 5
    scheduler_min_lr = read_field_from_json(hyperparam_path, 'scheduler_min_lr_nbeats') or 1e-6
    patience = read_field_from_json(hyperparam_path, 'patience_nbeats') or 20
    clip_value = read_field_from_json(hyperparam_path, 'clip_grad_nbeats') or 1.0

    # Seed
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)

    # Concatenar df_train e df_eval para garantir splits temporais contíguos
    if df_eval is not None and len(df_eval) > 0:
        df_full = pd.concat([df_train, df_eval], axis=0).sort_values('Timestamp').reset_index(drop=True)
    else:
        df_full = df_train.sort_values('Timestamp').reset_index(drop=True)

    # Splits 70/15/15 (usar constantes de Common.utils)
    from Common.utils import TRAINNING_SET, VALIDATION_SET, TESTING_SET
    n = len(df_full)
    i_train = int(TRAINNING_SET * n)
    i_val = i_train + int(VALIDATION_SET * n)

    df_t = df_full.iloc[:i_train].copy()
    df_v = df_full.iloc[i_train:i_val].copy()
    df_te = df_full.iloc[i_val:].copy()

    # Preparar arrays via prepare_training_data
    X_train, y_train = prepare_training_data(df_t, file_rawdata_name, file_rawdata_columns)
    X_val, y_val = prepare_training_data(df_v, file_rawdata_name, file_rawdata_columns)
    X_test, y_test = prepare_training_data(df_te, file_rawdata_name, file_rawdata_columns)

    if X_train is None:
        print("Training data empty.")
        return

    # Scalers
    scaler_X = StandardScaler()
    scaler_y = StandardScaler()

    X_train_scaled = scaler_X.fit_transform(X_train)
    y_train_scaled = scaler_y.fit_transform(y_train)

    X_val_scaled = scaler_X.transform(X_val) if X_val is not None else None
    y_val_scaled = scaler_y.transform(y_val) if y_val is not None else None
    X_test_scaled = scaler_X.transform(X_test) if X_test is not None else None
    y_test_scaled = scaler_y.transform(y_test) if y_test is not None else None

    # Tensores e loaders
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"[NBEATS] Device: {device}")

    X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32)
    y_train_tensor = torch.tensor(y_train_scaled, dtype=torch.float32)
    train_ds = TensorDataset(X_train_tensor, y_train_tensor)
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=False)

    if X_val_scaled is not None:
        X_val_tensor = torch.tensor(X_val_scaled, dtype=torch.float32)
        y_val_tensor = torch.tensor(y_val_scaled, dtype=torch.float32)
        val_ds = TensorDataset(X_val_tensor, y_val_tensor)
        val_loader = DataLoader(val_ds, batch_size=batch_size, shuffle=False)
    else:
        val_loader = None

    # Modelo
    input_dim = X_train.shape[1]
    output_dim = y_train.shape[1]
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat") or 64
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat") or 2

    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks).to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, betas=(beta1, beta2), weight_decay=weight_decay)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, factor=scheduler_factor, patience=scheduler_patience, min_lr=scheduler_min_lr)

    best_val = float('inf')
    best_epoch = 0
    history = {'train_loss': [], 'val_loss': []}

    print(f"[NBEATS] Starting training for {epochs} epochs...")
    for epoch in range(1, epochs+1):
        model.train()
        epoch_losses = []
        for xb, yb in train_loader:
            xb = xb.to(device)
            yb = yb.to(device)
            optimizer.zero_grad()
            out = model(xb)
            loss = criterion(out, yb)
            loss.backward()
            # gradient clipping
            clip_grad_norm_(model.parameters(), max_norm=clip_value)
            optimizer.step()
            epoch_losses.append(loss.item())
        train_loss = float(np.mean(epoch_losses)) if len(epoch_losses) > 0 else 0.0

        # Validação
        if val_loader is not None:
            model.eval()
            val_losses = []
            with torch.no_grad():
                for xb, yb in val_loader:
                    xb = xb.to(device)
                    yb = yb.to(device)
                    out = model(xb)
                    loss = criterion(out, yb)
                    val_losses.append(loss.item())
            val_loss = float(np.mean(val_losses)) if len(val_losses) > 0 else 0.0
        else:
            val_loss = train_loss

        history['train_loss'].append(train_loss)
        history['val_loss'].append(val_loss)

        # Scheduler step and early stopping
        scheduler.step(val_loss)

        if val_loss < best_val:
            best_val = val_loss
            best_epoch = epoch
            # salvar melhor modelo
            models_dir = os.path.join(script_dir, 'models')
            os.makedirs(models_dir, exist_ok=True)
            filename = file_rawdata_name.split('/')[-1].split('.')[0]
            torch.save({'model_state_dict': model.state_dict()}, os.path.join(models_dir, f'nbeats_model_general_{filename}.pth'))
            joblib.dump(scaler_X, os.path.join(models_dir, f'scaler_x_{filename}.pkl'))
            joblib.dump(scaler_y, os.path.join(models_dir, f'scaler_y_{filename}.pkl'))

        if epoch % 10 == 0 or epoch == 1:
            print(f"Epoch {epoch}/{epochs} | train_loss: {train_loss:.6f} | val_loss: {val_loss:.6f}")

        # early stopping
        if epoch - best_epoch > patience:
            print(f"Early stopping at epoch {epoch}. Best epoch {best_epoch} (val_loss={best_val:.6f})")
            break

    # Após treino, salvar metadata e histórico
    try:
        models_dir = os.path.join(script_dir, 'models')
        os.makedirs(models_dir, exist_ok=True)
        filename = file_rawdata_name.split('/')[-1].split('.')[0]
        metadata = {
            'filename': filename,
            'input_dim': int(input_dim),
            'output_dim': int(output_dim),
            'hidden_dim': int(hidden_dim),
            'num_blocks': int(num_blocks),
            'best_epoch': int(best_epoch),
            'best_val_loss': float(best_val),
            'seed': int(seed)
        }
        with open(os.path.join(models_dir, f'nbeats_metadata_{filename}.json'), 'w') as fh:
            json.dump(metadata, fh, indent=2)
        # salvar historico
        with open(os.path.join(models_dir, f'nbeats_history_{filename}.json'), 'w') as fh:
            json.dump(history, fh, indent=2)
    except Exception:
        pass

    print("✅ Training finished. Best epoch:", best_epoch)

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