# nbeat_trainer.py
import os
import torch
import torch.nn as nn
import pandas as pd
import numpy as np
import joblib  # Necessário para salvar os scalers
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error

from Common.utils import results_folder, read_field_from_json, TRAINNING_SET
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
from Interpolation.nbeat_model import NBeats

# =============================================================================
# PREPARAÇÃO DE DADOS (Retorna Numpy para permitir Scaling)
# =============================================================================
def prepare_training_data(df, file_rawdata_name, file_rawdata_columns):
    """
    Prepara os dados e retorna Numpy Arrays (X, y) para serem normalizados.
    """
    if df is None or len(df) == 0:
        return None, None

    # Normalizar nomes de colunas
    df.columns = [str(c) for c in df.columns]

    # Tenta encontrar coluna de timestamp
    timestamp_col = None
    for col in df.columns:
        low = col.lower()
        if any(k in low for k in ('timestamp', 'date', 'datetime', 'time')):
            timestamp_col = col
            break

    if timestamp_col is None and 'Timestamp' in df.columns:
        timestamp_col = 'Timestamp'

    if timestamp_col is None:
        print("[NBEATS] ERROR: no timestamp-like column found.")
        return None, None

    if timestamp_col != 'Timestamp':
        df = df.rename(columns={timestamp_col: 'Timestamp'})

    # Limpeza e conversão de Timestamp
    df['Timestamp'] = df['Timestamp'].astype(str).str.strip()
    header_mask = df['Timestamp'].str.lower().isin(['timestamp', 'datetime', 'date', 'time'])
    if header_mask.any():
        df = df.loc[~header_mask].reset_index(drop=True)

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    if mask:
        try:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
        except:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
    else:
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')

    df = df.dropna(subset=['Timestamp']).reset_index(drop=True)
    if df.empty: return None, None

    # Engenharia de Features
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600
    df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)
    
    # Remove NaNs gerados pelo diff/shift
    df = df.dropna(subset=['Time Difference (hours)', 'Prev Time Difference (hours)']).reset_index(drop=True)
    if df.empty: return None, None

    # Definição de Features e Targets
    features = ['Prev Time Difference (hours)', 'Longitude', 'Latitude']
    targets = ['Time Difference (hours)', 'Longitude', 'Latitude']

    # Coerção numérica
    df[features] = df[features].apply(pd.to_numeric, errors='coerce')
    for t in targets:
        df[t] = pd.to_numeric(df[t], errors='coerce')

    valid_mask = ~(df[features].isna().any(axis=1) | df[targets].isna().any(axis=1))
    df = df.loc[valid_mask].reset_index(drop=True)

    if df.empty: return None, None

    # Retorna Numpy Arrays (float32)
    X = df[features].values.astype(np.float32)
    y = df[targets].values.astype(np.float32)

    return X, y

# =============================================================================
# FUNÇÕES AUXILIARES DE AVALIAÇÃO
# =============================================================================
def calculate_metrics_unscaled(y_true, y_pred):
    """Calcula métricas nos dados REAIS (desnormalizados)"""
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    
    # MAPE seguro (apenas para TimeDiff, geralmente índice 0)
    # Aqui calculamos geral, mas cuidado com zeros
    mask = y_true != 0
    if np.any(mask):
        mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    else:
        mape = float('inf')
    
    return mae, rmse, mape

# =============================================================================
# FUNÇÃO PRINCIPAL DE TREINAMENTO
# =============================================================================
def train_nbeats_model(df_train, df_eval, file_rawdata_name, file_rawdata_columns, epochs=100, lr=0.0001):
    
    # 1. Obter dados em formato Numpy
    print("[NBEATS] Preparing Training Data...")
    X_train_raw, y_train_raw = prepare_training_data(df_train, file_rawdata_name, file_rawdata_columns)
    
    print("[NBEATS] Preparing Evaluation Data...")
    X_eval_raw, y_eval_raw = prepare_training_data(df_eval, file_rawdata_name, file_rawdata_columns)

    if X_train_raw is None or y_train_raw is None:
        print("Training data is empty. Skipping.")
        return

    # 2. Configurar e Ajustar Scalers (MUITO IMPORTANTE)
    # Normaliza para range [0, 1]. Isso ajuda o modelo a lidar com Lat/Lon negativas.
    scaler_X = MinMaxScaler(feature_range=(0, 1))
    scaler_y = MinMaxScaler(feature_range=(0, 1))

    # Fit apenas no treino para evitar vazamento de dados
    X_train_scaled = scaler_X.fit_transform(X_train_raw)
    y_train_scaled = scaler_y.fit_transform(y_train_raw)

    # Transformar eval usando os scalers do treino
    if X_eval_raw is not None:
        X_eval_scaled = scaler_X.transform(X_eval_raw)
        y_eval_scaled = scaler_y.transform(y_eval_raw)
    else:
        X_eval_scaled, y_eval_scaled = None, None

    # 3. Converter para Tensores PyTorch
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"[NBEATS] Training on device: {device}")

    X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32).to(device)
    y_train_tensor = torch.tensor(y_train_scaled, dtype=torch.float32).to(device)
    
    if X_eval_scaled is not None:
        X_eval_tensor = torch.tensor(X_eval_scaled, dtype=torch.float32).to(device)
        # Mantemos y_eval_raw para calcular métricas reais depois, não precisamos do tensor para loss de validação obrigatoriamente
    
    # 4. Configuração do Modelo
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    input_dim = X_train_tensor.shape[1]
    output_dim = 3
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat") or 32
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat") or 2
    
    # Otimização
    beta1 = read_field_from_json(hyperparam_path, 'beta1_nbeats') or 0.9
    beta2 = read_field_from_json(hyperparam_path, 'beta2_nbeats') or 0.999
    weight_decay = read_field_from_json(hyperparam_path, 'weight_decay_nbeat') or 0.0001
    
    print(f"Model architecture: in={input_dim}, out={output_dim}, hidden={hidden_dim}, blocks={num_blocks}")

    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks).to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, betas=(beta1, beta2), weight_decay=weight_decay)

    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    log_file_path = "training_log_nbeat.txt"
    
    best_train_mae = float('inf')

    # 5. Loop de Treinamento
    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        
        # Forward pass
        forecast_scaled = model(X_train_tensor)
        loss = criterion(forecast_scaled, y_train_tensor)
        
        loss.backward()
        optimizer.step()
        
        # Logs e Métricas (a cada 20 epochs)
        if epoch % 20 == 0 or epoch == epochs - 1:
            model.eval()
            with torch.no_grad():
                # Predição escalada
                train_pred_scaled = model(X_train_tensor).cpu().numpy()
                
                # INVERSE TRANSFORM para métricas reais
                train_pred_real = scaler_y.inverse_transform(train_pred_scaled)
                # y_train_raw já está em numpy e escala real
                
                # Calcular MAE por componente
                mae_time = mean_absolute_error(y_train_raw[:, 0], train_pred_real[:, 0])
                mae_lon  = mean_absolute_error(y_train_raw[:, 1], train_pred_real[:, 1])
                mae_lat  = mean_absolute_error(y_train_raw[:, 2], train_pred_real[:, 2])
                
                total_mae = (mae_time + mae_lon + mae_lat) / 3
                
                if total_mae < best_train_mae:
                    best_train_mae = total_mae
                
                log_msg = (f"[{filename}] Epoch {epoch:3d} | Loss(MSE): {loss.item():.5f} | "
                           f"Real MAE: Time={mae_time:.3f}h, Lon={mae_lon:.5f}, Lat={mae_lat:.5f}")
                print(log_msg)
                
                with open(log_file_path, "a") as f:
                    f.write(log_msg + "\n")
            model.train()

    # 6. Salvar Modelo e Scalers
    models_dir = os.path.join(script_dir, 'models')
    os.makedirs(models_dir, exist_ok=True)
    
    model_path = os.path.join(models_dir, f'nbeats_model_general_{filename}.pth')
    scaler_x_path = os.path.join(models_dir, f'scaler_x_{filename}.pkl')
    scaler_y_path = os.path.join(models_dir, f'scaler_y_{filename}.pkl')
    
    torch.save({'model_state_dict': model.state_dict()}, model_path)
    joblib.dump(scaler_X, scaler_x_path)
    joblib.dump(scaler_y, scaler_y_path)
    
    print(f"✅ Model saved to {model_path}")
    print(f"✅ Scalers saved to {scaler_x_path} and {scaler_y_path}")

    # 7. Avaliação Final no Test Set
    if X_eval_scaled is not None:
        evaluate_on_test(model, X_eval_tensor, y_eval_raw, scaler_y, results_folder(file_rawdata_name), best_train_mae)

def evaluate_on_test(model, X_eval_tensor, y_eval_real, scaler_y, results_dir, best_train_mae):
    model.eval()
    with torch.no_grad():
        pred_scaled = model(X_eval_tensor).cpu().numpy()
    
    # Desnormalizar
    pred_real = scaler_y.inverse_transform(pred_scaled)
    
    # Calcular métricas
    mae, rmse, mape = calculate_metrics_unscaled(y_eval_real.flatten(), pred_real.flatten())
    
    print("\n" + "="*60)
    print("📊 EVALUATION RESULTS (Real Scale)")
    print(f"  MAE:  {mae:.4f}")
    print(f"  RMSE: {rmse:.4f}")
    print("="*60)

    # Salvar hiperparâmetros/resultados
    hiper_path = os.path.join(results_dir, 'hiperparameters.txt')
    with open(hiper_path, "a") as f:
        f.write("\n# N-BEATS Evaluation\n")
        f.write(f"Eval MAE: {mae:.4f}\n")
        f.write(f"Eval RMSE: {rmse:.4f}\n")
        f.write(f"Best Train MAE: {best_train_mae:.4f}\n")

# =============================================================================
# WRAPPERS PARA CHAMADA EXTERNA
# =============================================================================
def get_train_eval(df_full):
    df_full = df_full.sort_values(by='Timestamp').reset_index(drop=True)
    split_index = int(TRAINNING_SET * len(df_full))
    df_train = df_full.iloc[:split_index].copy()
    df_eval  = df_full.iloc[split_index:].copy()
    return df_train, df_eval

def train_nbeats_model_single(current_animal, file_rawdata_name, file_rawdata_columns):
    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    
    try:
        df_full = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return

    df_train, df_eval = get_train_eval(df_full)
    
    train_nbeats_model(df_train, df_eval, file_rawdata_name, file_rawdata_columns)

def train_nbeats_model_list(animal_list, file_rawdata_name, file_rawdata_columns):
    results_dir = results_folder(file_rawdata_name)
    combined_df_list = []

    for current_animal in animal_list:
        file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
        try:
            df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
            combined_df_list.append(df)
        except:
            continue

    if not combined_df_list:
        print("No data found for list.")
        return

    combined_df = pd.concat(combined_df_list, ignore_index=True)
    df_train, df_eval = get_train_eval(combined_df)

    data_prep_dir = os.path.join(os.path.dirname(__file__), '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    lr = read_field_from_json(hyperparam_path, "lr_nbeats") or 0.0001

    train_nbeats_model(df_train, df_eval, file_rawdata_name, file_rawdata_columns, lr=lr)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python nbeat_trainer.py <animal_id> <rawdata_csv> <columns_json>")
        sys.exit(1)

    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    file_rawdata_columns = sys.argv[3]

    train_nbeats_model_single(current_animal, file_rawdata_name, file_rawdata_columns)