# nbeat_trainer.py
import os
import torch
import torch.nn as nn
import pandas as pd

from Common.utils import results_folder, read_field_from_json, TRAINNING_SET, remove_nan_data
from Data_preparation.clear_outtliers import run as run_clear_outliers
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
from Interpolation.nbeat_model import NBeats

from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split
import numpy as np

def evaluate_nbeats_model(model, df, file_rawdata_columns):
    # Recriar features e targets
    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600
    df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)
    df = df.dropna(subset=['Time Difference (hours)', 'Prev Time Difference (hours)', 'Longitude', 'Latitude'])

    features = ['Prev Time Difference (hours)', 'Longitude', 'Latitude']
    target = ['Time Difference (hours)', 'Longitude', 'Latitude'] # <-- Alvo correto

    X = df[features].values
    y_true = df[target].values 

    if len(X) == 0:
        print("❌ Not enough data to evaluate test set.")
        return None # Retornar None em caso de falha

    X_tensor = torch.tensor(X, dtype=torch.float32)

    model.eval()
    with torch.no_grad():
        predictions = model(X_tensor) # Saída será (batch_size, 3)
        predictions_np = predictions.cpu().numpy()

    # Calcular métricas
    
    # Métricas Gerais (RMSE e MAE sobre todos os 3 valores)
    mae_overall = mean_absolute_error(y_true, predictions_np)
    rmse_overall = np.sqrt(mean_squared_error(y_true, predictions_np))
    
    # Métricas Específicas (MAPE apenas para TimeDiff)
    y_true_time = y_true[:, 0]
    pred_time = predictions_np[:, 0]
    
    mask = y_true_time != 0
    if np.any(mask):
        mape_time = np.mean(np.abs((y_true_time[mask] - pred_time[mask]) / y_true_time[mask])) * 100
    else:
        mape_time = float('inf')

    print(f"📊 N-BEATS Test Evaluation (Multivariate):")
    print(f"Overall MAE: {mae_overall:.4f} | Overall RMSE: {rmse_overall:.4f} | TimeDiff MAPE: {mape_time:.2f}%")

    # Retorne as métricas principais para log
    return mae_overall, rmse_overall, mape_time

def prepare_training_data(df, file_rawdata_name, file_rawdata_columns):

    if df.empty:
        return None, None
    
    df = remove_nan_data(df)
    
    print("Inside nbeat_trainer")

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600
    df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)

    df = df.dropna(subset=['Time Difference (hours)', 'Prev Time Difference (hours)', 'Longitude', 'Latitude'])
    
    features = ['Prev Time Difference (hours)', 'Longitude', 'Latitude']
    
    # === CORREÇÃO CRÍTICA AQUI ===
    # O alvo deve ser a 'Time Difference (hours)' (a próxima), não a 'Prev'
    target = ['Time Difference (hours)', 'Longitude', 'Latitude']

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
        file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
        df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
        combined_df_list.append(df)

    combined_df = pd.concat(combined_df_list, ignore_index=True)
    
    df_train, df_eval = get_train_eval( combined_df )

    data_prep_dir = os.path.join(os.path.dirname(__file__), '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    
    lr_ratting = read_field_from_json(hyperparam_path, "lr_nbeats")
    patience_nbeats = read_field_from_json(hyperparam_path, "patience_nbeats") # Assumindo que você adicione isso
    epochs_nbeats = read_field_from_json(hyperparam_path, "epochs_nbeats") # Assumindo que você adicione isso

    if lr_ratting is None:
        print(f"Learn ratting de N-Beats não encontrada no arquivo de hiperparâmetros")
        return 
    
    # Use valores padrão se não encontrados
    if patience_nbeats is None:
        patience_nbeats = 10
    if epochs_nbeats is None:
        epochs_nbeats = 100

    train_nbeats_model( df_train,
                        df_eval,
                        file_rawdata_name, 
                        file_rawdata_columns,
                        epochs=epochs_nbeats,
                        lr=lr_ratting,
                        patience=patience_nbeats)

def get_train_eval( df_full ):
    # Sort by timestamp to maintain time series order
    df_full = df_full.sort_values(by='Timestamp').reset_index(drop=True)

    # Split index
    split_index = int(TRAINNING_SET * len(df_full))

    # 80% for training, 20% for evaluation
    df_train = df_full.iloc[:split_index].copy()
    df_eval  = df_full.iloc[split_index:].copy()

    return df_train, df_eval

# ... (função train_nbeats_model_single omitida por brevidade, precisa ser atualizada como train_nbeats_model_list) ...

# Esta função 'calculate_metrics' SÓ funciona para 1D.
# Vamos mantê-la, mas usá-la com cuidado.
def calculate_metrics(y_true, y_pred):
    """Calculate MAE, RMSE, and MAPE metrics for 1D arrays"""
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    
    mask = y_true != 0
    if np.any(mask):
        mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    else:
        mape = float('inf')
    
    return mae, rmse, mape

def train_nbeats_model( df_train,
                        df_eval,
                        file_rawdata_name, 
                        file_rawdata_columns, 
                        epochs=100, 
                        lr=0.001,
                        patience=10):

    X_tensor_full, y_tensor_full = prepare_training_data(df_train, file_rawdata_name, file_rawdata_columns)

    if X_tensor_full is None or y_tensor_full is None or len(X_tensor_full) == 0:
        print("Training data is empty. Skipping training.")
        return

    # Dividir dados de treino em treino/validação para Early Stopping
    X_train, X_val, y_train, y_val = train_test_split(
        X_tensor_full, y_tensor_full, test_size=0.2, shuffle=False # Não embaralhar dados de série temporal
    )

    if len(X_train) == 0 or len(X_val) == 0:
        print("Not enough data to create train/validation split. Skipping training.")
        return

    print(f"Dados de treino: {len(X_train)} amostras, Dados de validação: {len(X_val)} amostras")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    input_dim = X_train.shape[1]
    output_dim = y_train.shape[1] # Agora será 3
    
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat")
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat")
    beta1 = read_field_from_json(hyperparam_path, 'beta1_nbeats')
    beta2 = read_field_from_json(hyperparam_path, 'beta2_nbeats')
    betas = (beta1, beta2)
    weight_decay = read_field_from_json(hyperparam_path, 'weight_decay_nbeat')

    print(f"Model architecture: input_dim={input_dim}, output_dim={output_dim}," \
          f"hidden_dim={hidden_dim}, num_blocks={num_blocks}," \
          f"beta1={beta1}, beta2={beta2}, weight_decay={weight_decay}, patience={patience}")

    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, betas=betas, weight_decay=weight_decay)

    # === LINHA QUEBRADA REMOVIDA ===
    # y_tensor = y_tensor.view(-1, 1)  <-- REMOVIDA

    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    log_file_path = "training_log_nbeat.txt"
    model_path = "" # Inicializar path do modelo

    best_val_loss = float('inf')
    patience_counter = 0

    # === LOOP DE TREINAMENTO ANTIGO REMOVIDO ===
    
    # === LOOP DE TREINAMENTO CORRETO (com Validação e Paciência) ===
    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        
        forecast = model(X_train) # Treinar no conjunto X_train
        
        # Calcular a perda para cada saída
        loss_time = criterion(forecast[:, 0], y_train[:, 0])
        loss_lon = criterion(forecast[:, 1], y_train[:, 1])
        loss_lat = criterion(forecast[:, 2], y_train[:, 2])
        loss = loss_time + loss_lon + loss_lat # Perda total
        
        loss.backward()
        optimizer.step()
        
        # --- Loop de Validação (para Early Stopping) ---
        model.eval()
        with torch.no_grad():
            val_forecast = model(X_val)
            val_loss_time = criterion(val_forecast[:, 0], y_val[:, 0])
            val_loss_lon = criterion(val_forecast[:, 1], y_val[:, 1])
            val_loss_lat = criterion(val_forecast[:, 2], y_val[:, 2])
            val_loss = val_loss_time + val_loss_lon + val_loss_lat
        
        if (epoch % 20 == 0) or (epoch == epochs - 1):
             log_msg = f"Epoch {epoch:3d} | Train Loss: {loss.item():.4f} | Val Loss: {val_loss.item():.4f}"
             print(log_msg)
             with open(log_file_path, "a") as f:
                 f.write(log_msg + "\n")

        # Lógica de Paciência
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            # Salvar o melhor modelo
            filename = file_rawdata_name.split('/')[-1].split('.')[0]
            data_prep_dir = os.path.join(script_dir, '..', 'Interpolation/models')
            model_path = os.path.join(data_prep_dir, f'nbeats_model_general_{filename}.pth')
            torch.save({'model_state_dict': model.state_dict()}, model_path)
        else:
            patience_counter += 1
            
        if patience_counter >= patience:
            stop_msg = f"Early stopping at epoch {epoch+1}"
            print(stop_msg)
            with open(log_file_path, "a") as f:
                f.write(stop_msg + "\n")
            break # Interrompe o loop de treino

    # --- Fim do Loop de Treinamento ---

    if not model_path: # Se o modelo nunca foi salvo (ex: 1 época)
        print("Model was not saved (early stopping did not trigger, or only 1 epoch). Saving last state.")
        filename = file_rawdata_name.split('/')[-1].split('.')[0]
        data_prep_dir = os.path.join(script_dir, '..', 'Interpolation/models')
        model_path = os.path.join(data_prep_dir, f'nbeats_model_general_{filename}.pth')
        torch.save({'model_state_dict': model.state_dict()}, model_path)

    print(f"Best model saved to {model_path}")

    # === CÁLCULO CORRIGIDO DAS MÉTRICAS DE TREINO ===
    model.eval()
    with torch.no_grad():
        final_train_predictions = model(X_train)
        final_train_pred_np = final_train_predictions.cpu().numpy()
        final_train_true_np = y_train.cpu().numpy() # <-- Correção
        
        # Calcular métricas gerais de treino
        final_train_mae = mean_absolute_error(final_train_true_np, final_train_pred_np)
        final_train_rmse = np.sqrt(mean_squared_error(final_train_true_np, final_train_pred_np))
        
        # Calcular MAPE apenas para TimeDiff (coluna 0)
        y_true_time_train = final_train_true_np[:, 0]
        pred_time_train = final_train_pred_np[:, 0]
        
        mask_train = y_true_time_train != 0
        if np.any(mask_train):
            final_train_mape = np.mean(np.abs((y_true_time_train[mask_train] - pred_time_train[mask_train]) / y_true_time_train[mask_train])) * 100
        else:
            final_train_mape = float('inf')

    ############ Evaluation on Test Set ############
    model_eval = NBeats(input_dim, output_dim, hidden_dim, num_blocks)
    checkpoint = torch.load(model_path)
    model_eval.load_state_dict(checkpoint['model_state_dict'])

    eval_result = evaluate_nbeats_model(model_eval, df_eval, file_rawdata_columns)

    if eval_result is None:
        print("❌ Evaluation failed. Skipping logging of metrics.")
        return

    eval_mae, eval_rmse, eval_mape = eval_result

    ############ Performance Comparison ############
    print("\n" + "="*60)
    print("📊 PERFORMANCE COMPARISON (Multivariate)")
    print("="*60)
    print(f"Training Set Performance:")
    print(f"  Overall MAE:  {final_train_mae:.4f}")
    print(f"  Overall RMSE: {final_train_rmse:.4f}")
    print(f"  TimeDiff MAPE: {final_train_mape:.2f}%")
    print()
    print(f"Evaluation (Test) Set Performance:")
    print(f"  Overall MAE:  {eval_mae:.4f}")
    print(f"  Overall RMSE: {eval_rmse:.4f}")
    print(f"  TimeDiff MAPE: {eval_mape:.2f}%")
    print()
    print(f"Performance Analysis:")
    
    # Comparar métricas gerais
    mae_diff = eval_mae - final_train_mae
    rmse_diff = eval_rmse - final_train_rmse
    mape_diff = eval_mape - final_train_mape
    
    print(f"  MAE Difference (Eval - Train):  {mae_diff:+.4f} ({mae_diff/final_train_mae*100:+.1f}%)")
    print(f"  RMSE Difference (Eval - Train): {rmse_diff:+.4f} ({rmse_diff/final_train_rmse*100:+.1f}%)")
    print(f"  MAPE Difference (Eval - Train): {mape_diff:+.2f}% ({mape_diff/final_train_mape*100:+.1f}%)")
    
    if mae_diff > final_train_mae * 0.2:  # 20% worse
        print("  ⚠️  Possible overfitting detected (evaluation MAE significantly higher)")
    elif mae_diff < final_train_mae * 0.1:  # Less than 10% worse
        print("  ✅ Good generalization (similar performance on train/eval)")
    else:
        print("  ℹ️  Normal generalization gap")
    
    print("="*60)

    ############ Save Results ############
    hiper_content = []
    hiper_content.append(f"Hyper nbeat input_dim {input_dim}")
    hiper_content.append(f"Hyper nbeat output_dim {output_dim}")
    # ... (salvar outros hiperparâmetros) ...
    hiper_content.append(f"Hyper nbeat best_val_loss {best_val_loss}")
    hiper_content.append(f"Hyper nbeat epochs {epochs}")
    hiper_content.append(f"Hyper nbeat patience {patience}")
    hiper_content.append("")
    hiper_content.append("# Training Performance (Multivariate NBeat)")
    hiper_content.append(f"Train nbeat Overall MAE {final_train_mae:.4f}")
    hiper_content.append(f"Train nbeat Overall RMSE {final_train_rmse:.4f}")
    hiper_content.append(f"Train nbeat TimeDiff MAPE {final_train_mape:.2f}%")
    hiper_content.append("")
    hiper_content.append("# Evaluation Performance (Multivariate NBeat)")
    hiper_content.append(f"Eval nbeat Overall MAE {eval_mae:.4f}")
    hiper_content.append(f"Eval nbeat Overall RMSE {eval_rmse:.4f}")
    hiper_content.append(f"Eval nbeat TimeDiff MAPE {eval_mape:.2f}%")
    hiper_content.append("")
    hiper_content.append("# Performance Gap (Multivariate NBeat)")
    hiper_content.append(f"MAE Gap {mae_diff:+.4f} ({mae_diff/final_train_mae*100:+.1f}%)")
    hiper_content.append(f"RMSE Gap {rmse_diff:+.4f} ({rmse_diff/final_train_rmse*100:+.1f}%)")
    hiper_content.append(f"MAPE Gap {mape_diff:+.2f}% ({mape_diff/final_train_mape*100:+.1f}%)")

    hiper_path = os.path.join(results_folder(file_rawdata_name), f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')
    
    return {
        'train_metrics': (final_train_mae, final_train_rmse, final_train_mape),
        'eval_metrics': (eval_mae, eval_rmse, eval_mape),
        'performance_gap': (mae_diff, rmse_diff, mape_diff)
    }

if __name__ == "__main__":
    import sys
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    
    from Common.utils import load_columns_config
    file_rawdata_columns = load_columns_config(file_rawdata_name)

    # Exemplo de como chamar (você precisa de uma lista de animais)
    # train_nbeats_model_list([current_animal], file_rawdata_name, file_rawdata_columns)
    print("Execute através da função train_nbeats_model_list")