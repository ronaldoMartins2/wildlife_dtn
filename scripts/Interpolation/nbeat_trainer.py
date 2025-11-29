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
import numpy as np

def evaluate_nbeats_model(model, df, file_rawdata_columns):
    # Recreate features and targets from the full DataFrame
    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600
    df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)
    df = df.dropna(subset=['Time Difference (hours)', 'Prev Time Difference (hours)'])

    features = ['Prev Time Difference (hours)', 'Longitude', 'Latitude']
    target = 'Time Difference (hours)'

    X = df[features].values
    y = df[target].values

    # Debugging: Check the shapes of X and y
    print(f"X shape: {X.shape}")
    print(f"y shape: {y.shape}")

    if len(X) == 0:
        print("❌ Not enough data to evaluate test set.")
        return

    X_tensor = torch.tensor(X, dtype=torch.float32)

    model.eval()
    with torch.no_grad():
        predictions = model(X_tensor)

        # Debugging: Check the shape of predictions
        print(f"Predictions shape before processing: {predictions.shape}")
        
        # Handle different prediction shapes
        if predictions.dim() == 2:
            if predictions.shape[1] == 1:
                # Shape is (batch_size, 1) - squeeze to (batch_size,)
                predictions = predictions.squeeze(1)
            else:
                print(f"❌ Unexpected prediction shape: {predictions.shape}. Expected (batch_size, 1) or (batch_size,)")
                return
        elif predictions.dim() == 1:
            # Shape is already (batch_size,) - good to go
            pass
        else:
            print(f"❌ Unexpected prediction dimensions: {predictions.dim()}")
            return
        
        # Convert to numpy for sklearn metrics
        predictions = predictions.cpu().numpy()
        
        # Debugging the final shape
        print(f"Final predictions shape: {predictions.shape}")
        print(f"Target y shape: {y.shape}")

        # Ensure the shapes match
        if predictions.shape != y.shape:
            print(f"❌ Shape mismatch: predictions.shape = {predictions.shape}, y.shape = {y.shape}")
            return

    # Calculate metrics
    mae = mean_absolute_error(y, predictions)
    rmse = np.sqrt(mean_squared_error(y, predictions))
    
    # Handle MAPE calculation with zero-division protection
    mask = y != 0
    if np.any(mask):
        mape = np.mean(np.abs((y[mask] - predictions[mask]) / y[mask])) * 100
    else:
        mape = float('inf')  # or np.nan

    print(f"📊 N-BEATS Test Evaluation:")
    print(f"MAE: {mae:.4f} | RMSE: {rmse:.4f} | MAPE: {mape:.2f}%")

    return mae, rmse, mape

def prepare_training_data(  #current_animal, 
                            df,
                            file_rawdata_name,
                            file_rawdata_columns):

    if df.empty:
        return None, None
    
    df = remove_nan_data(df)
    
    print("Inside nbeat_trainer")

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

    # Robust timestamp parsing: try format mask first, fallback to coercion/infer
    if mask:
        try:
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
        except Exception as e:
            print(f"[NBEATS] Warning: parsing with mask '{mask}' failed: {e}. Falling back to infer/coerce.")
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce', infer_datetime_format=True)
    else:
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce', infer_datetime_format=True)

    # Drop rows with invalid timestamps
    n_invalid_ts = df['Timestamp'].isna().sum()
    if n_invalid_ts > 0:
        print(f"[NBEATS] Dropping {n_invalid_ts} rows with invalid timestamps.")
        df = df.dropna(subset=['Timestamp']).reset_index(drop=True)

    # Now compute time differences
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600
    df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)

    df = df.dropna(subset=['Time Difference (hours)', 'Prev Time Difference (hours)'])

    features = ['Prev Time Difference (hours)', 'Longitude', 'Latitude']
    # --- Mudança: targets agora são 3 colunas ---
    #target = 'Time Difference (hours)'
    targets = ['Time Difference (hours)', 'Longitude', 'Latitude']

    X = df[features].values
    y = df[targets].values  # Shape: (n_samples, 3)

    try:
        X = X.astype(np.float32)
        y = y.astype(np.float32)
    except (ValueError, TypeError) as e:
        print(f"[NBEATS] Erro ao converter X,y para float32: {e}")
        print(f"[NBEATS] X dtype: {X.dtype}, y dtype: {y.dtype}")
        return None, None

    # Remove rows with NaN/inf after conversion
    valid_mask = ~(np.isnan(X).any(axis=1) | np.isinf(X).any(axis=1) |
                   np.isnan(y).any(axis=1) | np.isinf(y).any(axis=1))
    if not np.all(valid_mask):
        removed = len(valid_mask) - np.count_nonzero(valid_mask)
        print(f"[NBEATS] Removed {removed} rows with NaN/inf in features/targets.")
        X = X[valid_mask]
        y = y[valid_mask]

    if len(X) == 0:
        print("[NBEATS] Nenhum dado válido após limpeza.")
        return None, None

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

    if lr_ratting is None:
        print(f"Learn ratting de N-Beats não encontrada no arquivo de hiperparâmetros")
        return 

    train_nbeats_model( df_train,
                        df_eval,
                        file_rawdata_name, 
                        file_rawdata_columns,
                        lr=lr_ratting)

def get_train_eval( df_full ):
    # Sort by timestamp to maintain time series order
    df_full = df_full.sort_values(by='Timestamp').reset_index(drop=True)

    # Split index
    split_index = int(TRAINNING_SET * len(df_full))

    # 80% for training, 20% for evaluation
    df_train = df_full.iloc[:split_index].copy()
    df_eval  = df_full.iloc[split_index:].copy()

    return df_train, df_eval

def train_nbeats_model_single(
                            current_animal,
                            file_rawdata_name, 
                            file_rawdata_columns, 
                            ):

    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')

    df_full = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])

    df_train, df_eval = get_train_eval( df_full )

    #lr_training = read_field_from_json()

    train_nbeats_model( df_train,
                        df_eval,
                        file_rawdata_name, 
                        file_rawdata_columns
                        )

def calculate_metrics(y_true, y_pred):
    """Calculate MAE, RMSE, and MAPE metrics"""
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    
    # Handle MAPE calculation with zero-division protection
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
                        lr=0.0001): #0.01

    X_tensor, y_tensor = prepare_training_data(df_train, file_rawdata_name, file_rawdata_columns)

    if X_tensor is None or y_tensor is None:
        print("Training data is empty. Skipping training.")
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    input_dim = X_tensor.shape[1]
    output_dim = 3  # --- Mudança: sempre 3 targets ---
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat") or 32
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat") or 2
    beta1 = read_field_from_json(hyperparam_path, 'beta1_nbeats') or 0.9
    beta2 = read_field_from_json(hyperparam_path, 'beta2_nbeats') or 0.999
    betas = (beta1, beta2)
    weight_decay = read_field_from_json(hyperparam_path, 'weight_decay_nbeat') or 0.0001

    print(f"Model architecture: input_dim={input_dim}, output_dim={output_dim}, hidden_dim={hidden_dim}, num_blocks={num_blocks}")

    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, betas=betas, weight_decay=weight_decay)

    # --- Mudança: y_tensor já tem shape (batch_size, 3) ---
    # Não precisa reshape, o modelo retorna shape (batch_size, 3)
    
    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    log_file_path = "training_log_nbeat.txt"

    final_loss = 0
    best_train_mae = float('inf')
    train_mae_history = []

    # Training loop
    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        forecast = model(X_tensor)  # Shape: (batch_size, 3)
        
        # forecast já tem shape (batch_size, 3), y_tensor também
        loss = criterion(forecast, y_tensor)
        loss.backward()
        optimizer.step()
        
        final_loss = loss.item()
        
        # Calcular métricas a cada 20 epochs
        if epoch % 20 == 0 or epoch == epochs - 1:
            model.eval()
            with torch.no_grad():
                train_predictions = model(X_tensor)  # Shape: (batch_size, 3)
                
                # Converter para numpy
                train_pred_np = train_predictions.cpu().numpy()
                train_true_np = y_tensor.cpu().numpy()
                
                # Calcular MAE para cada target
                mae_time_diff = np.mean(np.abs(train_true_np[:, 0] - train_pred_np[:, 0]))
                mae_lon = np.mean(np.abs(train_true_np[:, 1] - train_pred_np[:, 1]))
                mae_lat = np.mean(np.abs(train_true_np[:, 2] - train_pred_np[:, 2]))
                train_mae = (mae_time_diff + mae_lon + mae_lat) / 3
                
                if train_mae < best_train_mae:
                    best_train_mae = train_mae
                
                log_msg = f"[{filename}] Epoch {epoch:3d} | Loss: {loss.item():.4f} | MAE_TimeDiff: {mae_time_diff:.4f} | MAE_Lon: {mae_lon:.6f} | MAE_Lat: {mae_lat:.6f}"
                print(log_msg)
                
                with open(log_file_path, "a") as f:
                    f.write(log_msg + "\n")
            
            model.train()

    # Final metrics (train)
    model.eval()
    with torch.no_grad():
        final_train_predictions = model(X_tensor)
        final_train_pred_np = final_train_predictions.cpu().numpy()
        final_train_true_np = y_tensor.cpu().numpy()
        
        final_mae_time_diff = np.mean(np.abs(final_train_true_np[:, 0] - final_train_pred_np[:, 0]))
        final_mae_lon = np.mean(np.abs(final_train_true_np[:, 1] - final_train_pred_np[:, 1]))
        final_mae_lat = np.mean(np.abs(final_train_true_np[:, 2] - final_train_pred_np[:, 2]))
        final_train_mae = (final_mae_time_diff + final_mae_lon + final_mae_lat) / 3

        # RMSE per target
        final_rmse_time = np.sqrt(mean_squared_error(final_train_true_np[:, 0], final_train_pred_np[:, 0]))
        final_rmse_lon  = np.sqrt(mean_squared_error(final_train_true_np[:, 1], final_train_pred_np[:, 1]))
        final_rmse_lat  = np.sqrt(mean_squared_error(final_train_true_np[:, 2], final_train_pred_np[:, 2]))
        final_train_rmse = (final_rmse_time + final_rmse_lon + final_rmse_lat) / 3

        # MAPE per target (safeguard zeros)
        def safe_mape(true, pred):
            mask = true != 0
            if np.any(mask):
                return np.mean(np.abs((true[mask] - pred[mask]) / true[mask])) * 100
            return float('inf')
        final_mape_time = safe_mape(final_train_true_np[:, 0], final_train_pred_np[:, 0])
        final_mape_lon  = safe_mape(final_train_true_np[:, 1], final_train_pred_np[:, 1])
        final_mape_lat  = safe_mape(final_train_true_np[:, 2], final_train_pred_np[:, 2])
        final_train_mape = np.mean([final_mape_time, final_mape_lon, final_mape_lat])

    # Save model
    results_dir = results_folder(file_rawdata_name)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = os.path.join(script_dir, 'models')
    os.makedirs(models_dir, exist_ok=True)
    model_path = os.path.join(models_dir, f'nbeats_model_general_{filename}.pth')
    torch.save({'model_state_dict': model.state_dict()}, model_path)
    print(f"Model saved to {model_path}")

    ############ Evaluation on Test Set ############
    model_eval = NBeats(input_dim, output_dim, hidden_dim, num_blocks)
    checkpoint = torch.load(model_path)
    model_eval.load_state_dict(checkpoint['model_state_dict'])

    # Prepare evaluation tensors (same preprocessing as training)
    X_eval_tensor, y_eval_tensor = prepare_training_data(df_eval, file_rawdata_name, file_rawdata_columns)
    if X_eval_tensor is None or y_eval_tensor is None:
        print("❌ No evaluation data available. Skipping evaluation metrics.")
        eval_mae = eval_rmse = eval_mape = None
    else:
        model_eval.eval()
        with torch.no_grad():
            preds_eval = model_eval(X_eval_tensor).cpu().numpy()
            true_eval = y_eval_tensor.cpu().numpy()

            # MAE per target
            mae_time = np.mean(np.abs(true_eval[:, 0] - preds_eval[:, 0]))
            mae_lon  = np.mean(np.abs(true_eval[:, 1] - preds_eval[:, 1]))
            mae_lat  = np.mean(np.abs(true_eval[:, 2] - preds_eval[:, 2]))
            eval_mae = (mae_time + mae_lon + mae_lat) / 3

            # RMSE per target
            rmse_time = np.sqrt(mean_squared_error(true_eval[:, 0], preds_eval[:, 0]))
            rmse_lon  = np.sqrt(mean_squared_error(true_eval[:, 1], preds_eval[:, 1]))
            rmse_lat  = np.sqrt(mean_squared_error(true_eval[:, 2], preds_eval[:, 2]))
            eval_rmse = (rmse_time + rmse_lon + rmse_lat) / 3

            # MAPE per target (safeguard)
            mape_time = safe_mape(true_eval[:, 0], preds_eval[:, 0])
            mape_lon  = safe_mape(true_eval[:, 1], preds_eval[:, 1])
            mape_lat  = safe_mape(true_eval[:, 2], preds_eval[:, 2])
            eval_mape = np.mean([mape_time, mape_lon, mape_lat])

    if eval_mae is None:
        print("❌ Evaluation failed. Skipping logging of metrics.")
        return

    eval_mae, eval_rmse, eval_mape = eval_mae, eval_rmse, eval_mape

    ############ Performance Comparison ############
    print("\n" + "="*60)
    print("📊 PERFORMANCE COMPARISON")
    print("="*60)
    print(f"Training Set Performance:")
    print(f"  Final MAE:  {final_train_mae:.4f}")
    print(f"  Final RMSE: {final_train_rmse:.4f}")
    print(f"  Final MAPE: {final_train_mape:.2f}%")
    print(f"  Best MAE:   {best_train_mae:.4f}")
    print()
    print(f"Evaluation Set Performance:")
    print(f"  MAE:  {eval_mae:.4f}")
    print(f"  RMSE: {eval_rmse:.4f}")
    print(f"  MAPE: {eval_mape:.2f}%")
    print()
    print(f"Performance Analysis:")
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
    hiper_content.append(f"Hyper nbeat hidden_dim {hidden_dim}")
    hiper_content.append(f"Hyper nbeat num_blocks {num_blocks}")
    hiper_content.append(f"Hyper nbeat loss {final_loss}")
    hiper_content.append(f"Hyper nbeat epochs {epochs}")
    hiper_content.append("")
    hiper_content.append("# Training Performance")
    hiper_content.append(f"Train nbeat MAE {final_train_mae:.4f}")
    hiper_content.append(f"Train nbeat RMSE {final_train_rmse:.4f}")
    hiper_content.append(f"Train nbeat MAPE {final_train_mape:.2f}%")
    hiper_content.append(f"Train nbeat Best MAE {best_train_mae:.4f}")
    hiper_content.append("")
    hiper_content.append("# Evaluation Performance")
    hiper_content.append(f"Eval nbeat MAE {eval_mae:.4f}")
    hiper_content.append(f"Eval nbeat RMSE {eval_rmse:.4f}")
    hiper_content.append(f"Eval nbeat MAPE {eval_mape:.2f}%")
    hiper_content.append("")
    hiper_content.append("# Performance Gap NBeat")
    hiper_content.append(f"MAE Gap {mae_diff:+.4f} ({mae_diff/final_train_mae*100:+.1f}%)")
    hiper_content.append(f"RMSE Gap {rmse_diff:+.4f} ({rmse_diff/final_train_rmse*100:+.1f}%)")
    hiper_content.append(f"MAPE Gap {mape_diff:+.2f}% ({mape_diff/final_train_mape*100:+.1f}%)")

    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')
    
    return {
        'train_metrics': (final_train_mae, final_train_rmse, final_train_mape),
        'eval_metrics': (eval_mae, eval_rmse, eval_mape),
        'best_train_mae': best_train_mae,
        'performance_gap': (mae_diff, rmse_diff, mape_diff)
    }

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python nhit_trainer.py <animal_id> <rawdata_csv> <rawdata_columns_json>")
        sys.exit(1)

    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    file_rawdata_columns = sys.argv[3]

    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        sys.exit(1)

    df_full = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    df_train, df_eval = get_train_eval(df_full)

    train_nbeats_model(df_train, df_eval, file_rawdata_name, file_rawdata_columns)