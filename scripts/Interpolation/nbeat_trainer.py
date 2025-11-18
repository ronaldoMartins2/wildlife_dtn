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
from torch.utils.data import TensorDataset, DataLoader

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

    df = remove_nan_data(df, file_rawdata_columns)

    # --- CORREÇÃO: Converter string para datetime ---
    # Isso é necessário para que o .diff() funcione matematicamente
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    # Ordena por tempo para garantir consistência
    df = df.sort_values(by='Timestamp')

    # Calcular diferenças de tempo (agora funcionará pois são datetimes)
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600.0
    
    # Shift para pegar a diferença anterior como input
    df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)

    # Definição das FEATURES (Entrada) e TARGET (Saída)
    features = ['Prev Time Difference (hours)', 'Longitude', 'Latitude']
    target = ['Time Difference (hours)', 'Longitude', 'Latitude'] 

    # Remove linhas com NaN gerados pelo diff/shift
    df = df.dropna(subset=features + target)

    X = df[features].values
    y = df[target].values 

    # Retorna tensores. X shape: (N, 3), y shape: (N, 3)
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

    lr_training = read_field_from_json()

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
                        lr=0.001): #0.01

    X_tensor, y_tensor = prepare_training_data(df_train, file_rawdata_name, file_rawdata_columns)

    if X_tensor is None or y_tensor is None:
        print("Training data is empty. Skipping training.")
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    input_dim = X_tensor.shape[1]
    output_dim = y_tensor.shape[1]
    #output_dim = 1  # Force to 1 for single value prediction
    
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nbeat")
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nbeat")
    dropout_rate = read_field_from_json(hyperparam_path, "dropout_rate_nbeat")
    beta1 = read_field_from_json(hyperparam_path, 'beta1_nbeats')
    beta2 = read_field_from_json(hyperparam_path, 'beta2_nbeats')
    betas = (beta1, beta2)
    batch_size = read_field_from_json(hyperparam_path, "batch_size_nbeat")
    weight_decay = read_field_from_json(hyperparam_path, 'weight_decay_nbeat')

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    X_tensor = X_tensor.to(device)
    y_tensor = y_tensor.to(device)

    # Cria o Dataset e Dataloader
    dataset = TensorDataset(X_tensor, y_tensor)
    
    # Shuffle=True é recomendado aqui pois cada ponto (prev -> next) é tratado 
    # como uma amostra independente no seu modelo atual, ajudando na generalização.
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    print(f"Model architecture: input_dim={input_dim}, output_dim={output_dim}, hidden_dim={hidden_dim}, ...")

    # Instancia o modelo
    model = NBeats(input_dim, output_dim, hidden_dim, num_blocks, dropout=dropout_rate)
    model.to(device)

    print(f"Model architecture: input_dim={input_dim}, output_dim={output_dim}," \
         f"hidden_dim={hidden_dim}, num_blocks={num_blocks}," \
         f"beta1={beta1}, beta2={beta2}, weight_decay={weight_decay}")

    #model = NBeats(input_dim, output_dim, hidden_dim, num_blocks, dropout=dropout_rate)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, betas=betas, weight_decay=weight_decay)

    # Reshape target to match model output
    #y_tensor = y_tensor.view(-1, 1)  # Shape: (batch_size, 1)
    '''
    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    log_file_path = "training_log_nbeat.txt"

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    X_tensor = X_tensor.to(device)
    y_tensor = y_tensor.to(device)

    final_loss = 0
    best_train_mae = float('inf')
    train_mae_history = []
    train_rmse_history = []
    train_mape_history = []
    '''
    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    log_file_path = "training_log_nbeat.txt"

    criterion = nn.MSELoss()

    # --- ADICIONE/CORRIJA ESTE BLOCO ANTES DO LOOP 'for epoch' ---
    train_loss_history = []
    train_mae_history = []
    train_rmse_history = []
    train_mape_history = []
    final_loss = 0
    best_train_mae = float('inf')

    # Training loop with periodic evaluation
    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        forecast = model(X_tensor)
        
        # Ensure shapes match for loss calculation
        if forecast.dim() == 1:
            forecast = forecast.view(-1, 1)
        
        loss = criterion(forecast, y_tensor)
        loss.backward()
        optimizer.step()
        
        final_loss = loss.item()
        
        # Calculate training metrics every 20 epochs
        if epoch % 20 == 0 or epoch == epochs - 1:
            model.eval()
            with torch.no_grad():
                train_predictions = model(X_tensor)
                if train_predictions.dim() == 2 and train_predictions.shape[1] == 1:
                    train_predictions = train_predictions.squeeze(1)
                
                # Convert to numpy
                train_pred_np = train_predictions.cpu().numpy()
                train_true_np = y_tensor.squeeze().cpu().numpy()
                
                # Calculate metrics
                train_mae, train_rmse, train_mape = calculate_metrics(train_true_np, train_pred_np)
                
                train_mae_history.append(train_mae)
                train_rmse_history.append(train_rmse)
                train_mape_history.append(train_mape)
                
                if train_mae < best_train_mae:
                    best_train_mae = train_mae
                
                log_msg = f"[{filename}] Epoch {epoch:3d} | Loss: {loss.item():.4f} | Train MAE: {train_mae:.4f} | Train RMSE: {train_rmse:.4f} | Train MAPE: {train_mape:.2f}%"
                print(log_msg)
                
                # Write log to file
                with open(log_file_path, "a") as f:
                    f.write(log_msg + "\n")
            
            model.train()  # Switch back to training mode

    # Final training metrics
    model.eval()
    with torch.no_grad():
        final_train_predictions = model(X_tensor)
        if final_train_predictions.dim() == 2 and final_train_predictions.shape[1] == 1:
            final_train_predictions = final_train_predictions.squeeze(1)
        
        final_train_pred_np = final_train_predictions.cpu().numpy()
        final_train_true_np = y_tensor.squeeze().cpu().numpy()
        
        final_train_mae, final_train_rmse, final_train_mape = calculate_metrics(final_train_true_np, final_train_pred_np)

    # Save model
    results_dir = results_folder(file_rawdata_name)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Interpolation/models')
    model_path = os.path.join(data_prep_dir, f'nbeats_model_general_{filename}.pth')
    torch.save({'model_state_dict': model.state_dict()}, model_path)
    print(f"Model saved to {model_path}")

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
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    
    # You need to load this from somewhere; this is placeholder
    from Common.utils import load_columns_config
    file_rawdata_columns = load_columns_config(file_rawdata_name)  # implement if missing

    train_nbeats_model(current_animal, file_rawdata_name, file_rawdata_columns)