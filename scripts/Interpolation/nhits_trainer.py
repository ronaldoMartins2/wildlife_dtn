import torch
import torch.nn as nn
import torch.optim as optim
import os
import pandas as pd
import numpy as np
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
from Interpolation.nhits_model import NHits
from torch.serialization import safe_globals
from sklearn.preprocessing import MinMaxScaler

from Common.utils import (
    TRAINNING_SET,
    results_folder,
    read_field_from_json,
    remove_nan_data 
)

def evaluate_nhits_model(model, df, file_rawdata_columns):
    """
    Evaluate the trained NHiTS model using full trajectory data.
    """
    # Get datetime parsing mask
    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)

    # Sort and compute time difference
    df = df.sort_values('Timestamp').reset_index(drop=True)
    df['TimeDiff'] = df['Timestamp'].diff().dt.total_seconds() / 3600
    df = df.dropna(subset=['TimeDiff'])

    # Create sequences and targets using the same logic as training
    sequence_length = 10  # Use same as training
    prediction_horizon = 1  # Next point prediction

    # Features: [TimeDiff, Longitude, Latitude]
    features = df[['TimeDiff', 'Longitude', 'Latitude']].values

    sequences = []
    targets = []

    for i in range(len(features) - sequence_length - prediction_horizon + 1):
        seq = features[i:i + sequence_length]
        target_idx = i + sequence_length
        target = features[target_idx]  # Predict time_diff, lon, lat
        sequences.append(seq)
        targets.append(target)

    if len(sequences) == 0:
        print("❌ Not enough sequences for evaluation.")
        return

    X = np.array(sequences)
    y = np.array(targets)

    # Flatten X to scale, and then reshape
    n_seq, seq_len, n_features = X.shape
    X_flat = X.reshape(-1, n_features)
    
    scaler_features = MinMaxScaler()
    X_scaled = scaler_features.fit_transform(X_flat).reshape(n_seq, seq_len, n_features)

    scaler_targets = MinMaxScaler()
    y_scaled = scaler_targets.fit_transform(y)

    # Use only last step of sequence as model input
    X_tensor = torch.tensor(X_scaled[:, -1, :], dtype=torch.float32)

    model.eval()
    with torch.no_grad():
        time_pred, lon_pred, lat_pred = model(X_tensor)

        predictions = torch.stack([time_pred, lon_pred, lat_pred], dim=1).cpu().numpy()
        y_true = y_scaled

    # Inverse transform
    y_true_inv = scaler_targets.inverse_transform(y_true)
    pred_inv = scaler_targets.inverse_transform(predictions)

    # Calculate metrics
    mae = mean_absolute_error(y_true_inv, pred_inv)
    rmse = np.sqrt(mean_squared_error(y_true_inv, pred_inv))

    mask = y_true_inv[:, 0] != 0  # MAPE on TimeDiff
    if np.any(mask):
        mape = np.mean(np.abs((y_true_inv[mask, 0] - pred_inv[mask, 0]) / y_true_inv[mask, 0])) * 100
    else:
        mape = float('inf')

    print("📊 NHiTS Test Evaluation:")
    print(f"MAE: {mae:.4f} | RMSE: {rmse:.4f} | MAPE (TimeDiff only): {mape:.2f}%")

    return mae, rmse, mape

# Custom Dataset for time series data
class TimeSeriesDataset(Dataset):
    def __init__(self, sequences, targets):
        self.sequences = sequences
        self.targets = targets
    
    def __len__(self):
        return len(self.sequences)
    
    def __getitem__(self, idx):
        return torch.FloatTensor(self.sequences[idx]), torch.FloatTensor(self.targets[idx])

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

def get_train_eval( df_full ):
    # Sort by timestamp to maintain time series order
    df_full = df_full.sort_values(by='Timestamp').reset_index(drop=True)

    # Split index
    split_index = int(TRAINNING_SET * len(df_full))

    # 80% for training, 20% for evaluation
    df_train = df_full.iloc[:split_index].copy()
    df_eval  = df_full.iloc[split_index:].copy()

    return df_train, df_eval

def load_data_for_training(current_animal, file_rawdata_name, file_rawdata_columns):
    """
    Load and prepare data for training
    """
    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')

    # # Get datetime mask
    # mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    
    # # Load data
    # df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    # df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
    
    # # Use training set percentage
    # limit = int(TRAINNING_SET * len(df))
    # df = df.iloc[:limit]

    try:
        # Load data
        df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    except FileNotFoundError:
        print(f"Warning: File not found for animal {current_animal}. Skipping.")
        return pd.DataFrame()

    # 1. Check if the CSV has at least 10 rows initially
    if len(df) < 10:
        print(f"Warning: CSV for animal {current_animal} has fewer than 10 rows. Skipping.")
        return pd.DataFrame()

    # 2. Remove rows with missing interesting data
    df = remove_nan_data(df, current_animal)

    # 3. Check if there are still enough rows after cleaning
    if len(df) < 10:
        print(f"Warning: After cleaning, animal {current_animal} has fewer than 10 valid rows. Skipping.")
        return pd.DataFrame()

    try:
        # Get datetime mask and convert Timestamp
        mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask, errors='coerce')
        df.dropna(subset=['Timestamp'], inplace=True) # Drop rows where conversion failed
    except Exception as e:
        print(f"Warning: Could not process timestamps for animal {current_animal}. Error: {e}. Skipping.")
        return pd.DataFrame()

    return df
 
def getModelPath( file_rawdata_name ):

    results_dir = results_folder(file_rawdata_name)

    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    model_path = os.path.join(results_dir, f'nhits_model_general_{filename}.pth')

    return model_path

def getNhitsModel():

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')  # Navigate to the parent directory and into 'Results'

    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    input_dim = read_field_from_json(hyperparam_path, "input_dim_nhits")
    hidden_dim = read_field_from_json(hyperparam_path, "hidden_dim_nhits")
    num_blocks = read_field_from_json(hyperparam_path, "num_blocks_nhits")
    num_hierarchies = read_field_from_json(hyperparam_path, "num_hierarchies_nhits")

    model = NHits(input_dim, hidden_dim, num_blocks, num_hierarchies)

    return model

class NHiTSTrainer:
    def __init__(self, model, device='cpu'):
        self.model = model.to(device)
        self.device = device
        self.scaler_features = MinMaxScaler()
        self.scaler_targets = MinMaxScaler()
        self.train_losses = []
        self.val_losses = []
        self.train_metrics_history = []
        self.val_metrics_history = []
        
    def prepare_sequences(self, df, sequence_length=10, prediction_horizon=1):
        """Prepare sequences for training from trajectory data"""
        # Convert timestamp to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(df['Timestamp']):
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        
        # Sort by timestamp
        df = df.sort_values('Timestamp').reset_index(drop=True)
        
        # Calculate time differences in hours
        time_diffs = []
        for i in range(1, len(df)):
            diff = (df.iloc[i]['Timestamp'] - df.iloc[i-1]['Timestamp']).total_seconds() / 3600.0
            time_diffs.append(diff)
        
        # Add the first time difference as 0 or use the second one
        time_diffs.insert(0, time_diffs[0] if time_diffs else 0)
        df['TimeDiff'] = time_diffs
        
        # Create features and targets
        features = df[['TimeDiff', 'Longitude', 'Latitude']].values
        
        sequences = []
        targets = []
        
        for i in range(len(features) - sequence_length - prediction_horizon + 1):
            # Input sequence
            seq = features[i:i + sequence_length]
            
            # Target: next time_diff, longitude, latitude
            target_idx = i + sequence_length
            if target_idx < len(features):
                target = features[target_idx]  # [time_diff, lon, lat]
                sequences.append(seq)
                targets.append(target)
        
        return np.array(sequences), np.array(targets)
    
    def prepare_data(self, df, sequence_length=10, test_split=0.2, batch_size=32, prediction_horizon=1):
        """Prepare training and validation datasets"""
        sequences, targets = self.prepare_sequences(df, sequence_length, prediction_horizon)
        
        if len(sequences) == 0:
            raise ValueError("No sequences generated. Check your data or sequence_length parameter.")
        
        # Reshape for scaling
        n_sequences, seq_len, n_features = sequences.shape
        sequences_reshaped = sequences.reshape(-1, n_features)
        
        # Fit scalers and transform
        sequences_scaled = self.scaler_features.fit_transform(sequences_reshaped)
        targets_scaled = self.scaler_targets.fit_transform(targets)
        
        # Reshape back
        sequences_scaled = sequences_scaled.reshape(n_sequences, seq_len, n_features)
        
        # Split data
        split_idx = int(len(sequences_scaled) * (1 - test_split))
        
        train_sequences = sequences_scaled[:split_idx]
        train_targets = targets_scaled[:split_idx]
        val_sequences = sequences_scaled[split_idx:]
        val_targets = targets_scaled[split_idx:]
        
        # Create datasets and dataloaders
        train_dataset = TimeSeriesDataset(train_sequences, train_targets)
        val_dataset = TimeSeriesDataset(val_sequences, val_targets)
        
        train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)
        
        return train_loader, val_loader
    
    def calculate_detailed_metrics(self, data_loader, split_name=""):
        """Calculate detailed metrics (MAE, RMSE, MAPE) on a dataset"""
        self.model.eval()
        all_predictions = []
        all_targets = []
        
        with torch.no_grad():
            for batch_sequences, batch_targets in data_loader:
                batch_sequences = batch_sequences.to(self.device)
                batch_targets = batch_targets.to(self.device)
                
                model_input = batch_sequences[:, -1, :]
                time_pred, lon_pred, lat_pred = self.model(model_input)
                
                predictions = torch.stack([time_pred, lon_pred, lat_pred], dim=1)
                
                all_predictions.append(predictions.cpu().numpy())
                all_targets.append(batch_targets.cpu().numpy())
        
        # Concatenate all batches
        all_predictions = np.concatenate(all_predictions, axis=0)
        all_targets = np.concatenate(all_targets, axis=0)
        
        # Inverse transform to original scale
        pred_inv = self.scaler_targets.inverse_transform(all_predictions)
        true_inv = self.scaler_targets.inverse_transform(all_targets)
        
        # Calculate overall metrics
        mae_overall, rmse_overall, _ = calculate_metrics(true_inv.flatten(), pred_inv.flatten())
        
        # Calculate metrics for each component
        mae_time, rmse_time, mape_time = calculate_metrics(true_inv[:, 0], pred_inv[:, 0])
        mae_lon, rmse_lon, mape_lon = calculate_metrics(true_inv[:, 1], pred_inv[:, 1])
        mae_lat, rmse_lat, mape_lat = calculate_metrics(true_inv[:, 2], pred_inv[:, 2])
        
        metrics = {
            'overall': {'mae': mae_overall, 'rmse': rmse_overall},
            'time': {'mae': mae_time, 'rmse': rmse_time, 'mape': mape_time},
            'longitude': {'mae': mae_lon, 'rmse': rmse_lon, 'mape': mape_lon},
            'latitude': {'mae': mae_lat, 'rmse': rmse_lat, 'mape': mape_lat}
        }
        
        return metrics
    
    def train_epoch(self, train_loader, optimizer, criterion):
        """Train for one epoch"""
        self.model.train()
        total_loss = 0
        
        for batch_sequences, batch_targets in train_loader:
            batch_sequences = batch_sequences.to(self.device)
            batch_targets = batch_targets.to(self.device)
            
            optimizer.zero_grad()
            
            model_input = batch_sequences[:, -1, :]
            time_pred, lon_pred, lat_pred = self.model(model_input)
            
            # Calculate losses for each component
            time_loss = criterion(time_pred, batch_targets[:, 0])
            lon_loss = criterion(lon_pred, batch_targets[:, 1])
            lat_loss = criterion(lat_pred, batch_targets[:, 2])
            
            # Combined loss
            loss = time_loss + lon_loss + lat_loss
            
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
        
        return total_loss / len(train_loader)
    
    def validate(self, val_loader, criterion):
        """Validate the model"""
        self.model.eval()
        total_loss = 0
        
        with torch.no_grad():
            for batch_sequences, batch_targets in val_loader:
                batch_sequences = batch_sequences.to(self.device)
                batch_targets = batch_targets.to(self.device)
                
                model_input = batch_sequences[:, -1, :]
                time_pred, lon_pred, lat_pred = self.model(model_input)
                
                time_loss = criterion(time_pred, batch_targets[:, 0])
                lon_loss = criterion(lon_pred, batch_targets[:, 1])
                lat_loss = criterion(lat_pred, batch_targets[:, 2])
                
                loss = time_loss + lon_loss + lat_loss
                total_loss += loss.item()
        
        return total_loss / len(val_loader)
    
    def train(self, train_loader, val_loader, file_rawdata_name, epochs=100, lr=0.001, patience=10):
        """Full training loop with comprehensive metrics tracking"""
        criterion = nn.MSELoss()
        optimizer = optim.Adam(self.model.parameters(), lr=lr)
        
        best_val_loss = float('inf')
        patience_counter = 0
        
        print("Starting training...")
        log_file_path = "training_log_nhits.txt"

        for epoch in range(epochs):
            train_loss = self.train_epoch(train_loader, optimizer, criterion)
            val_loss = self.validate(val_loader, criterion)
            
            self.train_losses.append(train_loss)
            self.val_losses.append(val_loss)

            # Calculate detailed metrics every 20 epochs
            if epoch % 20 == 0 or epoch == epochs - 1:
                train_metrics = self.calculate_detailed_metrics(train_loader, "train")
                val_metrics = self.calculate_detailed_metrics(val_loader, "val")
                
                self.train_metrics_history.append(train_metrics)
                self.val_metrics_history.append(val_metrics)
                
                log_msg = (f"Epoch {epoch+1:3d}/{epochs} | "
                          f"Train Loss: {train_loss:.6f} | Val Loss: {val_loss:.6f} | "
                          f"Train MAE: {train_metrics['overall']['mae']:.4f} | "
                          f"Val MAE: {val_metrics['overall']['mae']:.4f} | "
                          f"TimeDiff MAPE: {train_metrics['time']['mape']:.2f}%/{val_metrics['time']['mape']:.2f}%")
                print(log_msg)
                
                with open(log_file_path, "a") as f:
                    f.write(log_msg + "\n")
            else:
                log_msg = f"Epoch {epoch+1:3d}/{epochs}: Train Loss = {train_loss:.6f}, Val Loss = {val_loss:.6f}"
                print(log_msg)
                with open(log_file_path, "a") as f:
                    f.write(log_msg + "\n")

            # Early stopping
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                patience_counter = 0
                torch.save(self.model.state_dict(), 'best_nhits_model.pth')
            else:
                patience_counter += 1
                
            if patience_counter >= patience:
                stop_msg = f"Early stopping at epoch {epoch+1}"
                print(stop_msg)
                with open(log_file_path, "a") as f:
                    f.write(stop_msg + "\n")
                break
        
        # Load best model
        self.model.load_state_dict(torch.load('best_nhits_model.pth'))
        
        # Final metrics calculation
        final_train_metrics = self.calculate_detailed_metrics(train_loader, "final_train")
        final_val_metrics = self.calculate_detailed_metrics(val_loader, "final_val")
        
        print("Training completed!")
        
        # Save hyperparameters
        hiper_content = []
        hiper_content.append(f"Hyper nhits final_train_loss {train_loss}")
        hiper_content.append(f"Hyper nhits final_val_loss {val_loss}")
        hiper_content.append(f"Hyper nhits best_val_loss {best_val_loss}")
        hiper_content.append(f"Hyper nhits epochs {epochs}")
        hiper_content.append(f"Hyper nhits lr {lr}")
        hiper_content.append(f"Hyper nhits patience {patience}")
        
        # Add final training metrics
        hiper_content.append("")
        hiper_content.append("# Final Training Metrics Nhits")
        hiper_content.append(f"Final train Nhits overall MAE {final_train_metrics['overall']['mae']:.4f}")
        hiper_content.append(f"Final train Nhits overall RMSE {final_train_metrics['overall']['rmse']:.4f}")
        hiper_content.append(f"Final train Nhits time MAE {final_train_metrics['time']['mae']:.4f}")
        hiper_content.append(f"Final train Nhits time RMSE {final_train_metrics['time']['rmse']:.4f}")
        hiper_content.append(f"Final train Nhits time MAPE {final_train_metrics['time']['mape']:.2f}%")
        
        # Add final validation metrics
        hiper_content.append("")
        hiper_content.append("# Final Validation Metrics Nhits")
        hiper_content.append(f"Final val Nhits overall MAE {final_val_metrics['overall']['mae']:.4f}")
        hiper_content.append(f"Final val Nhits overall RMSE {final_val_metrics['overall']['rmse']:.4f}")
        hiper_content.append(f"Final val Nhits time MAE {final_val_metrics['time']['mae']:.4f}")
        hiper_content.append(f"Final val Nhits time RMSE {final_val_metrics['time']['rmse']:.4f}")
        hiper_content.append(f"Final val Nhits time MAPE {final_val_metrics['time']['mape']:.2f}%")

        results_dir = results_folder(file_rawdata_name)
        hiper_path = os.path.join(results_dir, f'hiperparameters.txt')

        with open(hiper_path, "a") as file:
            for line in hiper_content:
                file.write(line + '\n')
        
        return final_train_metrics, final_val_metrics
    
    def save_model(self, filepath):
        """Save the trained model"""
        torch.save({
            'model_state_dict': self.model.state_dict(),
            'scaler_features': self.scaler_features,
            'scaler_targets': self.scaler_targets
        }, filepath)
        print(f"Model saved to {filepath}")

    def load_model(self, filepath):
        """
        Load a trained model including scalers, safely.
        """
        with safe_globals([MinMaxScaler]):
            checkpoint = torch.load(filepath, weights_only=False)

        self.model.load_state_dict(checkpoint['model_state_dict'])
        self.scaler_features = checkpoint['scaler_features']
        self.scaler_targets = checkpoint['scaler_targets']
        print(f"Model loaded from {filepath}")


def nhits_main_training_list(   animal_list, 
                                file_rawdata_name, 
                                file_rawdata_columns ):

    # Load data
    print("Loading data...")

    combined_df_list = []

    for current_animal in animal_list:
        df = load_data_for_training(current_animal, file_rawdata_name, file_rawdata_columns)
        combined_df_list.append(df)

    combined_df = pd.concat(combined_df_list, ignore_index=True)

    print(f"Loaded {len(df)} data points for animal {current_animal}")
    
    df_train, df_eval = get_train_eval( combined_df )

    main_training(  df_train,
                    df_eval,
                    file_rawdata_name, 
                    file_rawdata_columns )

# Enhanced main_training function with performance comparison
def main_training(df_train, df_eval, file_rawdata_name, file_rawdata_columns):
    """Main training function with comprehensive performance comparison"""
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    # Read hyperparameters
    epochs = read_field_from_json(hyperparam_path, "epochs_nhits")
    lr = read_field_from_json(hyperparam_path, "lr_nhits")
    patience = read_field_from_json(hyperparam_path, "patience_nhits")
    sequence_length = read_field_from_json(hyperparam_path, "sequence_length_nhits")
    prediction_horizon = read_field_from_json(hyperparam_path, "prediction_horizon_nhits")
    test_split = read_field_from_json(hyperparam_path, "test_split_nhits")
    batch_size = read_field_from_json(hyperparam_path, "batch_size_nhits")

    model = getNhitsModel()
    print(f"Created NHiTS model with {sum(p.numel() for p in model.parameters())} parameters")
    
    # Create trainer
    trainer = NHiTSTrainer(model, device)
    
    # Prepare data
    print("Preparing sequences...")
    train_loader, val_loader = trainer.prepare_data(df_train, sequence_length, test_split, batch_size, prediction_horizon)
    print(f"Training batches: {len(train_loader)}, Validation batches: {len(val_loader)}")

    # Train model and get final metrics
    final_train_metrics, final_val_metrics = trainer.train(train_loader, val_loader, file_rawdata_name, epochs, lr, patience)
    
    # Save model
    model_path = getModelPath(file_rawdata_name)
    trainer.save_model(model_path)

    ############ Evaluation on Test Set ############
    model_eval = getNhitsModel()
    with safe_globals([MinMaxScaler]):
        checkpoint = torch.load(model_path, weights_only=False)
    model_eval.load_state_dict(checkpoint['model_state_dict'])

    eval_result = evaluate_nhits_model(model_eval, df_eval, file_rawdata_columns)

    if eval_result is None:
        print("❌ Evaluation failed. Skipping logging of metrics.")
        return

    eval_mae, eval_rmse, eval_mape = eval_result

    ############ Performance Comparison ############
    print("\n" + "="*80)
    print("📊 NHiTS PERFORMANCE COMPARISON")
    print("="*80)
    
    print("Training Set Performance (Final):")
    print(f"  Overall MAE:  {final_train_metrics['overall']['mae']:.4f}")
    print(f"  Overall RMSE: {final_train_metrics['overall']['rmse']:.4f}")
    print(f"  TimeDiff MAE: {final_train_metrics['time']['mae']:.4f}")
    print(f"  TimeDiff RMSE: {final_train_metrics['time']['rmse']:.4f}")
    print(f"  TimeDiff MAPE: {final_train_metrics['time']['mape']:.2f}%")
    
    print("\nValidation Set Performance (Final):")
    print(f"  Overall MAE:  {final_val_metrics['overall']['mae']:.4f}")
    print(f"  Overall RMSE: {final_val_metrics['overall']['rmse']:.4f}")
    print(f"  TimeDiff MAE: {final_val_metrics['time']['mae']:.4f}")
    print(f"  TimeDiff RMSE: {final_val_metrics['time']['rmse']:.4f}")
    print(f"  TimeDiff MAPE: {final_val_metrics['time']['mape']:.2f}%")
    
    print(f"\nTest Set Performance (Separate Evaluation):")
    print(f"  Overall MAE:  {eval_mae:.4f}")
    print(f"  Overall RMSE: {eval_rmse:.4f}")
    print(f"  TimeDiff MAPE: {eval_mape:.2f}%")
    
    print("\nPerformance Analysis:")
    
    # Compare training vs validation
    train_val_mae_diff = final_val_metrics['overall']['mae'] - final_train_metrics['overall']['mae']
    train_val_mape_diff = final_val_metrics['time']['mape'] - final_train_metrics['time']['mape']
    
    print(f"  Training vs Validation:")
    print(f"    MAE Difference:  {train_val_mae_diff:+.4f} ({train_val_mae_diff/final_train_metrics['overall']['mae']*100:+.1f}%)")
    print(f"    MAPE Difference: {train_val_mape_diff:+.2f}% ({train_val_mape_diff/final_train_metrics['time']['mape']*100:+.1f}%)")
    
    # Compare validation vs test
    val_test_mae_diff = eval_mae - final_val_metrics['overall']['mae']
    val_test_mape_diff = eval_mape - final_val_metrics['time']['mape']
    
    print(f"  Validation vs Test:")
    print(f"    MAE Difference:  {val_test_mae_diff:+.4f} ({val_test_mae_diff/final_val_metrics['overall']['mae']*100:+.1f}%)")
    print(f"    MAPE Difference: {val_test_mape_diff:+.2f}% ({val_test_mape_diff/final_val_metrics['time']['mape']*100:+.1f}%)")
    
    # Overall assessment
    if abs(train_val_mae_diff/final_train_metrics['overall']['mae']) < 0.1:
        print("  ✅ Good generalization (train/val performance similar)")
    elif abs(train_val_mae_diff/final_train_metrics['overall']['mae']) < 0.2:
        print("  ℹ️  Normal generalization gap")
    else:
        print("  ⚠️  Possible overfitting detected")
    
    print("="*80)
    
    # Save comprehensive results
    results_dir = results_folder(file_rawdata_name)
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')
    
    performance_content = [
        "",
        "# Test Set Evaluation Results Nhits",
        f"Test overall MAE {eval_mae:.4f}",
        f"Test overall RMSE {eval_rmse:.4f}",
        f"Test time MAPE {eval_mape:.2f}%",
        "",
        "# Performance Gaps Nhits",
        f"Train-Val MAE gap {train_val_mae_diff:+.4f} ({train_val_mae_diff/final_train_metrics['overall']['mae']*100:+.1f}%)",
        f"Train-Val MAPE gap {train_val_mape_diff:+.2f}% ({train_val_mape_diff/final_train_metrics['time']['mape']*100:+.1f}%)",
        f"Val-Test MAE gap {val_test_mae_diff:+.4f} ({val_test_mae_diff/final_val_metrics['overall']['mae']*100:+.1f}%)",
        f"Val-Test MAPE gap {val_test_mape_diff:+.2f}% ({val_test_mape_diff/final_val_metrics['time']['mape']*100:+.1f}%)"
    ]
    
    with open(hiper_path, "a") as file:
        for line in performance_content:
            file.write(line + '\n')
    
    print("Training completed successfully!")
    return trainer

if __name__ == "__main__":
    import sys
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    file_rawdata_columns = sys.argv[3]

    main_training(current_animal, file_rawdata_name, file_rawdata_columns)