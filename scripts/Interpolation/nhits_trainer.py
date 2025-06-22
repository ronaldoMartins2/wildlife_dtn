import torch
import torch.nn as nn
import torch.optim as optim
import os
import pandas as pd
import numpy as np
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import MinMaxScaler
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
import matplotlib.pyplot as plt

from Common.utils import (
    #create_clusterization_results,
    #read_field_from_json,
    TRAINNING_SET,
    results_folder
)

from Interpolation.nhits_interpolation import NHiTS

class NHiTSTrainer:
    def __init__(self, model, device='cpu'):
        self.model = model.to(device)
        self.device = device
        self.scaler_features = MinMaxScaler()
        self.scaler_targets = MinMaxScaler()
        self.train_losses = []
        self.val_losses = []
        
    def prepare_sequences(self, df, sequence_length=10, prediction_horizon=1):
        """
        Prepare sequences for training from trajectory data
        """
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
    
    def prepare_data(self, df, sequence_length=10, test_split=0.2, batch_size=32):
        """
        Prepare training and validation datasets
        """
        sequences, targets = self.prepare_sequences(df, sequence_length)
        
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
    
    def train_epoch(self, train_loader, optimizer, criterion):
        """
        Train for one epoch
        """
        self.model.train()
        total_loss = 0
        
        for batch_sequences, batch_targets in train_loader:
            batch_sequences = batch_sequences.to(self.device)
            batch_targets = batch_targets.to(self.device)
            
            optimizer.zero_grad()
            
            # Use the last timestep of the sequence as input to the model
            # Your model expects [batch_size, input_dim] where input_dim=3
            model_input = batch_sequences[:, -1, :]  # Take last timestep
            
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
        """
        Validate the model
        """
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
    
    def train(self, train_loader, val_loader, epochs=100, lr=0.001, patience=10):
        """
        Full training loop with early stopping
        """
        criterion = nn.MSELoss()
        optimizer = optim.Adam(self.model.parameters(), lr=lr)
        
        best_val_loss = float('inf')
        patience_counter = 0
        
        print("Starting training...")
        
        '''
        for epoch in range(epochs):
            train_loss = self.train_epoch(train_loader, optimizer, criterion)
            val_loss = self.validate(val_loader, criterion)
            
            self.train_losses.append(train_loss)
            self.val_losses.append(val_loss)
            
            print(f"Epoch {epoch+1}/{epochs}: Train Loss = {train_loss:.6f}, Val Loss = {val_loss:.6f}")
            
            # Early stopping
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                patience_counter = 0
                # Save best model
                torch.save(self.model.state_dict(), 'best_nhits_model.pth')
            else:
                patience_counter += 1
                
            if patience_counter >= patience:E
                print(f"Early stopping at epoch {epoch+1}")
                break
        '''

        #results_dir = results_folder(file_rawdata_name)
        #model_path = os.path.join(results_dir, f'nhits_trainning_model_results.pth')

        log_file_path = "training_log_nhits.txt"

        for epoch in range(epochs):
            train_loss = self.train_epoch(train_loader, optimizer, criterion)
            val_loss = self.validate(val_loader, criterion)
            
            self.train_losses.append(train_loss)
            self.val_losses.append(val_loss)

            log_msg = f"Epoch {epoch+1}/{epochs}: Train Loss = {train_loss:.6f}, Val Loss = {val_loss:.6f}"
            print(log_msg)

            # Write log to file
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

        print("Training completed!")

    
    def plot_losses(self):
        """
        Plot training and validation losses
        """
        plt.figure(figsize=(10, 6))
        plt.plot(self.train_losses, label='Training Loss')
        plt.plot(self.val_losses, label='Validation Loss')
        plt.xlabel('Epoch')
        plt.ylabel('Loss')
        plt.title('Training and Validation Losses')
        plt.legend()
        plt.grid(True)
        plt.show()
    
    def save_model(self, filepath):
        """
        Save the trained model
        """
        torch.save({
            'model_state_dict': self.model.state_dict(),
            'scaler_features': self.scaler_features,
            'scaler_targets': self.scaler_targets
        }, filepath)
        print(f"Model saved to {filepath}")
    
    def load_model(self, filepath):
        """
        Load a trained model
        """
        checkpoint = torch.load(filepath)
        self.model.load_state_dict(checkpoint['model_state_dict'])
        self.scaler_features = checkpoint['scaler_features']
        self.scaler_targets = checkpoint['scaler_targets']
        print(f"Model loaded from {filepath}")

# Custom Dataset for time series data
class TimeSeriesDataset(Dataset):
    def __init__(self, sequences, targets):
        self.sequences = sequences
        self.targets = targets
    
    def __len__(self):
        return len(self.sequences)
    
    def __getitem__(self, idx):
        return torch.FloatTensor(self.sequences[idx]), torch.FloatTensor(self.targets[idx])


def load_data_for_training(current_animal, file_rawdata_name, file_rawdata_columns):
    """
    Load and prepare data for training
    """
    results_dir = results_folder(file_rawdata_name)
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    
    # Get datetime mask
    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    
    # Load data
    df = pd.read_csv(file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=mask)
    
    # Use training set percentage
    limit = int(TRAINNING_SET * len(df))
    df = df.iloc[:limit]
    
    return df


'''

python scripts/Interpolation/nhits_trainer.py 93 rawdata/jaguar_mamiraua.csv rawdata/jaguar_columns.json


'''
def main_training_list( animal_list, 
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

    main_training(  combined_df, 
                    file_rawdata_name, 
                    file_rawdata_columns )

def main_training_single(   current_animal, 
                            file_rawdata_name, 
                            file_rawdata_columns ):

    # Load data
    print("Loading data...")
    df = load_data_for_training(current_animal, file_rawdata_name, file_rawdata_columns)
    print(f"Loaded {len(df)} data points for animal {current_animal}")

    main_training(  df, 
                    file_rawdata_name, 
                    file_rawdata_columns )

def main_training(  df, 
                    file_rawdata_name, 
                    file_rawdata_columns ):
    """
    Main training function
    """
    # Set device
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    
    # Create model
    input_dim = 3
    hidden_dim = 64  # Increased for better capacity
    num_blocks = 4
    num_hierarchies = 3
    
    model = NHiTS(input_dim, hidden_dim, num_blocks, num_hierarchies)
    print(f"Created NHiTS model with {sum(p.numel() for p in model.parameters())} parameters")
    
    # Create trainer
    trainer = NHiTSTrainer(model, device)
    
    # Prepare data
    print("Preparing sequences...")
    train_loader, val_loader = trainer.prepare_data(df, sequence_length=20, batch_size=32)
    print(f"Training batches: {len(train_loader)}, Validation batches: {len(val_loader)}")
    
    # Train model
    trainer.train(train_loader, val_loader, epochs=200, lr=0.001, patience=20)
    
    # Plot losses
    trainer.plot_losses()
    
    # Save model
    results_dir = results_folder(file_rawdata_name)

    filename = file_rawdata_name.split('/')[-1].split('.')[0]
    model_path = os.path.join(results_dir, f'nhits_model_general_{filename}.pth')

    print(f'model_path >>> {model_path}')

    trainer.save_model(model_path)
    
    print("Training completed successfully!")
    return trainer

if __name__ == "__main__":
    import sys
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    file_rawdata_columns = sys.argv[3]

    main_training(current_animal, file_rawdata_name, file_rawdata_columns)