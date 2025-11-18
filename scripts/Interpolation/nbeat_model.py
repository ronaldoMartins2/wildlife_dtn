'''
import torch
import torch.nn as nn

# Define the NBeatsBlock with residual connection fix and ReLU on output
class NBeatsBlock(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim):
        super(NBeatsBlock, self).__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, output_dim)
        self.fc_res = nn.Linear(output_dim, input_dim)

    def forward(self, x):
        x_residual = x
        x = torch.relu(self.fc1(x))
        x = torch.relu(self.fc2(x))
        forecast = self.fc3(x)
        forecast = self.fc_res(forecast)
        return forecast

class NBeats(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, num_blocks):
        super(NBeats, self).__init__()
        self.blocks = nn.ModuleList([NBeatsBlock(input_dim, output_dim, hidden_dim) for _ in range(num_blocks)])

    def forward(self, x):
        forecasts = []
        for block in self.blocks:
            forecast = block(x)
            forecasts.append(forecast)
            x = x + forecast  # Residual connection

        # Average the forecasts from each block
        final_forecast = sum(forecasts) / len(forecasts)

        # Ensure the final forecast is of the correct shape
        final_forecast = final_forecast.view(-1)  # Flatten to match expected output shape

        # Apply ReLU to ensure positive predictions
        final_forecast = torch.relu(final_forecast)

        # Clip to a minimum threshold to avoid too small predictions
        final_forecast = torch.maximum(final_forecast, torch.tensor(0.1))  # Clip to 0.1 if necessary

        return final_forecast
'''
import torch
import torch.nn as nn

class NBeatsBlock(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, dropout=0.1):
        super(NBeatsBlock, self).__init__()
        self.input_dim = input_dim
        self.output_dim = output_dim
        
        # Feature extraction layers
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.bn1 = nn.BatchNorm1d(hidden_dim)
        
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.bn2 = nn.BatchNorm1d(hidden_dim)
        
        self.fc3 = nn.Linear(hidden_dim, hidden_dim)
        self.bn3 = nn.BatchNorm1d(hidden_dim)
        
        self.fc4 = nn.Linear(hidden_dim, hidden_dim)
        self.bn4 = nn.BatchNorm1d(hidden_dim)
        
        # Dropout layer
        self.dropout = nn.Dropout(p=dropout)
        
        # Output branches (No BN/Dropout typically on the final projection heads)
        self.forecast_head = nn.Linear(hidden_dim, output_dim)
        self.backcast_head = nn.Linear(hidden_dim, input_dim)
        
    def forward(self, x):
        # Feature extraction block 1
        h = self.fc1(x)
        h = self.bn1(h)
        h = torch.relu(h)
        h = self.dropout(h)

        # Feature extraction block 2
        h = self.fc2(h)
        h = self.bn2(h)
        h = torch.relu(h)
        h = self.dropout(h)
        
        # Feature extraction block 3
        h = self.fc3(h)
        h = self.bn3(h)
        h = torch.relu(h)
        h = self.dropout(h)
        
        # Feature extraction block 4
        h = self.fc4(h)
        h = self.bn4(h)
        h = torch.relu(h)
        h = self.dropout(h)
        
        # Generate forecast and backcast
        forecast = self.forecast_head(h)
        backcast = self.backcast_head(h)
        
        return forecast, backcast

class NBeats(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, num_blocks, dropout=0.1):
        super(NBeats, self).__init__()
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.num_blocks = num_blocks
        
        self.blocks = nn.ModuleList([
            NBeatsBlock(input_dim, output_dim, hidden_dim, dropout) 
            for _ in range(num_blocks)
        ])
        
    def forward(self, x):
        batch_size = x.shape[0]
        
        # Initialize residual and forecast accumulator
        residual = x.clone()
        forecast_sum = torch.zeros(batch_size, self.output_dim, device=x.device)
        
        for block in self.blocks:
            # Get forecast and backcast from the current block
            forecast, backcast = block(residual)
            
            # Accumulate forecasts
            forecast_sum += forecast
            
            # Update residual (subtract backcast)
            residual = residual - backcast
        
        # Final forecast - ensure correct shape
        final_forecast = forecast_sum.squeeze()
        
        # Apply ReLU to ensure positive predictions
        final_forecast = torch.relu(final_forecast)
        
        # Clip to minimum threshold
        final_forecast = torch.maximum(final_forecast, torch.tensor(0.1, device=x.device))
        
        return final_forecast