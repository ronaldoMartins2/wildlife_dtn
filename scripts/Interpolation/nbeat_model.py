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
    def __init__(self, input_dim, output_dim, hidden_dim):
        super(NBeatsBlock, self).__init__()
        self.input_dim = input_dim
        self.output_dim = output_dim
        
        # Feature extraction layers
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, hidden_dim)
        self.fc4 = nn.Linear(hidden_dim, hidden_dim)
        
        # Output branches
        self.forecast_head = nn.Linear(hidden_dim, output_dim)  # For predictions
        self.backcast_head = nn.Linear(hidden_dim, input_dim)   # For residual connection
        
    def forward(self, x):
        # Feature extraction
        h = torch.relu(self.fc1(x))
        h = torch.relu(self.fc2(h))
        h = torch.relu(self.fc3(h))
        h = torch.relu(self.fc4(h))
        
        # Generate forecast and backcast
        forecast = self.forecast_head(h)  # Shape: (batch_size, output_dim)
        backcast = self.backcast_head(h)  # Shape: (batch_size, input_dim)
        
        return forecast, backcast

class NBeats(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, num_blocks):
        super(NBeats, self).__init__()
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.num_blocks = num_blocks
        
        self.blocks = nn.ModuleList([
            NBeatsBlock(input_dim, output_dim, hidden_dim) 
            for _ in range(num_blocks)
        ])
        
    def forward(self, x):
        batch_size = x.shape[0]
        
        # Initialize residual and forecast accumulator
        residual = x.clone()  # Shape: (batch_size, input_dim)
        forecast_sum = torch.zeros(batch_size, self.output_dim, device=x.device)
        
        for block in self.blocks:
            # Get forecast and backcast from the current block
            forecast, backcast = block(residual)
            
            # Accumulate forecasts
            forecast_sum += forecast
            
            # Update residual (subtract backcast)
            residual = residual - backcast
        
        final_forecast = forecast_sum
        #Para voltar ao normal, descomentar as linhas abaixo
        # Final forecast - ensure correct shape
        #final_forecast = forecast_sum.squeeze()  # Remove unnecessary dimensions
        
        # Apply ReLU to ensure positive predictions (if needed for your use case)
        #final_forecast = torch.relu(final_forecast)
        
        # Clip to minimum threshold if necessary
        #final_forecast = torch.maximum(final_forecast, torch.tensor(0.1, device=x.device))
        
        return final_forecast