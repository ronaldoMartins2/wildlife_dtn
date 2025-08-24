import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np

# Define the NHiTSBlock with hierarchical time series forecasting mechanism
class NHiTSBlock(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_hierarchies):
        super(NHiTSBlock, self).__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, hidden_dim)  # Output is hidden_dim (not scalar)
        
        # Optional: If you want residual connections, ensure they match input/output dimensions
        self.fc_res = nn.Linear(hidden_dim, hidden_dim)  # Fix the dimension mismatch here
        
        self.num_hierarchies = num_hierarchies  # Number of hierarchical levels

    def forward(self, x):
        # Initial forecast (without residual connection)
        forecast = torch.relu(self.fc1(x))  # Shape: [batch_size, hidden_dim]
        forecast = torch.relu(self.fc2(forecast))  # Shape: [batch_size, hidden_dim]
        forecast = self.fc3(forecast)  # Shape: [batch_size, hidden_dim]

        # Initialize forecast_residual as forecast (first step)
        forecast_residual = forecast  # The initial residual is just the forecast itself

        # Apply residual connections (hierarchical forecasting)
        hierarchical_forecasts = [forecast]  # List to store forecasts at different levels
        for _ in range(self.num_hierarchies - 1):
            forecast_residual = self.fc_res(forecast_residual)  # Shape: [batch_size, hidden_dim]
            forecast_residual = torch.relu(forecast_residual)  # Ensure activation for residuals
            forecast_residual = self.fc3(forecast_residual)  # Apply output layer to residual forecast
            hierarchical_forecasts.append(forecast_residual)
        
        return hierarchical_forecasts  # Return hierarchical forecasts


# Define the NHiTS model
class NHits(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_blocks, num_hierarchies):
        super(NHits, self).__init__()
        self.blocks = nn.ModuleList([NHiTSBlock(input_dim, hidden_dim, num_hierarchies) for _ in range(num_blocks)])
        
        # Separate output layers for time difference, longitude, and latitude
        self.fc_time = nn.Linear(hidden_dim, 1)  # For time difference
        self.fc_lon = nn.Linear(hidden_dim, 1)  # For longitude
        self.fc_lat = nn.Linear(hidden_dim, 1)  # For latitude

    def forward(self, x):
        forecasts = []
        for block in self.blocks:
            block_forecasts = block(x)  # Get hierarchical forecasts from each block
            forecasts.append(block_forecasts)

        # Aggregate all forecasts from each block and hierarchy level
        aggregated_forecasts = [torch.mean(torch.stack([forecast[i] for forecast in forecasts]), dim=0)
                                for i in range(len(forecasts[0]))]

        # The aggregated forecast for each hierarchy is expected to be of shape [batch_size, hidden_dim]
        # We take the first forecast from the first block for simplicity, which should have shape [batch_size, hidden_dim]
        aggregated_forecast = aggregated_forecasts[0]

        # Separate predictions for time difference, longitude, and latitude
        time_diff = self.fc_time(aggregated_forecast)  # Output shape: [batch_size, 1]
        longitude = self.fc_lon(aggregated_forecast)  # Output shape: [batch_size, 1]
        latitude = self.fc_lat(aggregated_forecast)  # Output shape: [batch_size, 1]

        return time_diff.view(-1), longitude.view(-1), latitude.view(-1)
