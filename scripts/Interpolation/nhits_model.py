import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np

class NHiTSBlock(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_hierarchies):
        super(NHiTSBlock, self).__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, hidden_dim)  
        
        self.fc_res = nn.Linear(hidden_dim, hidden_dim)  
        
        self.num_hierarchies = num_hierarchies  

    def forward(self, x):
        forecast = torch.relu(self.fc1(x)) 
        forecast = torch.relu(self.fc2(forecast))  
        forecast = self.fc3(forecast)  

       
        forecast_residual = forecast

        
        hierarchical_forecasts = [forecast]  
        for _ in range(self.num_hierarchies - 1):
            forecast_residual = self.fc_res(forecast_residual) 
            forecast_residual = torch.relu(forecast_residual) 
            forecast_residual = self.fc3(forecast_residual)  
            hierarchical_forecasts.append(forecast_residual)
        
        return hierarchical_forecasts  

class NHits(nn.Module):
    def __init__(self, input_dim, hidden_dim, num_blocks, num_hierarchies):
        super(NHits, self).__init__()
        self.blocks = nn.ModuleList([NHiTSBlock(input_dim, hidden_dim, num_hierarchies) for _ in range(num_blocks)])
        
        self.fc_time = nn.Linear(hidden_dim, 1) 
        self.fc_lat = nn.Linear(hidden_dim, 1)  

    def forward(self, x):
        forecasts = []
        for block in self.blocks:
            block_forecasts = block(x) 
            forecasts.append(block_forecasts)

        aggregated_forecasts = [torch.mean(torch.stack([forecast[i] for forecast in forecasts]), dim=0)
                                for i in range(len(forecasts[0]))]

        aggregated_forecast = aggregated_forecasts[0]

        time_diff = self.fc_time(aggregated_forecast)  
        longitude = self.fc_lon(aggregated_forecast)  
        latitude = self.fc_lat(aggregated_forecast)  

        return time_diff.view(-1), longitude.view(-1), latitude.view(-1)
