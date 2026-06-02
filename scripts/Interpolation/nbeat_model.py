import torch
import torch.nn as nn

class NBeatsBlock(nn.Module):
    def __init__(self, input_dim, hidden_dim, forecast_dim, backcast_dim):
        """
        Args:
            input_dim: Flattened input size (Input Steps * Features)
            hidden_dim: Hidden layer size
            forecast_dim: Flattened forecast size (Horizon * Features)
            backcast_dim: Flattened backcast size (Input Steps * Features) usually same as input_dim
        """
        super(NBeatsBlock, self).__init__()
        
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, hidden_dim)
        self.fc4 = nn.Linear(hidden_dim, hidden_dim)
        
        self.forecast_head = nn.Linear(hidden_dim, forecast_dim)
        self.backcast_head = nn.Linear(hidden_dim, backcast_dim)
        
    def forward(self, x):
        h = torch.relu(self.fc1(x))
        h = torch.relu(self.fc2(h))
        h = torch.relu(self.fc3(h))
        h = torch.relu(self.fc4(h))
        
        forecast = self.forecast_head(h)
        backcast = self.backcast_head(h)
        return forecast, backcast

class NBeats(nn.Module):
    def __init__(self, input_steps, output_steps, input_features, hidden_dim, num_blocks):
        super(NBeats, self).__init__()
        self.input_steps = input_steps
        self.output_steps = output_steps
        self.input_features = input_features
        self.hidden_dim = hidden_dim
        self.num_blocks = num_blocks
        
        # Flattened dimensions
        self.input_flat_dim = input_steps * input_features
        self.output_flat_dim = output_steps * input_features
        
        self.blocks = nn.ModuleList([
            NBeatsBlock(
                input_dim=self.input_flat_dim, 
                hidden_dim=hidden_dim,
                forecast_dim=self.output_flat_dim,
                backcast_dim=self.input_flat_dim
            ) 
            for _ in range(num_blocks)
        ])
        
    def forward(self, x):
        # x shape: (Batch, Input Steps, Features)
        batch_size = x.shape[0]
        
        # Flatten input
        x_flat = x.view(batch_size, -1)
        
        residual = x_flat.clone()
        forecast_sum = torch.zeros(batch_size, self.output_flat_dim, device=x.device)
        
        for block in self.blocks:
            forecast, backcast = block(residual)
            forecast_sum += forecast
            residual = residual - backcast
            
        # Reshape output to (Batch, Horizon, Features)
        final_forecast = forecast_sum.view(batch_size, self.output_steps, self.input_features)
        
        return final_forecast