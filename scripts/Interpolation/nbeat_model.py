import torch
import torch.nn as nn

class NBeatsBlock(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim):
        super(NBeatsBlock, self).__init__()
        self.input_dim = input_dim
        self.output_dim = output_dim
        
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, hidden_dim)
        self.fc4 = nn.Linear(hidden_dim, hidden_dim)
        
        self.forecast_head = nn.Linear(hidden_dim, output_dim)
        self.backcast_head = nn.Linear(hidden_dim, input_dim)
        
    def forward(self, x):
        h = torch.relu(self.fc1(x))
        h = torch.relu(self.fc2(h))
        h = torch.relu(self.fc3(h))
        h = torch.relu(self.fc4(h))
        
        forecast = self.forecast_head(h)
        backcast = self.backcast_head(h)
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
        # x shape: (batch, input_dim)
        residual = x.clone()
        forecast_sum = torch.zeros(x.shape[0], self.output_dim, device=x.device)
        
        for block in self.blocks:
            forecast, backcast = block(residual)
            forecast_sum += forecast
            residual = residual - backcast
        
        # O Output tem 3 dimensões: [TimeDiff, Longitude, Latitude]
        # Aplicamos restrição APENAS no TimeDiff (índice 0)
        
        # Separa as previsões
        pred_time = forecast_sum[:, 0]
        pred_coords = forecast_sum[:, 1:]
        
        # Garante tempo positivo (mínimo 0.01 hora para evitar travar o loop)
        pred_time = torch.relu(pred_time)
        pred_time = torch.maximum(pred_time, torch.tensor(0.01, device=x.device))
        
        # Reconecta (Stack) - Coordenadas ficam livres (podem ser negativas)
        final_forecast = torch.cat((pred_time.unsqueeze(1), pred_coords), dim=1)
        
        return final_forecast