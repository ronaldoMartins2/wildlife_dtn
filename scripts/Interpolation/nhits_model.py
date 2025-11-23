import torch
import torch.nn as nn
import torch.nn.functional as F

class NHITSBlock(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, n_layers=2, dropout=0.1):
        super(NHITSBlock, self).__init__()
        
        self.layers = nn.ModuleList()
        
        # Primeira camada
        self.layers.append(nn.Linear(input_dim, hidden_dim))
        self.layers.append(nn.BatchNorm1d(hidden_dim)) # Batch Norm
        self.layers.append(nn.ReLU())
        self.layers.append(nn.Dropout(dropout))        # Dropout
        
        # Camadas ocultas extras
        for _ in range(n_layers - 1):
            self.layers.append(nn.Linear(hidden_dim, hidden_dim))
            self.layers.append(nn.BatchNorm1d(hidden_dim)) # Batch Norm
            self.layers.append(nn.ReLU())
            self.layers.append(nn.Dropout(dropout))        # Dropout
            
        # Projeção final (backcast e forecast)
        self.backcast_head = nn.Linear(hidden_dim, input_dim)
        self.forecast_head = nn.Linear(hidden_dim, output_dim)
        
    def forward(self, x):
        h = x
        for layer in self.layers:
            h = layer(h)
            
        backcast = self.backcast_head(h)
        forecast = self.forecast_head(h)
        return backcast, forecast

class NHITS(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, num_blocks, dropout=0.1):
        super(NHITS, self).__init__()
        
        # N-HiTS geralmente usa "Stacks", aqui simplificamos como uma lista de blocos
        # para manter consistência com o input/output do seu pipeline
        self.blocks = nn.ModuleList([
            NHITSBlock(input_dim, output_dim, hidden_dim, n_layers=2, dropout=dropout)
            for _ in range(num_blocks)
        ])
        
    def forward(self, x):
        # x shape: (Batch, Input_Dim)
        residual = x
        forecast_sum = torch.zeros_like(x) # Assumindo output_dim ~= input_dim (3 para 3)
        
        # Se output_dim for diferente de input_dim, ajustamos o tensor de soma
        # Mas no seu caso ambos são 3 (Time, Lat, Lon)
        
        for block in self.blocks:
            backcast, forecast = block(residual)
            residual = residual - backcast
            forecast_sum = forecast_sum + forecast
            
        return forecast_sum