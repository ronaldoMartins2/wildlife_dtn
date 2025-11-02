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
import torch.nn.functional as F

class NBeatsBlock(nn.Module):
    """
    N-BEATS Block com Layer Normalization e Dropout para otimização.
    
    Args:
        input_dim: Dimensão da entrada
        theta_dim: Dimensão do vetor de saída (forecast)
        hidden_dim: Dimensão das camadas ocultas
        dropout_rate: Taxa de dropout (0.0 a 1.0). Default: 0.1
    """
    def __init__(self, input_dim, theta_dim, hidden_dim, dropout_rate=0.1):
        super().__init__()
        
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, hidden_dim)
        self.fc4 = nn.Linear(hidden_dim, theta_dim)
        
        # Substituir BatchNorm por Layer Norm que funciona com tamanho de lote 1
        self.ln1 = nn.LayerNorm(hidden_dim)
        self.ln2 = nn.LayerNorm(hidden_dim)
        self.ln3 = nn.LayerNorm(hidden_dim)
        
        self.dropout = nn.Dropout(dropout_rate)
        
    def forward(self, x):
        # Substituir chamadas de BatchNorm por LayerNorm
        h = self.fc1(x)
        h = self.ln1(h)
        h = F.relu(h)
        h = self.dropout(h)
        
        h = self.fc2(h)
        h = self.ln2(h)
        h = F.relu(h)
        h = self.dropout(h)
        
        h = self.fc3(h)
        h = self.ln3(h)
        h = F.relu(h)
        h = self.dropout(h)
        
        theta = self.fc4(h)
        return theta

class NBeats(nn.Module):
    """
    Modelo N-BEATS com Layer Normalization e Dropout para otimização.
    
    Args:
        input_dim: Dimensão da entrada
        output_dim: Dimensão da saída (forecast)
        hidden_dim: Dimensão das camadas ocultas
        num_blocks: Número de blocos N-BEATS
        dropout_rate: Taxa de dropout (0.0 a 1.0). Default: 0.1
    """
    def __init__(self, input_dim, output_dim, hidden_dim, num_blocks, dropout_rate=0.1):
        super(NBeats, self).__init__()
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.num_blocks = num_blocks
        self.dropout_rate = dropout_rate
        
        self.blocks = nn.ModuleList([
            NBeatsBlock(input_dim, output_dim, hidden_dim, dropout_rate) 
            for _ in range(num_blocks)
        ])
        
    def forward(self, x):
        batch_size = x.shape[0]
        
        # Inicializar resíduo e acumulador de previsão
        residual = x.clone()  # Shape: (batch_size, input_dim)
        forecast_sum = torch.zeros(batch_size, self.output_dim, device=x.device)
        
        for block in self.blocks:
            # Obter previsão e backcast do bloco atual
            forecast, backcast = block(residual)
            
            # Acumular previsões
            forecast_sum += forecast
            
            # Atualizar resíduo (subtrair backcast)
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
    
class NBeatsNet(nn.Module):
    def __init__(self, input_dim, hidden_dim, theta_dim, num_blocks=3, dropout_rate=0.1):
        super().__init__()
        self.blocks = nn.ModuleList([
            NBeatsBlock(
                input_dim=input_dim,
                theta_dim=theta_dim,
                hidden_dim=hidden_dim,
                dropout_rate=dropout_rate
            )
            for _ in range(num_blocks)
        ])

    def forward(self, x):
        residual = x
        forecasts = []
        
        for block in self.blocks:
            # Process one block at a time
            forecast = block(residual)
            residual = residual - forecast
            forecasts.append(forecast)
            
        return torch.stack(forecasts, dim=1)

class NBeatsBlock(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, dropout_rate=0.1, use_batch_norm=True):
        super(NBeatsBlock, self).__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, output_dim)
        self.fc_res = nn.Linear(output_dim, input_dim)
        
        self.dropout = nn.Dropout(dropout_rate)
        self.use_batch_norm = use_batch_norm
        
        if use_batch_norm:
            self.bn1 = nn.LayerNorm(hidden_dim)  # Using LayerNorm instead of BatchNorm
            self.bn2 = nn.LayerNorm(hidden_dim)

    def forward(self, x):
        x_residual = x
        x = self.fc1(x)
        if self.use_batch_norm:
            x = self.bn1(x)
        x = torch.relu(x)
        x = self.dropout(x)
        
        x = self.fc2(x)
        if self.use_batch_norm:
            x = self.bn2(x)
        x = torch.relu(x)
        x = self.dropout(x)
        
        forecast = self.fc3(x)
        forecast = self.fc_res(forecast)
        return forecast

class NBeats(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, num_blocks, dropout_rate=0.1, use_batch_norm=True):
        super(NBeats, self).__init__()
        self.blocks = nn.ModuleList([
            NBeatsBlock(
                input_dim, 
                output_dim, 
                hidden_dim, 
                dropout_rate=dropout_rate,
                use_batch_norm=use_batch_norm
            ) for _ in range(num_blocks)
        ])

    def forward(self, x):
        forecasts = []
        for block in self.blocks:
            forecast = block(x)
            forecasts.append(forecast)
            x = x + forecast  # Residual connection

        final_forecast = sum(forecasts) / len(forecasts)
        final_forecast = final_forecast.view(-1)
        final_forecast = torch.relu(final_forecast)
        final_forecast = torch.maximum(final_forecast, torch.tensor(0.1))
        
        # Ensure output has shape [batch_size, num_features]
        final_forecast = sum(forecasts) / len(forecasts)
        if final_forecast.dim() == 1:
            final_forecast = final_forecast.unsqueeze(-1)
        
        return final_forecast