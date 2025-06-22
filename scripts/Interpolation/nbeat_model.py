import torch
import torch.nn as nn

# Define the NBeatsBlock with residual connection fix
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

# Define the NBeats model
class NBeats(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, num_blocks):
        super(NBeats, self).__init__()
        self.blocks = nn.ModuleList([NBeatsBlock(input_dim, output_dim, hidden_dim) for _ in range(num_blocks)])

    def forward(self, x):
        forecasts = []
        for block in self.blocks:
            forecast = block(x)
            forecasts.append(forecast)
            x = x + forecast
        final_forecast = sum(forecasts) / len(forecasts)
        return final_forecast.view(-1)