
from n_beat import  NBeats
import torch 

# Hyperparameters
input_dim = 6  # Input size (number of past time steps)
#input_dim = 12  # Input size (number of past time steps)
output_dim = 6  # Output size (number of future time steps)
hidden_dim = 64  # Hidden layer size
num_blocks = 3  # Number of N-BEATS blocks

print('teste')

# Create the model
model = NBeats(input_dim, output_dim, hidden_dim, num_blocks)


# Example input (batch size, input_dim)
# Let's assume you have a batch of 32 samples with 12 time steps each.
# Each row in 'x' represents a different time series.
x = torch.rand(32, input_dim)  # Batch of 32 samples, each with 12 time steps (input_dim)


# Forward pass to get the forecast
forecast = model(x)


# Print the shape of the forecast
print(f"Forecast shape: {forecast.shape}")  # Should be [32, output_dim] (batch_size, forecast_length)


# Forecast for a single sample (e.g., first sample in the batch)
sample_forecast = forecast[0]  # Access the forecast for the first sample in the batch
print("Forecast for first sample:", sample_forecast)

