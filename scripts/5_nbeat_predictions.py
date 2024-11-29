import pandas as pd
import torch
import torch.nn as nn
from datetime import timedelta

# Define the NBeatsBlock with a residual connection fix
class NBeatsBlock(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim):
        super(NBeatsBlock, self).__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, output_dim)  # Output is scalar (1 value)
        self.fc_res = nn.Linear(output_dim, input_dim)  # Residual connection adjustment

    def forward(self, x):
        x_residual = x  # Save the residual input for later addition

        x = torch.relu(self.fc1(x))  # Shape: [batch_size, hidden_dim]
        x = torch.relu(self.fc2(x))  # Shape: [batch_size, hidden_dim]
        forecast = self.fc3(x)  # Shape: [batch_size, output_dim], output_dim = 1

        forecast = self.fc_res(forecast)  # Adjust the forecast for residual connection [batch_size, input_dim]
        return forecast

# Define the NBeats model
class NBeats(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, num_blocks):
        super(NBeats, self).__init__()
        self.blocks = nn.ModuleList([NBeatsBlock(input_dim, output_dim, hidden_dim) for _ in range(num_blocks)])

    def forward(self, x):
        forecasts = []
        for block in self.blocks:
            forecast = block(x)  # Pass the input through each block
            forecasts.append(forecast)
            x = x + forecast  # Residual connection (now they have the same shape)

        # Average the forecasts, resulting in shape [batch_size, output_dim]
        final_forecast = sum(forecasts) / len(forecasts)

        # Flatten the output to match the target shape: [batch_size]
        return final_forecast.view(-1)  # Ensure it's a 1D tensor of size [batch_size]

# Sample data (as you provided)
data = [
    [93, '3/12/14 17:39', -64.877263, -3.03684],
    [93, '3/14/14 4:00', -64.877105, -3.038135],
    [93, '3/14/14 10:02', -64.876807, -3.038255],
    [93, '3/14/14 16:00', -64.877153, -3.03809],
    [93, '3/14/14 22:00', -64.877072, -3.03821],
    [93, '3/15/14 10:01', -64.885507, -3.033158],
    [93, '3/15/14 22:00', -64.887686, -3.031688],
    [93, '3/16/14 4:00', -64.887788, -3.031543],
    [93, '3/16/14 10:00', -64.888473, -3.031137],
    [93, '3/16/14 22:00', -64.888699, -3.031063],
    [93, '3/17/14 4:00', -64.888955, -3.03231],
    [93, '3/17/14 10:01', -64.886187, -3.033822],
    [93, '3/17/14 22:00', -64.886215, -3.033063],
    [93, '3/18/14 4:02', -64.879131, -3.038117]
]

# Create a DataFrame
df = pd.DataFrame(data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude'])

# Convert the 'Timestamp' column to datetime objects
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%m/%d/%y %H:%M')

# Calculate the time differences between consecutive timestamps in hours
df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600

# Drop the first row since it will have a NaN value for 'Time Difference (hours)'
df = df.dropna(subset=['Time Difference (hours)'])

# Let's use the time differences as the target and the latitude, longitude, and previous time differences as features.
df['Prev Time Difference (hours)'] = df['Time Difference (hours)'].shift(1)

# Drop the NaN value created by the shift (first row)
df = df.dropna(subset=['Prev Time Difference (hours)'])

# Feature columns
features = ['Prev Time Difference (hours)', 'Longitude', 'Latitude']

# Target column
target = 'Time Difference (hours)'

# Prepare the data for training
X = df[features].values
y = df[target].values

# Convert to PyTorch tensors
X_tensor = torch.tensor(X, dtype=torch.float32)
y_tensor = torch.tensor(y, dtype=torch.float32)

# Hyperparameters
input_dim = X_tensor.shape[1]  # Number of features (Prev Time Difference, Longitude, Latitude)
#output_dim = 1  # We predict one time difference value
output_dim = 6  # We predict one time difference value - TO FIX
#hidden_dim = 64  # Hidden layer size
hidden_dim = 6  # Hidden layer size - TO FIX
#num_blocks = 3  # Number of N-BEATS blocks
num_blocks = 6  # Number of N-BEATS blocks - TO FIX

print(f'input_dim >>>>>>>>>>>> {input_dim}')

'''
input_dim = 6  # Input size (number of past time steps)
output_dim = 6  # Output size (number of future time steps)
hidden_dim = 6  # Hidden layer size
num_blocks = 3  # Number of N-BEATS blocks
'''

# Create the model
model = NBeats(input_dim, output_dim, hidden_dim, num_blocks)

# Example training loop
criterion = nn.MSELoss()  # Mean Squared Error Loss
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

# Ensure the target tensor is reshaped correctly to have the same shape as the forecast
y_tensor = y_tensor.view(-1, 1)  # Reshape to (12, 1) if the model is predicting single values

# Training loop
for epoch in range(100):  # 100 epochs
    model.train()  # Set the model to training mode
    optimizer.zero_grad()  # Zero the gradients

    # Forward pass
    forecast = model(X_tensor)

    print(f"Forecast shape: {forecast.shape}, Target shape: {y_tensor.shape}")

    # Ensure the forecast and target have the same shape for loss calculation
    loss = criterion(forecast, y_tensor)

    # Backward pass and optimization
    loss.backward()
    optimizer.step()

    if epoch % 10 == 0:
        print(f"Epoch {epoch}, Loss: {loss.item():.4f}")

# Once trained, generate new data using the model's predictions:
# Predict time differences for the next entries


# Generate predictions for 5 future data points
new_data = []
current_timestamp = df['Timestamp'].iloc[-1]  # Start with the last timestamp

for _ in range(5):
    # Prepare the input for the model (use the last known values from the previous row)
    last_row = df.iloc[-1]
    last_features = torch.tensor([[last_row['Prev Time Difference (hours)'], last_row['Longitude'], last_row['Latitude']]], dtype=torch.float32)

    # Predict the next time differences
    forecast = model(last_features)  # Model output is shape [1, 6] or [6]

    # Print forecast shape for debugging
    print(f"Forecast shape: {forecast.shape}")

    # Access the entire prediction vector (if it's a 1D tensor)
    predicted_time_diff = forecast.tolist()  # Convert the entire tensor to a list of predictions
    print(f"Predicted Time Differences: {predicted_time_diff}")

    # If you want to access a specific predicted value, e.g., the first time difference:
    first_predicted_time_diff = forecast[0].item()  # Get the first predicted value and convert it to scalar
    print(f"First Predicted Time Difference: {first_predicted_time_diff} hours")

    # Calculate the next timestamp using the predicted time difference (e.g., using the first prediction)
    new_timestamp = current_timestamp + timedelta(hours=first_predicted_time_diff)

    # Print column names for debugging
    print(f'df.columns: {df.columns}')

    # Append the new entry to the data with the correct number of columns
    # Here, you need to match the original df.columns structure (6 columns)
    new_data.append([
        93,
        new_timestamp.strftime('%m/%d/%y %H:%M'),
        last_row['Longitude'],
        last_row['Latitude'],
        last_row['Time Difference (hours)'],
        last_row['Prev Time Difference (hours)']
    ])

    # Update current_timestamp and last_row for the next iteration
    current_timestamp = new_timestamp

    # Use pd.concat to append the new row to the dataframe
    #new_row_df = pd.DataFrame([new_data[-1]], columns=df.columns)  # Ensure the new row has the same columns as df
    new_row_df = pd.DataFrame([new_data[-1]], columns=df.columns)  # Ensure the new row has the same columns as df

    df = pd.concat([df, new_row_df], ignore_index=True)

# New generated data
#new_df = pd.DataFrame(new_data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude', 'Predicted Time Difference (hours)', 'Time Difference (hours)', 'Prev Time Difference (hours)'])
new_df = pd.DataFrame(new_data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude', 'Time Difference (hours)', 'Prev Time Difference (hours)'])
print(new_df)

