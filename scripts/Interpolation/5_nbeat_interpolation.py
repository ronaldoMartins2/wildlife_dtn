import pandas as pd
import torch
import torch.nn as nn
from datetime import timedelta
import sys

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

def getDataFromCSV( current_animal ):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(f'map_{current_animal}.csv', header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])

    return df

# Create a DataFrame
#df = pd.DataFrame(data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude'])

# Convert the 'Timestamp' column to datetime objects
#df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%m/%d/%y %H:%M')

current_animal = sys.argv [1]

df = getDataFromCSV( current_animal )

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
output_dim = 6  # Output: predict multiple future time steps
hidden_dim = 6  # Hidden layer size
num_blocks = 6  # Number of N-BEATS blocks

# Create the model
model = NBeats(input_dim, output_dim, hidden_dim, num_blocks)

# Training loop (for demonstration)
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

    #print(f"Forecast shape: {forecast.shape}, Target shape: {y_tensor.shape}")

    # Ensure the forecast and target have the same shape for loss calculation
    loss = criterion(forecast, y_tensor)

    # Backward pass and optimization
    loss.backward()
    optimizer.step()

    if epoch % 10 == 0:
        print(f"Epoch {epoch}, Loss: {loss.item():.4f}")

# Function to predict values between dates
def predict_between_dates(start_date, end_date, df, model, num_steps=5):
    new_data = []
    current_timestamp = start_date

    while current_timestamp <= end_date:
        # Prepare the input for the model (use the last known values from the previous row)
        last_row = df.iloc[-1]
        last_features = torch.tensor([[last_row['Prev Time Difference (hours)'], last_row['Longitude'], last_row['Latitude']]], dtype=torch.float32)

        # Predict the next time difference (forecasting multiple steps)
        forecast = model(last_features)

        #print(f"Forecast shape: {forecast.shape}")  # This will show [1, 6] (6 time steps)

        # We can choose how to use the forecast vector. Here we use the first predicted time difference.
        predicted_time_diff = forecast[0].item()  # Use the first predicted time difference as a scalar
        #print(f"Predicted Time Difference: {predicted_time_diff} hours")

        # Calculate the next timestamp using the predicted time difference
        new_timestamp = current_timestamp + timedelta(hours=predicted_time_diff)

        # Append the new entry to the data with the correct number of columns
        new_data.append([current_animal, new_timestamp.strftime('%m/%d/%y %H:%M'), last_row['Longitude'], last_row['Latitude'],
                         last_row['Time Difference (hours)'], last_row['Prev Time Difference (hours)']])

        # Update current_timestamp and last_row for the next iteration
        current_timestamp = new_timestamp
        df = pd.concat([df, pd.DataFrame([new_data[-1]], columns=df.columns)], ignore_index=True)

    return pd.DataFrame(new_data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude', 'Time Difference (hours)', 'Prev Time Difference (hours)'])

def find_min_max_dates():
    """
    Reads a CSV file and identifies the earliest and latest dates in the 'Datetime' column.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        tuple: A tuple containing the earliest and latest dates.
    """
    # Load the CSV file without assuming a header
    data = pd.read_csv(f'map_{current_animal}.csv', header=None)

    # Rename columns for clarity (modify as per actual column names)
    data.columns = ['ID', 'Datetime', 'Longitude', 'Latitude']

    # Display the first few rows of the 'Datetime' column for validation
    print("Sample of 'Datetime' column:")
    print(data['Datetime'].head())

    # Convert the 'Datetime' column to datetime format
    data['Datetime'] = pd.to_datetime(data['Datetime'], format='%m/%d/%y %H:%M', errors='coerce')

    # Check for rows with invalid or missing dates
    invalid_dates = data[data['Datetime'].isna()]
    if not invalid_dates.empty:
        print("Warning: Some rows have invalid or missing dates:")
        print(invalid_dates)

    # Drop rows with invalid dates
    data = data.dropna(subset=['Datetime'])

    # Find the minimum and maximum dates
    min_date = data['Datetime'].min().strftime('%m/%d/%y %H:%M')
    max_date = data['Datetime'].max().strftime('%m/%d/%y %H:%M')

    return min_date, max_date
# Define start and end dates

#start_date_str = '3/18/14 4:02'
#end_date_str = '3/19/14 4:02'
start_date_str, end_date_str = find_min_max_dates()
print(start_date_str, end_date_str)
#start_date_str = '3/13/14 4:02'
#end_date_str = '3/17/14 4:02'

start_date = pd.to_datetime( start_date_str, format='%m/%d/%y %H:%M')
end_date = pd.to_datetime(end_date_str, format='%m/%d/%y %H:%M')
print(start_date, end_date)
# Call the function to predict data between the given dates
predicted_df = predict_between_dates(start_date, end_date, df, model)

# Display the predicted data
print(predicted_df)

#predicted_df.to_csv(f'map_{current_animal}_interpolation.csv', index=False)

columns_to_save = ['ID', 'Timestamp', 'Longitude', 'Latitude']
predicted_df[columns_to_save].to_csv( f'map_{current_animal}_interpolation.csv', index=False, header=False)