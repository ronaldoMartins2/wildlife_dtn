import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from datetime import timedelta

# Define the NHiTSBlock with hierarchical time series forecasting mechanism
class NHiTSBlock(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, num_hierarchies):
        super(NHiTSBlock, self).__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, output_dim)  # Output is scalar (1 value)
        
        # Optional: If you want residual connections, ensure they match input/output dimensions
        self.fc_res = nn.Linear(output_dim, hidden_dim)  # Fix the dimension mismatch here
        
        self.num_hierarchies = num_hierarchies  # Number of hierarchical levels

    def forward(self, x):
        # Initial forecast (without residual connection)
        forecast = torch.relu(self.fc1(x))  # Shape: [batch_size, hidden_dim]
        forecast = torch.relu(self.fc2(forecast))  # Shape: [batch_size, hidden_dim]
        forecast = self.fc3(forecast)  # Shape: [batch_size, output_dim]

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


# Define the NHiTS model: Predict Longitude, Latitude, and Time Difference
class NHiTS(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, num_blocks, num_hierarchies):
        super(NHiTS, self).__init__()
        self.blocks = nn.ModuleList([NHiTSBlock(input_dim, output_dim, hidden_dim, num_hierarchies) for _ in range(num_blocks)])

    def forward(self, x):
        forecasts = []
        for block in self.blocks:
            block_forecasts = block(x)  # Get hierarchical forecasts from each block
            forecasts.append(block_forecasts)

        # Aggregate all forecasts from each block and hierarchy level
        aggregated_forecasts = [torch.mean(torch.stack([forecast[i] for forecast in forecasts]), dim=0)
                                for i in range(len(forecasts[0]))]

        # Return all three predicted values: time_diff, longitude, latitude
        return aggregated_forecasts


# Sample hyperparameters (adjust as needed)
input_dim = 3  # Features: 'Prev Time Difference (hours)', 'Longitude', 'Latitude'
output_dim = 3  # Now outputting three values: time_diff, longitude, and latitude
hidden_dim = 6  # Hidden layer size
num_blocks = 6  # Number of NHiTS blocks
num_hierarchies = 3  # Number of hierarchical levels

# Initialize the NHiTS model
model = NHiTS(input_dim, output_dim, hidden_dim, num_blocks, num_hierarchies)

# Example function to predict between two dates
def predict_between_dates(start_date, end_date, df, model, num_steps=5):
    current_timestamp = start_date
    new_data = []

    # Get the last known row (most recent data point)
    last_row = df.iloc[-1]

    # Feature: Prev Time Difference (hours), Longitude, Latitude
    prev_time_diff = (current_timestamp - last_row['Timestamp']).total_seconds() / 3600.0  # in hours
    
    # Directly accessing values for Longitude and Latitude as scalars
    longitude = last_row['Longitude']
    latitude = last_row['Latitude']

    # Prepare the tensor for model input
    last_features = torch.tensor([[prev_time_diff, longitude, latitude]], dtype=torch.float32)

    while current_timestamp <= end_date:
        forecast = model(last_features)  # Get the forecast from the model
        
        # Access the forecast from the first hierarchy level (main prediction)
        predicted_values = forecast[0][0].detach().numpy()  # forecast[0] is the first block, [0] is the main forecast

        # Extract the 3 predicted values: time_diff, longitude, latitude
        predicted_time_diff = predicted_values[0]
        predicted_longitude = predicted_values[1]
        predicted_latitude = predicted_values[2]

        # Print the predicted values for debugging
        print(f"Predicted Time Diff: {predicted_time_diff} hours, Predicted Longitude: {predicted_longitude}, Predicted Latitude: {predicted_latitude}")

        # If the predicted time difference is too small, break the loop to avoid endless tiny steps
        if predicted_time_diff < 0.1:  # Adjust this threshold based on your needs
            print("Predicted time difference too small. Stopping predictions.")
            break

        # Convert predicted_time_diff to float before passing it to timedelta
        predicted_time_diff = float(predicted_time_diff)

        # Calculate the next timestamp using the predicted time difference
        new_timestamp = current_timestamp + timedelta(hours=predicted_time_diff)

        # Save the forecast data point
        new_data.append([last_row['ID'], new_timestamp.strftime('%m/%d/%y %H:%M'), predicted_longitude, predicted_latitude])

        # Update current timestamp and features for the next iteration
        current_timestamp = new_timestamp
        
        # Update the input features for the model
        last_features = torch.tensor([[predicted_time_diff, predicted_longitude, predicted_latitude]], dtype=torch.float32)

    return pd.DataFrame(new_data, columns=['ID', 'Timestamp', 'Longitude', 'Latitude'])


# Save the results to CSV
def save_to_csv(df, filename):
    df.to_csv(filename, index=False, header=True)
    print(f"Results saved to {filename}")

# Example usage:

# Create a sample dataframe (adjust according to your dataset)
data = {
    'ID': [94],
    'Timestamp': pd.to_datetime(['01/26/15 05:53']),
    'Longitude': [-64.908878],
    'Latitude': [-2.852821]
}
df = pd.DataFrame(data)

# Define start_date and end_date for the prediction range
start_date = pd.to_datetime('6/1/15 18:07')
end_date = pd.to_datetime('12/25/15 18:13')

# Run the prediction
predicted_df = predict_between_dates(start_date, end_date, df, model)

# Display the predictions
#print(predicted_df)

# Save the predictions to a CSV file
save_to_csv(predicted_df, 'map_94_nhits_interpolation.csv')
