import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json

from MLP_model import (
    get_model
)

# Load data
data = pd.read_csv('map_93.csv', header=None, names=['id', 'timestamp', 'longitude', 'latitude'])

'''
# Load the data
data = pd.DataFrame([
    [93, "3/12/14 17:39", -64.877263, -3.03684],
    [93, "3/14/14 4:00", -64.877105, -3.038135],
    [93, "3/14/14 10:02", -64.876807, -3.038255],
    [93, "3/14/14 16:00", -64.877153, -3.03809],
    [93, "3/14/14 22:00", -64.877072, -3.03821],
    [93, "3/15/14 10:01", -64.885507, -3.033158],
    [93, "3/15/14 22:00", -64.887686, -3.031688],
    [93, "3/16/14 4:00", -64.887788, -3.031543],
    [93, "3/16/14 10:00", -64.888473, -3.031137]
], columns=['id', 'timestamp', 'longitude', 'latitude'])
'''

# Convert timestamp to datetime and extract features
data['timestamp'] = pd.to_datetime(data['timestamp'], format='%m/%d/%y %H:%M')
data['hour'] = data['timestamp'].dt.hour
data['minute'] = data['timestamp'].dt.minute

# Normalize longitude and latitude
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
data[['longitude', 'latitude']] = scaler.fit_transform(data[['longitude', 'latitude']])

# Create lagged features
for lag in range(1, 4):  # Use 3 timesteps of lag
    data[f'lon_lag_{lag}'] = data['longitude'].shift(lag)
    data[f'lat_lag_{lag}'] = data['latitude'].shift(lag)

# Drop rows with NaN caused by lagging
data = data.dropna()

# Define input for the model
X = data[['hour', 'minute'] + [f'lon_lag_{lag}' for lag in range(1, 4)] + [f'lat_lag_{lag}' for lag in range(1, 4)]]

# Prepare the initial input for forecasting
initial_input = X.iloc[-1:].values  # Use the latest data

# Forecasting Function
def forecast_multiple_steps(model, initial_input, steps=5):
    forecasts = []
    current_input = initial_input

    for _ in range(steps):
        # Predict next step
        predicted_coords = model.predict(current_input)
        predicted_lon, predicted_lat = predicted_coords[0]
        forecasts.append((predicted_lon, predicted_lat))
        
        # Update input for next step: shift lag values
        new_input = np.array(current_input)
        new_input[:, 2:5] = new_input[:, 1:4]  # Shift longitudes
        new_input[:, 5:8] = new_input[:, 4:7]  # Shift latitudes
        new_input[:, 4] = predicted_lon  # Add new longitude
        new_input[:, 7] = predicted_lat  # Add new latitude
        current_input = new_input

    return forecasts

model = get_model()
# Forecast 5 steps into the future
forecasts = forecast_multiple_steps(model, initial_input, steps=5)

# Inverse transform forecasts to get original longitude and latitude
forecasted_lons = scaler.inverse_transform([[lon, 0] for lon, lat in forecasts])[:, 0]
forecasted_lats = scaler.inverse_transform([[0, lat] for lon, lat in forecasts])[:, 1]

# === JSON PARA LINGUAGEM ===
json_language = 'scripts/Data_preparation/hyperparameters.json'
with open(json_language, encoding='utf-8') as f:
    lang_params = json.load(f)
    language = lang_params["language"]

if language == 'PT_BR':
    json_path = 'scripts/Data_preparation/language_PT_BR.json'
else:
    json_path = 'scripts/Data_preparation/language_US_US.json'

with open(json_path, encoding='utf-8') as f:
    lang = json.load(f)

# Plot the forecasted trajectory
plt.figure(figsize=(8, 6))
plt.plot(forecasted_lons, forecasted_lats, marker='o', label='Forecasted Path', color='blue')

#plt.plot(data['longitude'].iloc[-5:], data['latitude'].iloc[-5:], marker='x', label='Recent Path', color='red')
plt.plot(data['longitude'].iloc[:], data['latitude'].iloc[:], marker='x', label='Recent Path', color='red')

plt.xlabel(lang["xlabel_mlp_forecast"])
plt.ylabel(lang["ylabel_mlp_forecast"])
plt.title(lang["grafico_mlp_forecast"])
plt.legend()
plt.grid(True)

# Save the plot as an image
plt.savefig(f'onca_93_mlp.png')

# Print forecasted values
for i, (lon, lat) in enumerate(zip(forecasted_lons, forecasted_lats), 1):
    print(f"Step {i} - Predicted Longitude: {lon}, Predicted Latitude: {lat}")
