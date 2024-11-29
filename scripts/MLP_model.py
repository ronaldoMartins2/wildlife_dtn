
# pip install tensorflow 

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense

# Load data
data = pd.read_csv('map_93.csv', header=None, names=['id', 'timestamp', 'longitude', 'latitude'])

# Filter for specific ID
data = data[data['id'] == 93]

# Convert timestamp to datetime and extract time features
data['timestamp'] = pd.to_datetime(data['timestamp'], format='%m/%d/%y %H:%M')
data['hour'] = data['timestamp'].dt.hour
data['minute'] = data['timestamp'].dt.minute

# Normalize features
scaler = MinMaxScaler()
data[['longitude', 'latitude']] = scaler.fit_transform(data[['longitude', 'latitude']])


# Create lagged features
for lag in range(1, 4):  # Use last 3 timesteps
    data[f'lon_lag_{lag}'] = data['longitude'].shift(lag)
    data[f'lat_lag_{lag}'] = data['latitude'].shift(lag)

# Drop rows with NaN values (caused by lagging)
data = data.dropna()

# Define input and output
X = data[['hour', 'minute'] + [f'lon_lag_{lag}' for lag in range(1, 4)] + [f'lat_lag_{lag}' for lag in range(1, 4)]]
y = data[['longitude', 'latitude']]

# Split into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


def get_model():
    # Build MLP model
    model = Sequential([
        Dense(64, activation='relu', input_dim=X_train.shape[1]),
        Dense(32, activation='relu'),
        Dense(16, activation='relu'),
        Dense(2)  # Two outputs: latitude and longitude
    ])

    model.compile(optimizer='adam', loss='mse', metrics=['mae'])

    # Train the model
    model.fit(X_train, y_train, epochs=100, batch_size=16, validation_split=0.2)

    # Evaluate the model
    loss, mae = model.evaluate(X_test, y_test)
    print(f"Test Loss: {loss}, Test MAE: {mae}")

    return model

