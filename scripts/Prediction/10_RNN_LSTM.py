import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

# Step 1: Load Data
raw_data = pd.read_csv('map_93.csv', header=None, names=['id', 'Timestamp', 'Longitude', 'Latitude'])

'''
data = [
    [93, "3/12/14 17:39", -64.877263, -3.03684],
    [93, "3/14/14 4:00", -64.877105, -3.038135],
    [93, "3/14/14 10:02", -64.876807, -3.038255],
    [93, "3/14/14 16:00", -64.877153, -3.03809],
    [93, "3/14/14 22:00", -64.877072, -3.03821],
    [93, "3/15/14 10:01", -64.885507, -3.033158],
    [93, "3/15/14 22:00", -64.887686, -3.031688],
    [93, "3/16/14 4:00", -64.887788, -3.031543],
    [93, "3/16/14 10:00", -64.888473, -3.031137],
]
'''

data = raw_data[:100]

df = pd.DataFrame(data, columns=["ID", "Timestamp", "Longitude", "Latitude"])

# Convert Timestamp to numeric for time forecasting
df['Timestamp'] = pd.to_datetime(df['Timestamp'])
df['TimeNumeric'] = (df['Timestamp'] - df['Timestamp'].min()).dt.total_seconds()

# Step 2: Normalize Data
scaler = MinMaxScaler()
normalized_data = scaler.fit_transform(df[['Longitude', 'Latitude', 'TimeNumeric']])

# Step 3: Create Sequences
def create_sequences(data, sequence_length):
    sequences = []
    targets = []
    for i in range(len(data) - sequence_length):
        sequences.append(data[i:i + sequence_length])
        targets.append(data[i + sequence_length])
    return np.array(sequences), np.array(targets)

sequence_length = 3  # Example sequence length
X, y = create_sequences(normalized_data, sequence_length)

# Step 4: Split Data into Training and Testing Sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Step 5: Build the LSTM Model
model = tf.keras.Sequential([
    tf.keras.layers.LSTM(64, activation='relu', input_shape=(sequence_length, X.shape[2]), return_sequences=False),
    tf.keras.layers.Dense(3)  # Output layer for Longitude, Latitude, TimeNumeric
])

model.compile(optimizer='adam', loss='mse')

# Step 6: Train the Model
history = model.fit(X_train, y_train, epochs=50, batch_size=16, validation_data=(X_test, y_test), verbose=1)

# Step 7: Make Predictions
predictions = model.predict(X_test)

# Step 8: Inverse Transform Predictions and Actuals
predictions = scaler.inverse_transform(predictions)
y_test_actual = scaler.inverse_transform(y_test)

# Step 9: Plot Raw Data (Latitude and Longitude)
plt.figure(figsize=(10, 6))
plt.plot(df['Longitude'], df['Latitude'], marker='o', label='Raw Data', linestyle='-', color='blue')
plt.title("Raw Data: Latitude vs Longitude")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.legend()
plt.grid()
plt.show()

# Step 10: Plot Predicted vs Actual Latitude and Longitude
plt.figure(figsize=(10, 6))
plt.scatter(predictions[:, 0], predictions[:, 1], label='Predicted', color='orange', marker='x')
plt.scatter(y_test_actual[:, 0], y_test_actual[:, 1], label='Actual', color='green', marker='o')
plt.title("RNN LSTM Predicted vs Actual: Latitude vs Longitude")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.legend()
plt.grid()

plt.savefig(f'onca_93_RNN_LSTM.png')

plt.show()
