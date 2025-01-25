import numpy as np
import pandas as pd
import sys
import re
import matplotlib.pyplot as plt
from minisom import MiniSom  # Import MiniSom for SOM

current_animal = sys.argv[1]

# Read data from CSV
file_name = f'map_{current_animal}.csv'
data = pd.read_csv(file_name, header=None)  # header=None to indicate no column names

# Print the first few rows to inspect the raw data
print("Raw data preview:")
print(data.head())  # Check if the data looks correct

# Remove commas from the longitude and latitude columns (columns 2 and 3)
data.iloc[:, 2] = data.iloc[:, 2].replace({',': ''}, regex=True)
data.iloc[:, 3] = data.iloc[:, 3].replace({',': ''}, regex=True)

# Convert columns to numeric (float), coercing errors (invalid values turn to NaN)
data.iloc[:, 2] = pd.to_numeric(data.iloc[:, 2], errors='coerce')
data.iloc[:, 3] = pd.to_numeric(data.iloc[:, 3], errors='coerce')

# Remove rows where longitude or latitude are NaN or have zero coordinates
data_cleaned = data.dropna(subset=[2, 3])  # Drop rows with NaN in longitude or latitude
data_cleaned = data_cleaned[(data_cleaned.iloc[:, 2] != 0) & (data_cleaned.iloc[:, 3] != 0)]  # Remove rows with zero coordinates

# Remove extreme values for longitude and latitude
data_cleaned = data_cleaned[
    (data_cleaned.iloc[:, 2] >= -180) & (data_cleaned.iloc[:, 2] <= 180) &  # Filter valid longitude
    (data_cleaned.iloc[:, 3] >= -90) & (data_cleaned.iloc[:, 3] <= 90)     # Filter valid latitude
]

# Print the number of rows after cleaning
print(f"Number of rows after cleaning: {len(data_cleaned)}")

# Extract longitude and latitude after cleaning
coords = data_cleaned.iloc[100:108, [2, 3]].values

# Print extracted coordinates
print("Extracted Coordinates:")
print(coords)

# Check if coords has valid data
if coords.shape[0] == 0:
    print("Error: No valid coordinates left for clustering.")
    sys.exit(1)  # Exit if no valid data is available

# SOM Parameters
som_x = 8  # Number of clusters horizontally
som_y = 8  # Number of clusters vertically
som = MiniSom(som_x, som_y, coords.shape[1], sigma=0.5, learning_rate=0.5)  # Initialize SOM
som.random_weights_init(coords)  # Initialize weights
som.train_random(coords, 500)  # Train SOM for 500 iterations

# Get cluster assignments
cluster_map = {}
for i, coord in enumerate(coords):
    winner = som.winner(coord)  # Find the winning node
    cluster_map[i] = winner

# Convert cluster assignments to a flat array for plotting
clusters = np.array([cluster_map[i][0] for i in range(len(coords))])  # Use the x-coordinate of the SOM node as cluster ID

# Plotting
plt.figure(figsize=(10, 6))

# Scatter plot of data points colored by SOM cluster
for cluster_id in np.unique(clusters):
    cluster_points = coords[clusters == cluster_id]
    plt.scatter(
        cluster_points[:, 0],
        cluster_points[:, 1],
        label=f'Cluster {cluster_id}',
        alpha=0.7
    )

# Add labels and legend
plt.title('SOM Clustering of GPS Coordinates')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.legend()
plt.grid(True)

# Save the plot as an image
plt.savefig(f'onca_{current_animal}_som.png')
plt.show()
