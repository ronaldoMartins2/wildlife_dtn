import numpy as np
import pandas as pd
import sys
import matplotlib.pyplot as plt
from minisom import MiniSom  # Import MiniSom for SOM

# pip install minisom

current_animal = sys.argv[1]

# Read data from CSV
file_name = f'../map_{current_animal}.csv'
data = pd.read_csv(file_name, header=None)

# Remove commas from the longitude and latitude columns (columns 2 and 3)
data.iloc[:, 2] = data.iloc[:, 2].replace({',': ''}, regex=True)
data.iloc[:, 3] = data.iloc[:, 3].replace({',': ''}, regex=True)

# Convert columns to numeric (float)
data.iloc[:, 2] = pd.to_numeric(data.iloc[:, 2], errors='coerce')
data.iloc[:, 3] = pd.to_numeric(data.iloc[:, 3], errors='coerce')

# Remove invalid coordinates
data_cleaned = data.dropna(subset=[2, 3])
data_cleaned = data_cleaned[(data_cleaned.iloc[:, 2] != 0) & (data_cleaned.iloc[:, 3] != 0)]
data_cleaned = data_cleaned[
    (data_cleaned.iloc[:, 2] >= -180) & (data_cleaned.iloc[:, 2] <= 180) &
    (data_cleaned.iloc[:, 3] >= -90) & (data_cleaned.iloc[:, 3] <= 90)
]

# Extract longitude and latitude
data_selected = data_cleaned.iloc[100:108, [2, 3]]
coords = data_selected.values

# Save cleaned coordinates to CSV
data_selected.to_csv(f'som_coords_{current_animal}.csv', index=False, header=['Longitude', 'Latitude'])

print("Coordinates saved to coords_processed.csv")

# Check if coords has valid data
if coords.shape[0] == 0:
    print("Error: No valid coordinates left for clustering.")
    sys.exit(1)

# SOM Parameters
som_x, som_y = 8, 8
som = MiniSom(som_x, som_y, coords.shape[1], sigma=0.5, learning_rate=0.5)
som.random_weights_init(coords)
som.train_random(coords, 500)

# Get cluster assignments
cluster_map = {i: som.winner(coord) for i, coord in enumerate(coords)}
clusters = np.array([cluster_map[i][0] for i in range(len(coords))])

# Plotting
plt.figure(figsize=(10, 6))
for cluster_id in np.unique(clusters):
    cluster_points = coords[clusters == cluster_id]
    plt.scatter(cluster_points[:, 0], cluster_points[:, 1], label=f'Centroíde {cluster_id}', alpha=0.7)

plt.title('SOM Clustering of GPS Coordinates')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.legend()
plt.grid(True)
plt.savefig(f'onca_{current_animal}_som.png')
plt.show()