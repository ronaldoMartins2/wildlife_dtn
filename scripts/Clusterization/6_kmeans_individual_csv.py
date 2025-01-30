import numpy as np
import pandas as pd
import sys
import re
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

# python3 6_kmeans_individual_csv.py 94

current_animal = sys.argv[1]

# Read data from CSV
file_name = f'../map_{current_animal}.csv'
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

# Print the data after conversion to check for NaN values
print("Data after conversion:")
print(data.head())

# Remove rows where longitude or latitude are NaN or have zero coordinates
data_cleaned = data.dropna(subset=[2, 3])  # Drop rows with NaN in longitude or latitude
data_cleaned = data_cleaned[(data_cleaned.iloc[:, 2] != 0) & (data_cleaned.iloc[:, 3] != 0)]  # Remove rows with zero coordinates

# Remove extreme values for longitude and latitude
# Longitude should be between -180 and 180, and latitude between -90 and 90
data_cleaned = data_cleaned[
    (data_cleaned.iloc[:, 2] >= -180) & (data_cleaned.iloc[:, 2] <= 180) &  # Filter valid longitude
    (data_cleaned.iloc[:, 3] >= -90) & (data_cleaned.iloc[:, 3] <= 90)     # Filter valid latitude
]

# Print the number of rows after cleaning
print(f"Number of rows after cleaning: {len(data_cleaned)}")

# Print cleaned data preview to verify the removal of extreme values
print("Cleaned data preview (no zeros or extreme values in longitude or latitude):")
print(data_cleaned.head())

# Extract longitude and latitude after cleaning
coords = data_cleaned.iloc[:, [2, 3]].values

# Print extracted coordinates
print("Extracted Coordinates:")
print(coords)

# Check if coords has valid data
if coords.shape[0] == 0:
    print("Error: No valid coordinates left for clustering.")
    sys.exit(1)  # Exit if no valid data is available

# Apply KMeans (after ensuring valid coordinates are present)
kmeans = KMeans(n_clusters=8, random_state=0)
kmeans.fit(coords)

# Get cluster labels and centroids
clusters = kmeans.labels_
centroids = kmeans.cluster_centers_

# Plotting
plt.figure(figsize=(10, 6))

# Scatter plot of data points colored by cluster
for cluster_id in np.unique(clusters):
    cluster_points = coords[clusters == cluster_id]
    plt.scatter(
        cluster_points[:, 0], 
        cluster_points[:, 1], 
        label=f'Cluster {cluster_id}', 
        alpha=0.7
    )

# Plot centroids
plt.scatter(
    centroids[:, 0], 
    centroids[:, 1], 
    color='red', 
    marker='x', 
    s=100, 
    label='Centroids'
)

# Add labels and legend
plt.title('K-Means Clustering of GPS Coordinates')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.legend()
plt.grid(True)

# Save the plot as an image
plt.savefig(f'onca_{current_animal}_kmeans.png')
plt.show()
