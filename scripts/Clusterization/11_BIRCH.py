import numpy as np
import pandas as pd
from sklearn.cluster import Birch
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import sys

# Step 1: Load Data
raw_data = pd.read_csv(f'../Data_preparation/map_{sys.argv[1]}.csv', header=None, names=['id', 'Timestamp', 'Longitude', 'Latitude'])

data = raw_data[:100]

df = pd.DataFrame(data, columns=["ID", "Timestamp", "Longitude", "Latitude"])

# Step 2: Standardize Latitude and Longitude
scaler = StandardScaler()
coordinates = scaler.fit_transform(df[['Longitude', 'Latitude']])

# Step 3: Apply BIRCH Clustering
birch_model = Birch(n_clusters=None, threshold=0.5)
df['Cluster'] = birch_model.fit_predict(coordinates)

# Step 4: Save Results to CSV
output_csv_path = f'birch_clusters_map_{sys.argv[1]}.csv'
df[['Longitude', 'Latitude', 'Cluster']].to_csv(output_csv_path, index=False, header=None)
print(f"Clusters saved to {output_csv_path}")

# Step 5: Plot Clusters
plt.figure(figsize=(10, 6))

# Plot each cluster with a unique color
for cluster_id in np.unique(df['Cluster']):
    cluster_data = df[df['Cluster'] == cluster_id]
    plt.scatter(cluster_data['Longitude'], cluster_data['Latitude'], label=f"Cluster {cluster_id}")

plt.title("BIRCH Clustering: Latitude vs Longitude")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.legend()
plt.grid()

plt.savefig(f'onca_{sys.argv[1]}_BIRCH.png')
plt.show()

