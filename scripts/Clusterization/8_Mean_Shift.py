import pandas as pd
import numpy as np
from sklearn.cluster import MeanShift
import matplotlib.pyplot as plt
import sys

# exemplo de execução
# python3 8_Mean_Shift.py 93

# Step 1: Load Data from Data_preparation folder

data = pd.read_csv(f'../Data_preparation/map_{sys.argv[1]}.csv', header=None, names=['id', 'Timestamp', 'Longitude', 'Latitude'])
data = data[:100]
df = pd.DataFrame(data, columns=["ID", "Timestamp", "Longitude", "Latitude"])

# Convert Timestamp to numeric (optional, for time-based clustering)
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

df['TimeNumeric'] = (df['Timestamp'] - df['Timestamp'].min()).dt.total_seconds()

# Step 2: Prepare Data for Clustering (Longitude, Latitude)
coordinates = df[['Longitude', 'Latitude']].values

# Step 3: Apply Mean-Shift
mean_shift = MeanShift(bandwidth=0.001)  # Adjust bandwidth as necessary
mean_shift.fit(coordinates)
df['Cluster'] = mean_shift.labels_


# Step 4: Save Results to CSV
output_csv_path = f'clusters_mean_shift_map_{sys.argv[1]}.csv'
df[['Longitude', 'Latitude', 'Cluster']].to_csv(output_csv_path, index=False, header=None)
print(f"Clusters saved to {output_csv_path}")

# Step 5: Visualize Clusters
plt.figure(figsize=(8, 6))
for cluster in np.unique(df['Cluster']):
    cluster_points = df[df['Cluster'] == cluster]
    plt.scatter(cluster_points['Longitude'], cluster_points['Latitude'], label=f"Cluster {cluster}")

plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('Mean-Shift Clustering of Latitude and Longitude')
plt.legend()

# salvar conforme id da onça
plt.savefig(f'onca_{sys.argv[1]}_mean_shift.png')

#plt.show()
