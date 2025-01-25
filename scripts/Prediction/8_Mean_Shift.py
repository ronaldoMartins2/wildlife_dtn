import pandas as pd
import numpy as np
from sklearn.cluster import MeanShift
import matplotlib.pyplot as plt

# Step 1: Load Data
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

df = pd.DataFrame(data, columns=["ID", "Timestamp", "Longitude", "Latitude"])

# Convert Timestamp to numeric (optional, for time-based clustering)
df['Timestamp'] = pd.to_datetime(df['Timestamp'])
df['TimeNumeric'] = (df['Timestamp'] - df['Timestamp'].min()).dt.total_seconds()

# Step 2: Prepare Data for Clustering (Longitude, Latitude)
coordinates = df[['Longitude', 'Latitude']].values

# Step 3: Apply Mean-Shift
mean_shift = MeanShift(bandwidth=0.001)  # Adjust bandwidth as necessary
mean_shift.fit(coordinates)
df['Cluster'] = mean_shift.labels_

# Step 4: Visualize Clusters
plt.figure(figsize=(8, 6))
for cluster in np.unique(df['Cluster']):
    cluster_points = df[df['Cluster'] == cluster]
    plt.scatter(cluster_points['Longitude'], cluster_points['Latitude'], label=f"Cluster {cluster}")

plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('Mean-Shift Clustering of Latitude and Longitude')
plt.legend()

plt.savefig(f'onca_93_mean_shift.png')


plt.show()