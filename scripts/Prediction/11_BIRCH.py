import numpy as np
import pandas as pd
from sklearn.cluster import Birch
from sklearn.preprocessing import StandardScaler
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

# Step 2: Standardize Latitude and Longitude
scaler = StandardScaler()
coordinates = scaler.fit_transform(df[['Longitude', 'Latitude']])

# Step 3: Apply BIRCH Clustering
birch_model = Birch(n_clusters=None, threshold=0.5)
df['Cluster'] = birch_model.fit_predict(coordinates)

# Step 4: Plot Clusters
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

plt.savefig(f'onca_93_BIRCH.png')

plt.show()
