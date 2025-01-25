import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

# Data preparation
data = [
    [93, '3/12/14 17:39', -64.877263, -3.03684],
    [93, '3/14/14 4:00', -64.877105, -3.038135],
    [93, '3/14/14 10:02', -64.876807, -3.038255],
    [93, '3/14/14 16:00', -64.877153, -3.03809],
    [93, '3/14/14 22:00', -64.877072, -3.03821],
    [93, '3/15/14 10:01', -64.885507, -3.033158],
    [93, '3/15/14 22:00', -64.887686, -3.031688],
    [93, '3/16/14 4:00', -64.887788, -3.031543],
    [93, '3/16/14 10:00', -64.888473, -3.031137],
    [93, '3/16/14 22:00', -64.888699, -3.031063],
    [93, '3/17/14 4:00', -64.888955, -3.03231],
    [93, '3/17/14 10:01', -64.886187, -3.033822],
    [93, '3/17/14 22:00', -64.886215, -3.033063],
    [93, '3/18/14 4:02', -64.879131, -3.038117],
]

# Extract latitude and longitude
coords = np.array([[row[2], row[3]] for row in data])

# Apply KMeans
kmeans = KMeans(n_clusters=3, random_state=0)
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
plt.savefig(f'onca_.png')

plt.show()
