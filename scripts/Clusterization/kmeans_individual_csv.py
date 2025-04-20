import numpy as np
import pandas as pd
import sys
import re
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import os
from Common.utils import (
    create_clusterization_results
)
# pip install scikit-learn

# python3 6_kmeans_individual_csv.py 94



def run(current_animal):

    #current_animal = sys.argv[1]

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results')  # Navigate to the parent directory and into 'Results'
    file_name = os.path.join(results_dir, f'map_{current_animal}.csv')

    # Read data from CSV
    #file_name = f'../Data_preparation/map_{current_animal}.csv'
    data = pd.read_csv(file_name, header=None)  # header=None to indicate no column names

    # Remove commas from the longitude and latitude columns (columns 2 and 3)
    data.iloc[:, 2] = data.iloc[:, 2].replace({',': ''}, regex=True)
    data.iloc[:, 3] = data.iloc[:, 3].replace({',': ''}, regex=True)

    # Convert columns to numeric (float), coercing errors (invalid values turn to NaN)
    data.iloc[:, 2] = pd.to_numeric(data.iloc[:, 2], errors='coerce')
    data.iloc[:, 3] = pd.to_numeric(data.iloc[:, 3], errors='coerce')

    # Remove rows where longitude or latitude are NaN or have zero coordinates
    data_cleaned = data.dropna(subset=[2, 3])
    data_cleaned = data_cleaned[(data_cleaned.iloc[:, 2] != 0) & (data_cleaned.iloc[:, 3] != 0)]

    # Remove extreme values for longitude and latitude
    data_cleaned = data_cleaned[
        (data_cleaned.iloc[:, 2] >= -180) & (data_cleaned.iloc[:, 2] <= 180) &
        (data_cleaned.iloc[:, 3] >= -90) & (data_cleaned.iloc[:, 3] <= 90)
    ]

    # Extract longitude and latitude after cleaning
    coords = data_cleaned.iloc[:, [2, 3]].values

    # Check if coords has valid data
    if coords.shape[0] == 0:
        print("Error: No valid coordinates left for clustering.")
        sys.exit(1)

    # Apply KMeans
    kmeans = KMeans(n_clusters=8, random_state=0, n_init=10)
    kmeans.fit(coords)

    # Get cluster labels and centroids
    clusters = kmeans.labels_
    centroids = kmeans.cluster_centers_

    # Save cluster coordinates to CSV
    create_clusterization_results('Results/Clusterization')
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results/Clusterization')  # Navigate to the parent directory and into 'Results'
    output_file = os.path.join(results_dir, f'clusters_kmeans_{current_animal}.csv')

    cluster_data = pd.DataFrame(centroids, columns=['Longitude', 'Latitude'])
    cluster_data.to_csv(output_file, index=False, header=None)
    print(f"Cluster centroids saved to {output_file}")

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


    create_clusterization_results('Results/Clusterization')
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results/Clusterization')  # Navigate to the parent directory and into 'Results'
    file_name = os.path.join(results_dir, f'onca_{current_animal}_kmeans.png')
    #plt.savefig(f'onca_{current_animal}_kmeans.png')
    plt.savefig(file_name)
    #plt.show()

def run_mock():
    current_animal = sys.argv [1]

    run( current_animal )