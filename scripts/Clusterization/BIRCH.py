import numpy as np
import pandas as pd
from sklearn.cluster import Birch
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import os
from Common.utils import (
    create_clusterization_results
)

# exemplode execução
# python3 11_BIRCH.py 93


def run(current_animal):

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results')  # Navigate to the parent directory and into 'Results'
    file_name = os.path.join(results_dir, f'map_{current_animal}.csv')

    # Step 1: Load Data
    raw_data = pd.read_csv(file_name, header=None, names=['id', 'Timestamp', 'Longitude', 'Latitude'])

    data = raw_data[:100]

    df = pd.DataFrame(data, columns=["ID", "Timestamp", "Longitude", "Latitude"])

    # Step 2: Standardize Latitude and Longitude
    scaler = StandardScaler()
    coordinates = scaler.fit_transform(df[['Longitude', 'Latitude']])

    # Step 3: Apply BIRCH Clustering
    birch_model = Birch(n_clusters=None, threshold=0.5)
    df['Cluster'] = birch_model.fit_predict(coordinates)

    # Step 4: Save Results to CSV
    output_csv_path = f'birch_clusters_map_{current_animal}.csv'
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


    create_clusterization_results('Results/Clusterization')
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results/Clusterization')  # Navigate to the parent directory and into 'Results'
    file_name = os.path.join(results_dir, f'onca_{current_animal}_BIRCH.png')

    plt.savefig(file_name)
#    plt.show()


def run_mock():
    current_animal = sys.argv [1]

    run( current_animal )
