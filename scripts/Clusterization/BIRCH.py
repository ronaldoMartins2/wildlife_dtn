import numpy as np
import pandas as pd
from sklearn.cluster import Birch
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import os
from Common.utils import (
    create_clusterization_results,
    results_folder,
    read_field_from_json
)

# exemplode execução
# python3 11_BIRCH.py 93


def run(current_animal, file_rawdata_name):

    results_dir = results_folder(file_rawdata_name)
    
    file_name = os.path.join(results_dir, f'map_{current_animal}.csv')

    # Step 1: Load Data
    raw_data = pd.read_csv(file_name, header=None, names=['id', 'Timestamp', 'Longitude', 'Latitude'])

    data = raw_data[:100]

    df = pd.DataFrame(data, columns=["ID", "Timestamp", "Longitude", "Latitude"])

    # Step 2: Standardize Latitude and Longitude
    scaler = StandardScaler()
    coordinates = scaler.fit_transform(df[['Longitude', 'Latitude']])

    # Check if there are enough samples for clustering
    if coordinates.shape[0] < 2:
        print(f"Warning: Not enough data points ({coordinates.shape[0]}) for BIRCH clustering for animal {current_animal}. Skipping.")
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')  # Navigate to the parent directory and into 'Results'
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    threshold = read_field_from_json(hyperparam_path, "threshold")    

    # Step 3: Apply BIRCH Clustering
    birch_model = Birch(n_clusters=None, threshold=threshold)
    df['Cluster'] = birch_model.fit_predict(coordinates)

    # Step 4: Save Results to CSV

    create_clusterization_results('Results/Clusterization')

    output_csv_path = os.path.join(results_dir, f'clusters_birch_map_{current_animal}.csv')

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

    file_name = os.path.join(results_dir, f'onca_{current_animal}_BIRCH.png')

    hiper_content = []
    hiper_content.append( f"Hyper BIRCH threshold {threshold}" )
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    plt.savefig(file_name)

def run_mock():
    current_animal = sys.argv [1]
    file_rawdata_name = sys.argv [2]
    
    run( current_animal, file_rawdata_name )
