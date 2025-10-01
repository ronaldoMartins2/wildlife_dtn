import numpy as np
import pandas as pd
import sys
import re
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import os
import json
from Common.utils import (
    create_clusterization_results,
    results_folder,
    read_field_from_json
)
# pip install scikit-learn

# python3 6_kmeans_individual_csv.py 94

def run(current_animal, file_rawdata_name):

    results_dir = results_folder(file_rawdata_name)

    # === ADICIONE ESTA LINHA PARA DEFINIR A PASTA FINAL ===
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
    # Garante que a pasta Clusterization exista
    create_clusterization_results(cluster_output_dir)

    file_name = os.path.join(results_dir, f'map_{current_animal}.csv')

    # Read data from CSV
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

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')  # Navigate to the parent directory and into 'Results'

    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    n_clusters = read_field_from_json(hyperparam_path, "n_clusters_kmeans")
    random_state = read_field_from_json(hyperparam_path, "random_state_kmeans")
    n_init = read_field_from_json(hyperparam_path, "n_init_kmeans")

    # Adjust n_clusters if it's larger than the number of samples
    if n_clusters > coords.shape[0]:
        n_clusters = coords.shape[0]

    # Apply KMeans
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=n_init)
    kmeans.fit(coords)

    hiper_content = []
    hiper_content.append( f"Hyper kmeans n_clusters {n_clusters}" )
    hiper_content.append( f"Hyper kmeans random_state {random_state}" )
    hiper_content.append( f"Hyper kmeans n_init {n_init}" )
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    # Get cluster labels and centroids
    clusters = kmeans.labels_
    centroids = kmeans.cluster_centers_

    # Save cluster coordinates to CSV
    create_clusterization_results(f'{results_dir}/Clusterization/')
    
    output_file = os.path.join(cluster_output_dir, f'clusters_kmeans_{current_animal}.csv')
    #output_file = os.path.join(results_dir, f'clusters_kmeans_{current_animal}.csv')

    cluster_data = pd.DataFrame(centroids, columns=['Longitude', 'Latitude'])
    cluster_data.to_csv(output_file, index=False, header=None)
    print(f"Cluster centroids saved to {output_file}")

    # === JSON PARA LINGUAGEM ===
    json_language = 'scripts/Data_preparation/hyperparameters.json'
    with open(json_language, encoding='utf-8') as f:
        lang_params = json.load(f)
        language = lang_params["language"]

    if language == 'PT_BR':
        json_path = 'scripts/Data_preparation/language_PT_BR.json'
    else:
        json_path = 'scripts/Data_preparation/language_US_US.json'

    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

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
    plt.title(lang["grafico_kmeans_individual"])
    plt.xlabel(lang["xlabel_kmeans_individual"])
    plt.ylabel(lang["ylabel_kmeans_individual"])
    plt.legend()
    plt.grid(True)

    # Save the plot as an image

    create_clusterization_results(f'{results_dir}/Clusterization/')
    #script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    #results_dir = os.path.join(script_dir, '..', 'Results/Clusterization')  # Navigate to the parent directory and into 'Results'

    #file_name = os.path.join(results_dir, f'onca_{current_animal}_kmeans.png')
    file_name = os.path.join(cluster_output_dir, f'onca_{current_animal}_kmeans.png')
    #plt.savefig(f'onca_{current_animal}_kmeans.png')
    plt.savefig(file_name)

def run_mock():
    current_animal = sys.argv [1]

    run( current_animal )