import numpy as np
import pandas as pd
import sys
import matplotlib.pyplot as plt
from minisom import MiniSom  # Import MiniSom for SOM
import os
from Common.utils import (
    create_clusterization_results
)

# pip install minisom

# python3 -m venv venv
# source ./venv/bin/activate

# python3 7_SOM_individual.py 94

def run(current_animal):

    # current_animal = sys.argv[1]

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results')  # Navigate to the parent directory and into 'Results'
    file_name = os.path.join(results_dir, f'map_{current_animal}.csv')

    # Read data from CSV
    #file_name = f'../Data_preparation/map_{current_animal}.csv'

    data = pd.read_csv(file_name, header=None)  # header=None to indicate no column names

    # Print the first few rows to inspect the raw data
    print("Raw data preview:")
    print(data.head())  # Check if the data looks correct


    # Remove commas from the longitude and latitude columns (columns 2 and 3)
    data.iloc[:, 2] = data.iloc[:, 2].replace({',': ''}, regex=True)
    data.iloc[:, 3] = data.iloc[:, 3].replace({',': ''}, regex=True)

    # Convert columns to numeric (float)
    data.iloc[:, 2] = pd.to_numeric(data.iloc[:, 2], errors='coerce')
    data.iloc[:, 3] = pd.to_numeric(data.iloc[:, 3], errors='coerce')

    # Remove invalid coordinates
    data_cleaned = data.dropna(subset=[2, 3])
    data_cleaned = data_cleaned[(data_cleaned.iloc[:, 2] != 0) & (data_cleaned.iloc[:, 3] != 0)]
    data_cleaned = data_cleaned[
        (data_cleaned.iloc[:, 2] >= -180) & (data_cleaned.iloc[:, 2] <= 180) &
        (data_cleaned.iloc[:, 3] >= -90) & (data_cleaned.iloc[:, 3] <= 90)
    ]

    # Extract longitude and latitude
    data_selected = data_cleaned.iloc[100:108, [2, 3]]
    coords = data_selected.values

    # Save cleaned coordinates to CSV
    data_selected.to_csv(f'som_coords_{current_animal}.csv', index=False, header=None)

    print("Coordinates saved to coords_processed.csv")

    # Check if coords has valid data
    if coords.shape[0] == 0:
        print("Error: No valid coordinates left for clustering.")
        sys.exit(1)

    # SOM Parameters
    som_x, som_y = 8, 8
    som = MiniSom(som_x, som_y, coords.shape[1], sigma=0.5, learning_rate=0.5)
    som.random_weights_init(coords)
    som.train_random(coords, 500)

    # Get cluster assignments
    cluster_map = {i: som.winner(coord) for i, coord in enumerate(coords)}
    clusters = np.array([cluster_map[i][0] for i in range(len(coords))])

    # Plotting
    plt.figure(figsize=(10, 6))
    for cluster_id in np.unique(clusters):
        cluster_points = coords[clusters == cluster_id]

        plt.scatter(
            cluster_points[:, 0],
            cluster_points[:, 1],
            label=f'Centroíde {cluster_id}',
            alpha=0.7
        )

    plt.title('SOM Clustering of GPS Coordinates')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.legend()
    plt.grid(True)

    create_clusterization_results('Results/Clusterization')
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results/Clusterization')  # Navigate to the parent directory and into 'Results'
    file_name = os.path.join(results_dir, f'onca_{current_animal}_som.png')

    plt.savefig(file_name)
    #plt.show()

def run_mock():
    current_animal = sys.argv [1]

    run( current_animal )