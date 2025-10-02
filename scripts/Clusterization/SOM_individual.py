import numpy as np
import pandas as pd
import sys
import matplotlib.pyplot as plt
import json
from minisom import MiniSom  # Import MiniSom for SOM
import os
from Common.utils import (
    create_clusterization_results,
    results_folder,
    read_field_from_json
)

# pip install minisom

# python3 -m venv venv
# source ./venv/bin/activate

# python3 7_SOM_individual.py 94

def run(current_animal, file_rawdata_name):

    results_dir = results_folder(file_rawdata_name)

    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    # TODO check if will use rawdata or interpolated data
    file_name = os.path.join(results_dir, f'map_{current_animal}.csv')

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
    # Use all cleaned data instead of a fixed slice
    coords = data_cleaned.iloc[:, [2, 3]].values

    # Save cleaned coordinates to CSV
    #create_clusterization_results('Results/Clusterization')
    
    file_name = os.path.join(results_dir, f'clusters_som_{current_animal}.csv')

    pd.DataFrame(coords).to_csv(file_name, index=False, header=None)

    output_csv_path = os.path.join(cluster_output_dir, f'clusters_som_{current_animal}.csv')
    data_selected.to_csv(output_csv_path, index=False, header=None)
    print(f"Coordinates saved to {output_csv_path}")

    # Check if coords has valid data
    if coords.shape[0] == 0:
        print("Error: No valid coordinates left for clustering.")
        return  # Troquei sys.exit(1) por return
        #sys.exit(1)
    

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')  # Navigate to the parent directory and into 'Results'

    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    sigma = read_field_from_json(hyperparam_path, "sigma_SOM")
    learning_rate = read_field_from_json(hyperparam_path, "learning_rate_SOM")
    ephocs = read_field_from_json(hyperparam_path, "ephocs_SOM")

    # SOM Parameters
    som_x, som_y = 8, 8
    som = MiniSom(som_x, som_y, coords.shape[1], sigma, learning_rate)
    som.random_weights_init(coords)
    som.train_random(coords, ephocs)
    
    hiper_content = []
    hiper_content.append( f"Hyper SOM sigma {sigma}" )
    hiper_content.append( f"Hyper SOM learning_rate {learning_rate}" )
    hiper_content.append( f"Hyper SOM ephocs {ephocs}" )
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')


    # Get cluster assignments
    cluster_map = {i: som.winner(coord) for i, coord in enumerate(coords)}
    clusters = np.array([cluster_map[i][0] for i in range(len(coords))])

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
    for cluster_id in np.unique(clusters):
        cluster_points = coords[clusters == cluster_id]

        plt.scatter(
            cluster_points[:, 0],
            cluster_points[:, 1],
            label=f'Centroíde {cluster_id}',
            alpha=0.7
        )

    plt.title(lang["grafico_SOM_individual"])
    plt.xlabel(lang["xlabel_SOM_individual"])
    plt.ylabel(lang["ylabel_SOM_individual"])
    plt.legend()
    plt.grid(True)

    create_clusterization_results('Results/Clusterization')

    #file_name = os.path.join(results_dir, f'onca_{current_animal}_som.png')
    #plt.savefig(file_name)
    
    output_png_path = os.path.join(cluster_output_dir, f'onca_{current_animal}_som.png')
    plt.savefig(output_png_path)

def run_mock():
    current_animal = sys.argv [1]

    run( current_animal )