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
    # --- CAMINHO DE ENTRADA (para ler os dados) ---
    input_results_dir = results_folder(file_rawdata_name)
    input_file_path = os.path.join(input_results_dir, f'map_{current_animal}.csv')

    # --- CAMINHO DE SAÍDA (para salvar os resultados) ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_main_dir = os.path.join(script_dir, '..', 'Results')
    cluster_output_dir = os.path.join(output_main_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}")
        return

    # Lê e limpa os dados
    data = pd.read_csv(input_file_path, header=None)
    data.iloc[:, 2] = data.iloc[:, 2].replace({',': ''}, regex=True)
    data.iloc[:, 3] = data.iloc[:, 3].replace({',': ''}, regex=True)
    data.iloc[:, 2] = pd.to_numeric(data.iloc[:, 2], errors='coerce')
    data.iloc[:, 3] = pd.to_numeric(data.iloc[:, 3], errors='coerce')

    data_cleaned = data.dropna(subset=[2, 3])
    data_cleaned = data_cleaned[(data_cleaned.iloc[:, 2] != 0) & (data_cleaned.iloc[:, 3] != 0)]
    data_cleaned = data_cleaned[
        (data_cleaned.iloc[:, 2] >= -180) & (data_cleaned.iloc[:, 2] <= 180) &
        (data_cleaned.iloc[:, 3] >= -90) & (data_cleaned.iloc[:, 3] <= 90)
    ]
    
    # Este script usa um subconjunto muito específico de dados
    if len(data_cleaned) < 108:
        print(f"Warning for animal {current_animal}: Not enough data for SOM slicing (needs at least 108 rows, found {len(data_cleaned)}). Skipping.")
        return

    data_selected = data_cleaned.iloc[100:108, [2, 3]]
    coords = data_selected.values

    if coords.shape[0] == 0:
        print(f"Error for animal {current_animal}: No valid coordinates left for clustering after slicing.")
        return

    # Salva as coordenadas usadas no diretório de saída correto
    output_csv_path = os.path.join(cluster_output_dir, f'clusters_som_{current_animal}.csv')
    data_selected.to_csv(output_csv_path, index=False, header=None)
    print(f"Coordinates saved to {output_csv_path}")

    # Carrega hiperparâmetros
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    sigma = read_field_from_json(hyperparam_path, "sigma_SOM")
    learning_rate = read_field_from_json(hyperparam_path, "learning_rate_SOM")
    ephocs = read_field_from_json(hyperparam_path, "ephocs_SOM")

    som = MiniSom(8, 8, coords.shape[1], sigma, learning_rate) # 64 clusters máximos
    #som = MiniSom(8, 4, coords.shape[1], sigma, learning_rate)  # 8x4 = 32 neurônios
    #som = MiniSom(4, 4, coords.shape[1], sigma, learning_rate)  # 16 clusters máximos
    #som = MiniSom(4, 2, coords.shape[1], sigma, learning_rate)  # 8 neurônios (clusters máx.)

    som.random_weights_init(coords)
    som.train_random(coords, ephocs)
    
    hiper_content = [
        f"Hyper SOM sigma {sigma}",
        f"Hyper SOM learning_rate {learning_rate}",
        f"Hyper SOM ephocs {ephocs}"
    ]
    hiper_path = os.path.join(output_main_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    cluster_map = {i: som.winner(coord) for i, coord in enumerate(coords)}
    clusters = np.array([cluster_map[i][0] for i in range(len(coords))])

    json_path = f'scripts/Data_preparation/language_{read_field_from_json(hyperparam_path, "language")}.json'
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Plota e salva o gráfico
    plt.figure(figsize=(10, 6))
    for cluster_id in np.unique(clusters):
        cluster_points = coords[clusters == cluster_id]
        plt.scatter(cluster_points[:, 0], cluster_points[:, 1], label=f'Centroíde {cluster_id}', alpha=0.7)
    
    plt.title(lang["grafico_SOM_individual"])
    plt.xlabel(lang["xlabel_SOM_individual"])
    plt.ylabel(lang["ylabel_SOM_individual"])
    plt.legend()
    plt.grid(True)

    output_png_path = os.path.join(cluster_output_dir, f'onca_{current_animal}_som.png')
    plt.savefig(output_png_path)
    plt.close() # Fecha a figura
    print(f"Plot saved to {output_png_path}")

def run_mock():
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    run(current_animal, file_rawdata_name)