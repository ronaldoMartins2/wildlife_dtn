import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import json
from Common.utils import (
    create_clusterization_results
)

# plot_kmeans_som_birch_mean_shift 

# python3 -m venv venv
# source ./venv/bin/activate

# python3 plot_kmeans_som_birch_mean_shift.py 94 

def run(current_animal):

    create_clusterization_results('Results/Clusterization')
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results/Clusterization')  # Navigate to the parent directory and into 'Results'

    kmeans_data_file = os.path.join(results_dir, f"clusters_kmeans_{current_animal}.csv")
    som_data_file = os.path.join(results_dir, f"clusters_som_{current_animal}.csv")
    birch_data_file = os.path.join(results_dir, f"clusters_birch_map_{current_animal}.csv")
    mean_shift_data_file = os.path.join(results_dir, f"clusters_mean_shift_map_{current_animal}.csv")

    # Carregar os dados sem cabeçalho
    kmeans_data = pd.read_csv(kmeans_data_file, header=None)
    som_data = pd.read_csv(som_data_file, header=None)
    birch = pd.read_csv(birch_data_file, header=None)
    mean_shift = pd.read_csv(mean_shift_data_file, header=None)

    # Definir nomes das colunas dinamicamente
    if kmeans_data.shape[1] == 2:
        kmeans_data.columns = ['latitude', 'longitude']
    elif kmeans_data.shape[1] == 3:
        kmeans_data.columns = ['latitude', 'longitude', 'label']

    if som_data.shape[1] == 2:
        som_data.columns = ['latitude', 'longitude']
    elif som_data.shape[1] == 3:
        som_data.columns = ['latitude', 'longitude', 'label']

    if birch.shape[1] == 2:
        birch.columns = ['latitude', 'longitude']
    elif birch.shape[1] == 3:
        birch.columns = ['latitude', 'longitude', 'label']

    if mean_shift.shape[1] == 2:
        mean_shift.columns = ['latitude', 'longitude']
    elif mean_shift.shape[1] == 3:
        mean_shift.columns = ['latitude', 'longitude', 'label']

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

    # Criar o gráfico
    plt.figure(figsize=(10, 6))

    # Plotar os pontos originais
    plt.scatter(kmeans_data['longitude'], kmeans_data['latitude'], label='K-Means Clusters', alpha=0.6, cmap='viridis')
    plt.scatter(som_data['longitude'], som_data['latitude'], label='SOM Clusters', alpha=0.6, cmap='coolwarm')
    plt.scatter(birch['longitude'], birch['latitude'], label='BIRCH Clusters', alpha=0.6, cmap='plasma')
    plt.scatter(mean_shift['longitude'], mean_shift['latitude'], label='MEAN-SHIFT Clusters', alpha=0.6, cmap='cividis')

    plt.xlabel(lang["xlabel_kmeans_individual"])
    plt.ylabel(lang["ylabel_kmeans_individual"])
    
    if language == 'PT_BR':
        plt.title(f"{lang['grafico_kmeans_individual']} da onça {current_animal}")
    else:
        plt.title(f"{lang['grafico_kmeans_individual']} of the jaguar {current_animal}")
    
    #plt.title(f"{lang['grafico_kmeans_individual']} da onça {current_animal}")

    plt.legend()
    plt.grid()

    create_clusterization_results('Results/Clusterization')
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results/Clusterization')  # Navigate to the parent directory and into 'Results'
    file_name = os.path.join(results_dir,f'onca_{current_animal}_clusterization_comparizon.png')

    # Save the plot as an image
    plt.savefig(file_name)

def run_mock():
    current_animal = sys.argv [1]

    run( current_animal )