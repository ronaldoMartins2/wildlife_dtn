import pandas as pd
import matplotlib.pyplot as plt
import sys
import numpy as np
import os
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

    # Dicionário para armazenar os dados carregados e seus metadados
    cluster_data = {
        'K-Means': {'file': kmeans_data_file, 'data': None, 'color': 'blue'},
        'SOM': {'file': som_data_file, 'data': None, 'color': 'green'},
        'BIRCH': {'file': birch_data_file, 'data': None, 'color': 'red'},
        'Mean-Shift': {'file': mean_shift_data_file, 'data': None, 'color': 'purple'}
    }

    # Tentar carregar os dados para cada algoritmo
    for name, info in cluster_data.items():
        file_path = info['file']
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            try:
                df = pd.read_csv(file_path, header=None)
                # K-Means e SOM salvam Longitude, Latitude, Label
                # BIRCH e Mean-Shift salvam Longitude, Latitude, Cluster
                if df.shape[1] >= 2:
                    df.columns = ['longitude', 'latitude', 'label'][:df.shape[1]]
                    info['data'] = df
                else:
                    print(f"Warning: File '{file_path}' for {name} has fewer than 2 columns. Skipping.")
            except pd.errors.EmptyDataError:
                print(f"Warning: File '{file_path}' for {name} is empty. Skipping.")
        else:
            print(f"Warning: File '{file_path}' for {name} not found or is empty. Skipping.")

    # Verificar se algum dado foi carregado
    if not any(info['data'] is not None for info in cluster_data.values()):
        print(f"No cluster data found for animal {current_animal}. Skipping plot generation.")
        return

    # Criar o gráfico
    plt.figure(figsize=(10, 6))

    # Plotar os pontos para cada algoritmo que teve dados carregados
    for name, info in cluster_data.items():
        if info['data'] is not None:
            df = info['data']
            # Usar a coluna 'label' para colorir os pontos, se existir
            if 'label' in df.columns:
                plt.scatter(df['longitude'], df['latitude'], c=df['label'], label=f'{name} Clusters', alpha=0.6, cmap='viridis')
            else:
                # Caso contrário, use uma cor única para o algoritmo
                plt.scatter(df['longitude'], df['latitude'], color=info['color'], label=f'{name} Points', alpha=0.6)

    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title(f"Clusters e Centróides da Onça {current_animal}")
    plt.legend()
    plt.grid()

    create_clusterization_results('Results/Clusterization')
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results/Clusterization')  # Navigate to the parent directory and into 'Results'
    file_name = os.path.join(results_dir,f'onca_{current_animal}_clusterization_comparizon.png')

    # Save the plot as an image
    plt.savefig(file_name)
    # Fechar a figura para liberar memória
    plt.close()
    print(f"Plot saved to {file_name}")

def run_mock():
    current_animal = sys.argv [1]

    run( current_animal )