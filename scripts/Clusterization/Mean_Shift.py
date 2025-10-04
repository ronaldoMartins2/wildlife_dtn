import pandas as pd
import numpy as np
from sklearn.cluster import MeanShift
import matplotlib.pyplot as plt
import sys
import os
import json 
from Common.utils import (
    create_clusterization_results,
    results_folder,
    read_field_from_json
)

# exemplo de execução
# python3 8_Mean_Shift.py 93

# Step 1: Load Data from Data_preparation folder

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

    data = pd.read_csv(input_file_path, header=None, names=['id', 'Timestamp', 'Longitude', 'Latitude'])
    
    if data.empty:
        print(f"Error for animal {current_animal}: Input file is empty.")
        return
        
    df = data[:100].copy()
    coordinates = df[['Longitude', 'Latitude']].values

    # Carrega hiperparâmetros
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    bandwidth = read_field_from_json(hyperparam_path, "bandwidth")

    mean_shift = MeanShift(bandwidth=bandwidth)
    mean_shift.fit(coordinates)
    df['Cluster'] = mean_shift.labels_

    # Salva resultados no diretório de saída correto
    output_csv_path = os.path.join(cluster_output_dir, f'clusters_mean_shift_map_{current_animal}.csv')
    df[['Longitude', 'Latitude', 'Cluster']].to_csv(output_csv_path, index=False, header=None)
    print(f"Clusters saved to {output_csv_path}")

    # Carrega idioma
    json_path = f'scripts/Data_preparation/language_{read_field_from_json(hyperparam_path, "language")}.json'
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Plota e salva o gráfico
    plt.figure(figsize=(8, 6))
    for cluster in np.unique(df['Cluster']):
        cluster_points = df[df['Cluster'] == cluster]
        plt.scatter(cluster_points['Longitude'], cluster_points['Latitude'], label=f"Cluster {cluster}")

    plt.xlabel(lang["xlabel_Mean_Shift"])
    plt.ylabel(lang["ylabel_Mean_Shift"])
    plt.title(lang["grafico_Mean_Shift"])
    plt.legend()
    plt.grid(True)

    output_png_path = os.path.join(cluster_output_dir, f'onca_{current_animal}_mean_shift.png')
    plt.savefig(output_png_path)
    plt.close() # Fecha a figura
    print(f"Plot saved to {output_png_path}")

    hiper_content = [f"Hyper Mean-Shift bandwidth {bandwidth}"]
    hiper_path = os.path.join(output_main_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        file.write(hiper_content[0] + '\n')

def run_mock():
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    run(current_animal, file_rawdata_name)