import numpy as np
import pandas as pd
from sklearn.cluster import Birch
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import os
import json
import sys

from Common.utils import (
    create_clusterization_results,
    results_folder,
    read_field_from_json
)

# exemplo de execução
# python3 11_BIRCH.py 93

def run_all(file_rawdata_name, output_prefix):
    # --- CAMINHO DE SAÍDA (para salvar os resultados) ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_main_dir = os.path.join(script_dir, '..', 'Results')
    cluster_output_dir = os.path.join(output_main_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    # --- LEITURA E LIMPEZA DOS DADOS ---
    if not os.path.exists(file_rawdata_name):
        print(f"Error: Input file not found at {file_rawdata_name}")
        return

    data = pd.read_csv(file_rawdata_name, header=None)

    # Assume longitude = coluna 2, latitude = coluna 3
    data.iloc[:, 2] = pd.to_numeric(data.iloc[:, 2], errors='coerce')
    data.iloc[:, 3] = pd.to_numeric(data.iloc[:, 3], errors='coerce')
    data_cleaned = data.dropna(subset=[2, 3])
    data_cleaned = data_cleaned[
        (data_cleaned.iloc[:, 2] >= -180) & (data_cleaned.iloc[:, 2] <= 180) &
        (data_cleaned.iloc[:, 3] >= -90) & (data_cleaned.iloc[:, 3] <= 90)
    ]
    
    if data_cleaned.empty:
        print(f"Error: No valid coordinates for clustering in {file_rawdata_name}.")
        return

    # Padroniza os dados
    scaler = StandardScaler()
    coordinates = scaler.fit_transform(data_cleaned.iloc[:, [2, 3]])

    # Carrega hiperparâmetros
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    threshold = read_field_from_json(hyperparam_path, "threshold")
    n_clusters = read_field_from_json(hyperparam_path, "BIRCH_NCLUSTERS")

    birch_model = Birch(n_clusters=n_clusters, threshold=threshold)
    clusters = birch_model.fit_predict(coordinates)
    data_cleaned['Cluster'] = clusters

    # Salva resultados
    output_csv_path = os.path.join(cluster_output_dir, f'clusters_birch_{output_prefix}.csv')
    data_cleaned.iloc[:, [2, 3] + [data_cleaned.columns.get_loc('Cluster')]].to_csv(output_csv_path, index=False, header=None)
    print(f"Clusters saved to {output_csv_path}")

    # Carrega idioma
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Plota e salva o gráfico
    plt.figure(figsize=(10, 6))
    for cluster_id in np.unique(clusters):
        cluster_data = data_cleaned[data_cleaned['Cluster'] == cluster_id]
        plt.scatter(cluster_data.iloc[:, 2], cluster_data.iloc[:, 3], label=f"Cluster {cluster_id}")

    plt.title(f"{lang['grafico_BIRCH']} - Clusters: {n_clusters} - {output_prefix.capitalize()}")
    plt.xlabel(lang["xlabel_BIRCH"])
    plt.ylabel(lang["ylabel_BIRCH"])
    plt.legend()
    plt.grid()

    output_png_path = os.path.join(cluster_output_dir, f'birch_{output_prefix}.png')
    plt.savefig(output_png_path)
    plt.close()
    print(f"Plot saved to {output_png_path}")

    # Salva hiperparâmetros usados
    hiper_content = [f"Hyper BIRCH threshold {threshold}"]
    hiper_path = os.path.join(output_main_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        file.write(hiper_content[0] + '\n')

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

    raw_data = pd.read_csv(input_file_path, header=None, names=['id', 'Timestamp', 'Longitude', 'Latitude'])
    
    if raw_data.empty:
        print(f"Error for animal {current_animal}: Input file is empty.")
        return

    df = raw_data[:100].copy()
    
    # Padroniza os dados
    scaler = StandardScaler()
    coordinates = scaler.fit_transform(df[['Longitude', 'Latitude']])

    # Carrega hiperparâmetros
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    threshold = read_field_from_json(hyperparam_path, "threshold")    
    n_clusters = read_field_from_json(hyperparam_path, "BIRCH_NCLUSTERS")

    #birch_model = Birch(n_clusters=None, threshold=threshold)
    birch_model = Birch(n_clusters=n_clusters, threshold=threshold)
    df['Cluster'] = birch_model.fit_predict(coordinates)

    # Salva resultados no diretório de saída correto
    output_csv_path = os.path.join(cluster_output_dir, f'clusters_birch_map_{current_animal}.csv')
    df[['Longitude', 'Latitude', 'Cluster']].to_csv(output_csv_path, index=False, header=None)
    print(f"Clusters saved to {output_csv_path}")

    # Carrega idioma
    json_path = f'scripts/Data_preparation/language_{read_field_from_json(hyperparam_path, "language")}.json'
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Plota e salva o gráfico
    plt.figure(figsize=(10, 6))
    for cluster_id in np.unique(df['Cluster']):
        cluster_data = df[df['Cluster'] == cluster_id]
        plt.scatter(cluster_data['Longitude'], cluster_data['Latitude'], label=f"Cluster {cluster_id}")

    plt.title(lang["grafico_BIRCH"])
    plt.xlabel(lang["xlabel_BIRCH"])
    plt.ylabel(lang["ylabel_BIRCH"])
    plt.legend()
    plt.grid()

    output_png_path = os.path.join(cluster_output_dir, f'onca_{current_animal}_BIRCH.png')
    plt.savefig(output_png_path)
    plt.close() # Fecha a figura
    print(f"Plot saved to {output_png_path}")

    hiper_content = [f"Hyper BIRCH threshold {threshold}"]
    hiper_path = os.path.join(output_main_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        file.write(hiper_content[0] + '\n')

def run_mock():
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    run(current_animal, file_rawdata_name)