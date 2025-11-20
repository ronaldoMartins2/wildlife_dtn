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

def extract_folder_name(file_rawdata):
    """Extrai o nome da pasta do file_rawdata_name"""
    file_name = file_rawdata.split('/')
    file_name = file_name[-1].split('.')[0]
    return file_name

# exemplo de execução
# python3 11_BIRCH.py 93

def run_all(file_rawdata_name, file_rawdata, output_prefix):
    # --- CAMINHO DE SAÍDA (para salvar os resultados) ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_name = extract_folder_name(file_rawdata)
    results_dir = os.path.join(script_dir, '..', 'Results', folder_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
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

    # Salva resultados dos clusters
    output_csv_path = os.path.join(cluster_output_dir, f'clusters_birch_{output_prefix}.csv')
    # adiciona índice (começando em 1) como primeira coluna antes de salvar
    df_clusters = data_cleaned.iloc[:, [2, 3] + [data_cleaned.columns.get_loc('Cluster')]].copy()
    df_clusters.insert(0, 'Index', range(1, len(df_clusters) + 1))
    df_clusters.to_csv(output_csv_path, index=False, header=None)
    print(f"Clusters saved to {output_csv_path}")
 
    # Após ajustar o modelo
    labels = birch_model.labels_
    coords_original = scaler.inverse_transform(coordinates)
    centroids_final = []
    for cluster_id in np.unique(labels):
        cluster_points = coords_original[labels == cluster_id]
        centroid = cluster_points.mean(axis=0)
        centroids_final.append(centroid)
    centroids_final = np.array(centroids_final)
 
    # Salva apenas os centroides finais
    output_centroids_csv = os.path.join(cluster_output_dir, f'centroids_birch_{output_prefix}.csv')
    # adiciona índice (começando em 1) aos centroides
    df_centroids = pd.DataFrame(centroids_final, columns=['Longitude', 'Latitude']).reset_index().rename(columns={'index': 'Index'})
    df_centroids['Index'] = df_centroids['Index'] + 1
    df_centroids.to_csv(output_centroids_csv, index=False, header=None)
    print(f"Centroids saved to {output_centroids_csv}")

    # --- Novo: salvar mapeamento ponto -> centróide ---
    labels = birch_model.labels_
    df_points = data_cleaned.reset_index(drop=True).copy()
    df_map = pd.DataFrame({
        'id_centroid': (labels + 1),                        # centróides numerados a partir de 1
        'id_animal': df_points.iloc[:, 0].values,           # coluna ID original
        'timestamp': df_points.iloc[:, 1].values,  # coluna 1 é o timestamp
        'latitude_animal': df_points.iloc[:, 3].values,     # latitude
        'longitude_animal': df_points.iloc[:, 2].values     # longitude
    })
    map_file = os.path.join(cluster_output_dir, f'points_birch_mapping_{output_prefix}.csv')
    df_map.to_csv(map_file, index=False)
    print(f"Point->centroid mapping saved to {map_file}")

    # Carrega idioma
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Plota e salva o gráfico
    plt.figure(figsize=(10, 6))
    for cluster_id in np.unique(clusters):
        cluster_data = data_cleaned[data_cleaned['Cluster'] == cluster_id]
        plt.scatter(cluster_data.iloc[:, 2], cluster_data.iloc[:, 3], label=f"Cluster {cluster_id + 1}")

    # Adiciona centroides ao gráfico (corrigido!)
    plt.scatter(centroids_final[:, 0], centroids_final[:, 1], color='red', marker='x', s=100, label='Centroids')

    plt.title(f"{lang['grafico_BIRCH']} - Clusters: {n_clusters} - Centroids:  - {output_prefix.capitalize()}")
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
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        file.write(hiper_content[0] + '\n')

def run(current_animal, file_rawdata_name):
    # --- CAMINHO DE ENTRADA (para ler os dados) ---
    input_results_dir = results_folder(file_rawdata_name)
    input_file_path = os.path.join(input_results_dir, f'map_{current_animal}.csv')

    # --- CAMINHO DE SAÍDA (para salvar os resultados) ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_name = extract_folder_name(file_rawdata_name)
    results_dir = os.path.join(script_dir, '..', 'Results', folder_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
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
    # adiciona índice (começando em 1) como primeira coluna antes de salvar
    df_out = df[['Longitude', 'Latitude', 'Cluster']].copy()
    df_out.insert(0, 'Index', range(1, len(df_out) + 1))
    df_out.to_csv(output_csv_path, index=False, header=None)
    print(f"Clusters saved to {output_csv_path}")

    # --- Novo: salvar mapeamento ponto -> centróide para este mapa ---
    df_map = pd.DataFrame({
        'id_centroid': (df['Cluster'].astype(int) + 1).values,
        'id_animal': df['id'].values,
        'latitude_animal': df['Latitude'].values,
        'longitude_animal': df['Longitude'].values
    })
    map_file = os.path.join(cluster_output_dir, f'points_birch_mapping_{current_animal}.csv')
    df_map.to_csv(map_file, index=False)
    print(f"Point->centroid mapping saved to {map_file}")

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
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        file.write(hiper_content[0] + '\n')

def run_mock():
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    run(current_animal, file_rawdata_name)