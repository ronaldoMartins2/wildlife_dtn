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

def run_all(file_rawdata_name, output_prefix):
    # --- CAMINHO DE SAÍDA (para salvar os resultados) ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_main_dir = os.path.join(script_dir, '..', 'Results')
    cluster_output_dir = os.path.join(output_main_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    if not os.path.exists(file_rawdata_name):
        print(f"Error: Input file not found at {file_rawdata_name}")
        return

    # Lê e limpa os dados
    data = pd.read_csv(file_rawdata_name, header=None)
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

    coordinates = data_cleaned.iloc[:, [2, 3]].values

    # Carrega hiperparâmetros
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    bandwidth = read_field_from_json(hyperparam_path, "bandwidth")

    mean_shift = MeanShift(bandwidth=bandwidth)
    mean_shift.fit(coordinates)
    data_cleaned['Cluster'] = mean_shift.labels_

    # Salva resultados
    output_csv_path = os.path.join(cluster_output_dir, f'clusters_mean_shift_{output_prefix}.csv')
    data_cleaned.iloc[:, [2, 3] + [data_cleaned.columns.get_loc('Cluster')]].to_csv(output_csv_path, index=False, header=None)
    print(f"Clusters saved to {output_csv_path}")

    # Carrega idioma
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Plota e salva o gráfico
    plt.figure(figsize=(8, 6))
    for cluster in np.unique(data_cleaned['Cluster']):
        cluster_points = data_cleaned[data_cleaned['Cluster'] == cluster]
        plt.scatter(cluster_points.iloc[:, 2], cluster_points.iloc[:, 3], label=f"Cluster {cluster}")

    plt.xlabel(f"{lang['xlabel_Mean_Shift']} - Clusters: {len(np.unique(mean_shift.labels_))}")
    plt.ylabel(lang["ylabel_Mean_Shift"])
    plt.title(lang["grafico_Mean_Shift"])
    plt.legend()
    plt.grid(True)

    output_png_path = os.path.join(cluster_output_dir, f'mean_shift_{output_prefix}.png')
    plt.savefig(output_png_path)
    plt.close()
    print(f"Plot saved to {output_png_path}")

    hiper_content = [f"Hyper Mean-Shift bandwidth {bandwidth}"]
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

    data = pd.read_csv(file_rawdata_name, header=None, names=['id', 'Timestamp', 'Longitude', 'Latitude'])
    data = data[:100]
    df = pd.DataFrame(data, columns=["ID", "Timestamp", "Longitude", "Latitude"])

    # Coerce coordinates to numeric, turning invalid values into NaN
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')

    # Convert Timestamp to numeric (optional, for time-based clustering)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    # Drop rows with NaN in Longitude or Latitude
    df.dropna(subset=['Longitude', 'Latitude'], inplace=True)

    df['TimeNumeric'] = (df['Timestamp'] - df['Timestamp'].min()).dt.total_seconds()
    # Remove rows with zero coordinates
    df = df[(df['Longitude'] != 0) & (df['Latitude'] != 0)]

    # Step 2: Prepare Data for Clustering (Longitude, Latitude)
    coordinates = df[['Longitude', 'Latitude']].values
    coordinates = df[['Longitude', 'Latitude']].iloc[:100].values

    # Check if there are enough samples for clustering
    if coordinates.shape[0] < 2:
        print(f"Warning: Not enough data points ({coordinates.shape[0]}) for Mean-Shift clustering for animal {current_animal}. Skipping.")
        return

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