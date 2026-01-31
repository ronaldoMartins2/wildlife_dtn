import numpy as np
import pandas as pd
import sys
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import os
import json
from sklearn.metrics.cluster import contingency_matrix
from Common.utils import (
    create_clusterization_results,
    results_folder,
    read_field_from_json
)

def calculate_quality_metrics(y_true, y_pred):
    """
    Calcula Purity, F-measure e Entropia baseados no PDF.
    y_true: IDs dos animais (L)
    y_pred: IDs dos clusters (C)
    """
    # Matriz de contingência: linhas são categorias reais (L), colunas são clusters (C)
    # n_ij = quantidade de itens da categoria i no cluster j
    matrix = contingency_matrix(y_true, y_pred)
    N = np.sum(matrix)
    
    # --- PURITY --- [cite: 36]
    # Soma dos máximos de cada cluster dividida pelo total N
    purity = np.sum(np.amax(matrix, axis=0)) / N
    
    # --- ENTROPIA --- [cite: 54, 55]
    total_entropy = 0
    cluster_sums = np.sum(matrix, axis=0)
    for j in range(matrix.shape[1]):
        nj = cluster_sums[j]
        if nj > 0:
            # Probabilidade p(i,j) de encontrar animal i no cluster j
            p_ij = matrix[:, j] / nj
            # Apenas valores maiores que zero para o log
            p_ij_nonzero = p_ij[p_ij > 0]
            cluster_entropy = -np.sum(p_ij_nonzero * np.log2(p_ij_nonzero))
            total_entropy += (nj / N) * cluster_entropy
            
    # --- F-MEASURE --- [cite: 45, 50]
    # F = (2 * Recall * Precision) / (Recall + Precision)
    precision = matrix / matrix.sum(axis=0) # Precisão por cluster
    recall = matrix / matrix.sum(axis=1)[:, None] # Revocação por categoria
    
    # Substituir NaNs por 0 em casos de divisão por zero
    f_matrix = np.nan_to_num((2 * precision * recall) / (precision + recall))
    # Para cada categoria real, pega o melhor F-measure entre os clusters e faz a média ponderada
    f_measured = np.sum(np.amax(f_matrix, axis=1) * matrix.sum(axis=1)) / N
    
    return {
        "Purity": purity,
        "Entropy": total_entropy,
        "F-Measure": f_measured
    }

def extract_folder_name(file_rawdata):
    """Extrai o nome da pasta do file_rawdata_name"""
    file_name = file_rawdata.split('/')
    file_name = file_name[-1].split('.')[0]
    return file_name
def run_all(file_rawdata_name, file_rawdata, output_prefix=None):
    
    # Define o caminho para SALVAR os resultados usando o nome extraído de file_rawdata (dataset original)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_name = extract_folder_name(file_rawdata)
    results_dir = os.path.join(script_dir, '..', 'Results', folder_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    if not os.path.exists(file_rawdata_name):
        print(f"Error: Input file not found at {file_rawdata_name}")
        return

    data = pd.read_csv(file_rawdata_name, header=None)
    data.iloc[:, 2] = pd.to_numeric(data.iloc[:, 2], errors='coerce')
    data.iloc[:, 3] = pd.to_numeric(data.iloc[:, 3], errors='coerce')
    data_cleaned = data.dropna(subset=[2, 3])
    data_cleaned = data_cleaned[
        (data_cleaned.iloc[:, 2] >= -180) & (data_cleaned.iloc[:, 2] <= 180) &
        (data_cleaned.iloc[:, 3] >= -90) & (data_cleaned.iloc[:, 3] <= 90)
    ]
    coords = data_cleaned.iloc[:, [2, 3]].values

    if coords.shape[0] == 0:
        print("No valid coordinates for clustering.")
        return

    # Hiperparâmetros
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    n_clusters = read_field_from_json(hyperparam_path, "n_clusters_kmeans")
    random_state = read_field_from_json(hyperparam_path, "random_state_kmeans")
    n_init = read_field_from_json(hyperparam_path, "n_init_kmeans")

    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=n_init)
    kmeans.fit(coords)
    centroids = kmeans.cluster_centers_

    # Salva centroides
    output_file_csv = os.path.join(cluster_output_dir, f'centroids_kmeans_{output_prefix}.csv')
    df_centroids = pd.DataFrame(centroids, columns=['Longitude', 'Latitude']).reset_index().rename(columns={'index': 'Index'})
    df_centroids['Index'] = df_centroids['Index'] + 1  # começa por 1
    df_centroids.to_csv(output_file_csv, index=False, header=None)
    print(f"Cluster centroids saved to {output_file_csv}")

    # --- Novo: salvar mapeamento ponto -> centróide ---
    labels = kmeans.predict(coords)
    df_points = data_cleaned.reset_index(drop=True).copy()
    df_map = pd.DataFrame({
        'id_centroid': (labels + 1),                        # centróides numerados a partir de 1
        'id_animal': df_points.iloc[:, 0].values,           # coluna ID original
        'timestamp': df_points.iloc[:, 1].values,           # coluna 1 é o timestamp
        'latitude_animal': df_points.iloc[:, 3].values,     # latitude
        'longitude_animal': df_points.iloc[:, 2].values     # longitude
    })
    map_file = os.path.join(cluster_output_dir, f'points_kmeans_mapping_{output_prefix}.csv')
    df_map.to_csv(map_file, index=False)
    print(f"Point->centroid mapping saved to {map_file}")

    # --- Metrics ---
    metrics = calculate_quality_metrics(df_points.iloc[:, 0].values, labels + 1)
    metrics['Algorithm'] = 'KMeans'
    metrics_file = os.path.join(cluster_output_dir, f'Metricas_de_qualidade_{output_prefix}.csv')
    
    if os.path.exists(metrics_file):
        existing_df = pd.read_csv(metrics_file)
        new_df = pd.DataFrame([metrics])
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = pd.DataFrame([metrics])
        
    final_df.to_csv(metrics_file, index=False)
    print(f"Metrics saved to {metrics_file}")

    # Gráfico
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    plt.figure(figsize=(10, 6))
    plt.scatter(coords[:, 0], coords[:, 1], c=kmeans.labels_, cmap='viridis', alpha=0.7, label='Data Points')
    plt.scatter(centroids[:, 0], centroids[:, 1], color='red', marker='x', s=100, label='Centroids')
    plt.title(f"{lang['grafico_kmeans_individual']} - Clusters: {n_clusters} - {output_prefix.capitalize()}")
    plt.xlabel(lang["xlabel_kmeans_individual"])
    plt.ylabel(lang["ylabel_kmeans_individual"])
    plt.legend()
    plt.grid(True)
    output_file_png = os.path.join(cluster_output_dir, f'kmeans_{output_prefix}.png')
    plt.savefig(output_file_png)
    plt.close()
    print(f"Plot saved to {output_file_png}")

def run(current_animal, file_rawdata_name):
    
    # --- CAMINHOS DE ENTRADA E SAÍDA ---
    # Define o caminho para LER os dados de entrada
    input_results_dir = results_folder(file_rawdata_name)
    input_file_path = os.path.join(input_results_dir, f'map_{current_animal}.csv')

    # Define o caminho para SALVAR os resultados usando o nome extraído do file_rawdata_name
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_name = extract_folder_name(file_rawdata_name)
    results_dir = os.path.join(script_dir, '..', 'Results', folder_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    # --- LEITURA E LIMPEZA DOS DADOS ---
    # Verifica se o arquivo de entrada existe antes de prosseguir
    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}")
        return

    data = pd.read_csv(input_file_path, header=None)
    
    # Limpa e formata as colunas de coordenadas
    data.iloc[:, 2] = pd.to_numeric(data.iloc[:, 2].replace({',': ''}, regex=True), errors='coerce')
    data.iloc[:, 3] = pd.to_numeric(data.iloc[:, 3].replace({',': ''}, regex=True), errors='coerce')

    # Remove linhas com coordenadas inválidas
    data_cleaned = data.dropna(subset=[2, 3])
    data_cleaned = data_cleaned[(data_cleaned.iloc[:, 2] != 0) & (data_cleaned.iloc[:, 3] != 0)]
    data_cleaned = data_cleaned[
        (data_cleaned.iloc[:, 2] >= -180) & (data_cleaned.iloc[:, 2] <= 180) &
        (data_cleaned.iloc[:, 3] >= -90) & (data_cleaned.iloc[:, 3] <= 90)
    ]

    coords = data_cleaned.iloc[:, [2, 3]].values

    # Interrompe a execução se não houver dados válidos após a limpeza
    if coords.shape[0] == 0:
        print(f"Error for animal {current_animal}: No valid coordinates left for clustering.")
        return

    # --- CLUSTERIZAÇÃO K-MEANS ---
    # Carrega os hiperparâmetros
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    n_clusters = read_field_from_json(hyperparam_path, "n_clusters_kmeans")
    random_state = read_field_from_json(hyperparam_path, "random_state_kmeans")
    n_init = read_field_from_json(hyperparam_path, "n_init_kmeans")

    # Aplica o algoritmo KMeans
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=n_init)
    kmeans.fit(coords)
    centroids = kmeans.cluster_centers_

    # --- SALVAMENTO DOS RESULTADOS ---
    # Salva os hiperparâmetros usados no arquivo de log
    hiper_content = [
        f"Hyper kmeans n_clusters {n_clusters}",
        f"Hyper kmeans random_state {random_state}",
        f"Hyper kmeans n_init {n_init}"
    ]
    hiper_path = os.path.join(results_dir, 'hiperparameters.txt') # Salva no diretório de resultados específico
    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    # Salva as coordenadas dos centroides em um arquivo CSV
    output_file_csv = os.path.join(cluster_output_dir, f'clusters_kmeans_{current_animal}.csv')
    df_centroids = pd.DataFrame(centroids, columns=['Longitude', 'Latitude']).reset_index().rename(columns={'index': 'Index'})
    df_centroids['Index'] = df_centroids['Index'] + 1  # começa por 1
    df_centroids.to_csv(output_file_csv, index=False, header=None)
    print(f"Cluster centroids saved to {output_file_csv}")

    # --- Novo: salvar mapeamento ponto -> centróide ---
    labels = kmeans.predict(coords)
    df_points = data_cleaned.reset_index(drop=True).copy()
    df_map = pd.DataFrame({
        'id_centroid': (labels + 1),
        'id_animal': df_points.iloc[:, 0].values,
        'latitude_animal': df_points.iloc[:, 3].values,
        'longitude_animal': df_points.iloc[:, 2].values
    })
    map_file = os.path.join(cluster_output_dir, f'points_kmeans_mapping_{current_animal}.csv')
    df_map.to_csv(map_file, index=False)
    print(f"Point->centroid mapping saved to {map_file}")

    # --- GERAÇÃO DO GRÁFICO ---
    # Carrega textos do gráfico (título, eixos) de acordo com o idioma definido
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Cria e personaliza o gráfico
    plt.figure(figsize=(10, 6))
    plt.scatter(coords[:, 0], coords[:, 1], c=kmeans.labels_, cmap='viridis', alpha=0.7, label='Data Points')
    plt.scatter(centroids[:, 0], centroids[:, 1], color='red', marker='x', s=100, label='Centroids')

    plt.title(f"{lang['grafico_kmeans_individual']} - Clusters: {n_clusters}")
    plt.xlabel(lang["xlabel_kmeans_individual"])
    plt.ylabel(lang["ylabel_kmeans_individual"])
    plt.legend()
    plt.grid(True)

    # Salva o gráfico como imagem e fecha a figura para liberar memória
    output_file_png = os.path.join(cluster_output_dir, f'onca_{current_animal}_kmeans.png')
    plt.savefig(output_file_png)
    plt.close()
    print(f"Plot saved to {output_file_png}")

def run_mock():
    # Garante que ambos os argumentos (ID do animal e nome do arquivo de dados) sejam lidos do terminal
    if len(sys.argv) < 3:
        print("Usage: python3 script_name.py <animal_id> <raw_data_name>")
        sys.exit(1)
    
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    run(current_animal, file_rawdata_name)

# Exemplo de como chamar o script
# python3 6_kmeans_individual_csv.py 94 jaguar_mamiraua