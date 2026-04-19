import numpy as np
import pandas as pd
import sys
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import os
import json
from sklearn.metrics.cluster import contingency_matrix
from sklearn.metrics import silhouette_score, davies_bouldin_score, pairwise_distances_argmin_min
from Common.utils import (
    create_clusterization_results,
    results_folder,
    read_field_from_json,
    plot_cluster_quality_metrics
)

def calculate_quality_metrics(y_true, y_pred):
    """
    y_true: IDs reais (ex: ID do animal)
    y_pred: IDs dos clusters gerados pelo algoritmo
    """
    # Matriz de contingência (linhas = classes reais, colunas = clusters)
    matrix = contingency_matrix(y_true, y_pred)
    N = np.sum(matrix) # Total de itens [cite: 35]
    
    # 1. PURITY [cite: 36]
    purity = np.sum(np.amax(matrix, axis=0)) / N
    
    # 2. ENTROPIA (Global) [cite: 54]
    total_entropy = 0
    cluster_sums = np.sum(matrix, axis=0)
    for j in range(matrix.shape[1]):
        nj = cluster_sums[j]
        if nj > 0:
            p_ij = matrix[:, j] / nj
            p_ij_nonzero = p_ij[p_ij > 0]
            cluster_entropy = -np.sum(p_ij_nonzero * np.log2(p_ij_nonzero))
            total_entropy += (nj / N) * cluster_entropy
            
    # 3. F-MEASURED [cite: 50]
    # Precisão = n_ij / col_sum; Revocação = n_ij / row_sum
    # Avoid division by zero
    col_sums = matrix.sum(axis=0)
    row_sums = matrix.sum(axis=1)
    
    precision = np.divide(matrix, col_sums, out=np.zeros_like(matrix, dtype=float), where=col_sums!=0)
    recall = np.divide(matrix, row_sums[:, None], out=np.zeros_like(matrix, dtype=float), where=row_sums[:, None]!=0)
    
    f_matrix = np.divide(2 * precision * recall, precision + recall, 
                         out=np.zeros_like(matrix, dtype=float), where=(precision + recall)!=0)
    # Média ponderada do melhor F-measure por categoria [cite: 40, 45]
    f_measured = np.sum(np.amax(f_matrix, axis=1) * row_sums) / N
    
    # 4. PARTITION COEFFICIENT (PC) [cite: 57]
    # Para cada cluster j, calcula a soma dos quadrados das proporções de cada animal
    pc_clusters = []
    for j in range(matrix.shape[1]):
        nj = cluster_sums[j]
        if nj > 0:
            # Fração de cada animal no cluster j: |Cp ∩ Cp+| / |Cp|
            proportions = matrix[:, j] / nj
            pc_j = np.sum(proportions**2) # Segundo a descrição do PDF de ser entre 1/k+ e 1 
            pc_clusters.append(pc_j)
    
    avg_pc = np.mean(pc_clusters) if pc_clusters else 0
    
    return {
        "Purity": purity,
        "Entropy": total_entropy,
        "F-Measure": f_measured,
        "PC": avg_pc
    }

def calculate_quantization_error(points, centers):
    _, distances = pairwise_distances_argmin_min(points, centers)
    return float(np.mean(distances)) if len(distances) > 0 else 0.0


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
    quantization_error = calculate_quantization_error(coords, centroids)

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

    #Metrics
    metrics_antigas = calculate_quality_metrics(df_points.iloc[:, 0].values, labels + 1)

    if len(np.unique(labels)) > 1:
        silhouette = silhouette_score(coords, labels)
        dbi = davies_bouldin_score(coords, labels)
    else:
        silhouette = -1.0
        dbi = -1.0

    metrics = {
        "Purity": metrics_antigas['Purity'],
        "Entropy": metrics_antigas['Entropy'],
        "F-Measure": metrics_antigas['F-Measure'],
        "Partition Coefficient (PC)": metrics_antigas['PC'],
        "Silhouette Score": silhouette,
        "Davies-Bouldin Index": dbi,
        "Quantization Error": quantization_error
    }

    print("\n--- Resultados de Qualidade da Clusterização (Run All) ---")
    print(f"Purity:      {metrics_antigas['Purity']:.4f}")
    print(f"Entropy:     {metrics_antigas['Entropy']:.4f}")
    print(f"F-Measure:   {metrics_antigas['F-Measure']:.4f}")
    print(f"Partition Coeff (PC): {metrics_antigas['PC']:.4f}")
    print(f"Silhouette Score: {silhouette:.4f}")
    print(f"Davies-Bouldin Index: {dbi:.4f}")
    print(f"Quantization Error: {quantization_error:.4f}")
    
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
    plot_cluster_quality_metrics(cluster_output_dir, output_prefix=f'quality_metrics_comparison_{output_prefix}')
    
    # Opcional: Salvar em arquivo txt também
    results_path_txt = os.path.join(cluster_output_dir, f'metrics_{output_prefix}.txt')
    with open(results_path_txt, "w") as f:
        for k, v in metrics.items():
            if k != 'Algorithm':
                f.write(f"{k}: {v}\n")

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
    quantization_error = calculate_quantization_error(coords, centroids)

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

    # --- INSERÇÃO DAS MÉTRICAS ---
    metrics_antigas = calculate_quality_metrics(df_map['id_animal'], df_map['id_centroid'])

    if len(np.unique(labels)) > 1:
        silhouette = silhouette_score(coords, labels)
        dbi = davies_bouldin_score(coords, labels)
    else:
        silhouette = -1.0
        dbi = -1.0

    metrias = {
        "Purity": metrics_antigas['Purity'],
        "Entropy": metrics_antigas['Entropy'],
        "F-Measure": metrics_antigas['F-Measure'],
        "Partition Coefficient (PC)": metrics_antigas['PC'],
        "Silhouette Score": silhouette,
        "Davies-Bouldin Index": dbi,
        "Quantization Error": quantization_error
    }

    print("\n--- Resultados de Qualidade da Clusterização ---")
    print(f"Purity:      {metrics_antigas['Purity']:.4f}")
    print(f"Entropy:     {metrics_antigas['Entropy']:.4f}")
    print(f"F-Measure:   {metrics_antigas['F-Measure']:.4f}")
    print(f"Partition Coeff (PC): {metrics_antigas['PC']:.4f}")
    print(f"Silhouette Score: {silhouette:.4f}")
    print(f"Davies-Bouldin Index: {dbi:.4f}")
    print(f"Quantization Error: {quantization_error:.4f}")
    
    metrias['Algorithm'] = 'KMeans'
    metrics_csv_path = os.path.join(cluster_output_dir, f'Metricas_de_qualidade_{current_animal}.csv')
    if os.path.exists(metrics_csv_path):
        existing_df = pd.read_csv(metrics_csv_path)
        final_df = pd.concat([existing_df, pd.DataFrame([metrias])], ignore_index=True)
    else:
        final_df = pd.DataFrame([metrias])
    final_df.to_csv(metrics_csv_path, index=False)
    print(f"Metrics saved to {metrics_csv_path}")
    plot_cluster_quality_metrics(cluster_output_dir, output_prefix=f'quality_metrics_comparison_{current_animal}')

    # Opcional: Salvar em arquivo
    results_path = os.path.join(cluster_output_dir, f'metrics_{current_animal}.txt')
    with open(results_path, "w") as f:
        for k, v in metrias.items():
            f.write(f"{k}: {v}\n")

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