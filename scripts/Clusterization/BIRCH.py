import numpy as np
import pandas as pd
from sklearn.cluster import Birch
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import os
import json
import sys

from sklearn.metrics.cluster import contingency_matrix
from sklearn.metrics import silhouette_score, davies_bouldin_score, pairwise_distances_argmin_min

from Common.utils import (
    create_clusterization_results,
    results_folder,
    read_field_from_json
)

def calculate_quality_metrics(y_true, y_pred):
    """
    y_true: IDs reais (ex: ID do animal)
    y_pred: IDs dos clusters gerados pelo algoritmo
    """
    # Matriz de contingência (linhas = classes reais, colunas = clusters)
    matrix = contingency_matrix(y_true, y_pred)
    N = np.sum(matrix)
    
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


def plot_quality_metrics_local(silhouette, dbi, quantization_error, output_dir, prefix, algorithm='birch'):
    """Plot silhouette score, davies-bouldin index e quantization error"""
    metrics = ['Silhouette Score', 'Davies-Bouldin Index', 'Quantization Error']
    values = [silhouette, dbi, quantization_error]
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(metrics, values, color=['#1f77b4', '#ff7f0e', '#2ca02c'], alpha=0.7)
    
    # Adiciona valores nas barras
    for bar, value in zip(bars, values):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{value:.4f}', ha='center', va='bottom', fontsize=10)
    
    plt.title(f'Clustering Quality Metrics - {prefix.replace("_", " ").title()} - {algorithm.upper()}')
    plt.ylabel('Score')
    plt.grid(axis='y', alpha=0.3)
    
    output_path = os.path.join(output_dir, f'quality_metrics_{prefix}_{algorithm}.png')
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()
    print(f"Quality metrics plot saved to {output_path}")


def extract_folder_name(file_rawdata):
    """Extrai o nome da pasta do file_rawdata_name"""
    file_name = file_rawdata.split('/')
    file_name = file_name[-1].split('.')[0]
    return file_name

def run_all(file_rawdata_name, file_rawdata, output_prefix):
    #Caminho de saída
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_name = extract_folder_name(file_rawdata)
    results_dir = os.path.join(script_dir, '..', 'Results', folder_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    #LEITURA E LIMPEZA DOS DADOS
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
    quantization_error = calculate_quantization_error(coords_original, centroids_final)
 
    # Salva apenas os centroides finais
    output_centroids_csv = os.path.join(cluster_output_dir, f'centroids_birch_{output_prefix}.csv')
    # adiciona índice (começando em 1) aos centroides
    df_centroids = pd.DataFrame(centroids_final, columns=['Longitude', 'Latitude']).reset_index().rename(columns={'index': 'Index'})
    df_centroids['Index'] = df_centroids['Index'] + 1
    df_centroids.to_csv(output_centroids_csv, index=False, header=None)
    print(f"Centroids saved to {output_centroids_csv}")

    #Novo: salvar mapeamento ponto -> centróide
    labels = birch_model.labels_
    df_points = data_cleaned.reset_index(drop=True).copy()
    df_map = pd.DataFrame({
        'id_centroid': (labels + 1),                        # centróides numerados a partir de 1
        'id_animal': df_points.iloc[:, 0].values,           # coluna ID original
        'timestamp': df_points.iloc[:, 1].values,           # coluna 1 é o timestamp
        'latitude_animal': df_points.iloc[:, 3].values,     # latitude
        'longitude_animal': df_points.iloc[:, 2].values     # longitude
    })
    map_file = os.path.join(cluster_output_dir, f'points_birch_mapping_{output_prefix}.csv')
    df_map.to_csv(map_file, index=False)
    print(f"Point->centroid mapping saved to {map_file}")

    #Metrics
    metrics_antigas = calculate_quality_metrics(df_points.iloc[:, 0].values, labels + 1)

    if len(np.unique(labels)) > 1:
        silhouette = silhouette_score(coordinates, labels)
        dbi = davies_bouldin_score(coordinates, labels)
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
    
    metrics['Algorithm'] = 'BIRCH'
    metrics_file = os.path.join(cluster_output_dir, f'Metricas_de_qualidade_{output_prefix}.csv')
    
    if os.path.exists(metrics_file):
        existing_df = pd.read_csv(metrics_file)
        new_df = pd.DataFrame([metrics])
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = pd.DataFrame([metrics])
        
    final_df.to_csv(metrics_file, index=False)
    print(f"Metrics saved to {metrics_file}")
    plot_quality_metrics_local(silhouette, dbi, quantization_error, cluster_output_dir, output_prefix, 'birch')
    
    # Opcional: Salvar em arquivo txt também (compatibilidade)
    results_path_txt = os.path.join(cluster_output_dir, f'metrics_{output_prefix}.txt')
    with open(results_path_txt, "w") as f:
        for k, v in metrics.items():
            if k != 'Algorithm':
                f.write(f"{k}: {v}\n")

    # Carrega idioma
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Plota e salva o gráfico
    plt.figure(figsize=(10, 6))
    plt.scatter(data_cleaned.iloc[:, 2], data_cleaned.iloc[:, 3], c=clusters, cmap='viridis', alpha=0.7, label='Data Points')

    # Adiciona centroides ao gráfico
    plt.scatter(centroids_final[:, 0], centroids_final[:, 1], color='red', marker='x', s=100, label='Centroids')

    plt.title(f"{lang['grafico_BIRCH']} - Clusters: {n_clusters} - Centroids:  {len(centroids_final)} - {output_prefix.capitalize()}")
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

    print(f"[BIRCH] n_clusters solicitado: {n_clusters}")
    print(f"[BIRCH] clusters únicos gerados: {len(np.unique(labels))}")
    print(f"[BIRCH] threshold usado: {threshold}")

def run(current_animal, file_rawdata_name):
    #Caminho de entrada
    input_results_dir = results_folder(file_rawdata_name)
    input_file_path = os.path.join(input_results_dir, f'map_{current_animal}.csv')

    #Caminhos para saida
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
    
    #Padroniza os dados
    scaler = StandardScaler()
    coordinates = scaler.fit_transform(df[['Longitude', 'Latitude']])

    #Carrega hiperparâmetros
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

    #Novo: salvar mapeamento ponto -> centróide para este mapa
    df_map = pd.DataFrame({
        'id_centroid': (df['Cluster'].astype(int) + 1).values,
        'id_animal': df['id'].values,
        'latitude_animal': df['Latitude'].values,
        'longitude_animal': df['Longitude'].values
    })
    map_file = os.path.join(cluster_output_dir, f'points_birch_mapping_{current_animal}.csv')
    df_map.to_csv(map_file, index=False)
    print(f"Point->centroid mapping saved to {map_file}")
    
    #INSERÇÃO DAS MÉTRICAS
    metrics_antigas = calculate_quality_metrics(df_map['id_animal'], df_map['id_centroid'])

    coords_original = df[['Longitude', 'Latitude']].values
    centroides_birch = []
    for i in np.unique(df['Cluster']):
        cluster_points = coords_original[df['Cluster'] == i]
        centroides_birch.append(cluster_points.mean(axis=0))
    centroides_birch = np.array(centroides_birch)
    quantization_error = calculate_quantization_error(coords_original, centroides_birch)

    labels_birch = df['Cluster'].values
    if len(np.unique(labels_birch)) > 1:
        silhouette = silhouette_score(coordinates, labels_birch)
        dbi = davies_bouldin_score(coordinates, labels_birch)
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
    
    metrias['Algorithm'] = 'BIRCH'
    metrics_csv_path = os.path.join(cluster_output_dir, f'Metricas_de_qualidade_{current_animal}.csv')
    if os.path.exists(metrics_csv_path):
        existing_df = pd.read_csv(metrics_csv_path)
        final_df = pd.concat([existing_df, pd.DataFrame([metrias])], ignore_index=True)
    else:
        final_df = pd.DataFrame([metrias])
    final_df.to_csv(metrics_csv_path, index=False)
    print(f"Metrics saved to {metrics_csv_path}")
    plot_quality_metrics_local(silhouette, dbi, quantization_error, cluster_output_dir, current_animal, 'birch')

    # Opcional: Salvar em arquivo
    results_path = os.path.join(cluster_output_dir, f'metrics_{current_animal}.txt')
    with open(results_path, "w") as f:
        for k, v in metrias.items():
            f.write(f"{k}: {v}\n")

    # Carrega idioma
    json_path = f'scripts/Data_preparation/language_{read_field_from_json(hyperparam_path, "language")}.json'
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Plota e salva o gráfico
    plt.figure(figsize=(10, 6))
    plt.scatter(df['Longitude'], df['Latitude'], c=df['Cluster'], cmap='viridis', label='Data Points', alpha=0.7)

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

# --- LEGACY CODE ---
# The following code was replaced to standardize the legend with kmeans style.

# In run_all():
#     for cluster_id in np.unique(clusters):
#         cluster_data = data_cleaned[data_cleaned['Cluster'] == cluster_id]
#         plt.scatter(cluster_data.iloc[:, 2], cluster_data.iloc[:, 3], label=f"Cluster {cluster_id + 1}")

# In run():
#     for cluster_id in np.unique(df['Cluster']):
#         cluster_data = df[df['Cluster'] == cluster_id]
#         plt.scatter(cluster_data['Longitude'], cluster_data['Latitude'], label=f"Cluster {cluster_id}")