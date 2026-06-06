import numpy as np
import pandas as pd
import sys
import matplotlib.pyplot as plt
import json
from minisom import MiniSom  # Import MiniSom for SOM
import os
from sklearn.metrics.cluster import contingency_matrix
from sklearn.metrics import silhouette_score, davies_bouldin_score
from Common.utils import (
    create_clusterization_results,
    results_folder,
    read_field_from_json
)

script_dir = os.path.dirname(os.path.abspath(__file__))
data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

# Global variables for cluster numbers (loaded from hyperparameters.json)
N_CLUSTERS_KMEANS = read_field_from_json(hyperparam_path, "n_clusters_kmeans")
N_CLUSTERS_BIRCH = read_field_from_json(hyperparam_path, "BIRCH_NCLUSTERS")
N_CLUSTERS_SOM = read_field_from_json(hyperparam_path, "som_x") * read_field_from_json(hyperparam_path, "som_y")

# Dictionary for easy access in plots
CLUSTER_CONFIG = {
    'K-Means': N_CLUSTERS_KMEANS,
    'SOM': N_CLUSTERS_SOM,
    'BIRCH': N_CLUSTERS_BIRCH
}

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

def extract_folder_name(file_rawdata):
    """Extrai o nome da pasta do file_rawdata_name"""
    file_name = file_rawdata.split('/')
    file_name = file_name[-1].split('.')[0]
    return file_name


def normalize_title_text(text):
    normalized = text.replace('_', ' ')
    normalized = normalized.replace('BiLSTM', 'BiLSTM').replace('bilstm', 'BiLSTM').replace('bilstm', 'BiLSTM')
    formatted_words = []
    for token in normalized.split():
        if token == 'BiLSTM':
            formatted_words.append(token)
        else:
            formatted_words.append(token.capitalize())
    return ' '.join(formatted_words).strip()


def plot_quality_metrics_local(silhouette, dbi, quantization_error, output_dir, prefix, algorithm='som', n_clusters=None):
    """Plot silhouette score, davies-bouldin index e quantization error"""
    metrics = ['Silhouette Score', 'Davies-Bouldin Index', 'Quantization Error']
    values = [silhouette if silhouette is not None else 0, dbi if dbi is not None else 0, quantization_error]
    hatches = ['/', '\\', 'x']
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(metrics, values, color=['#1f77b4', '#ff7f0e', '#2ca02c'], alpha=0.7)
    for bar, hatch in zip(bars, hatches):
        bar.set_hatch(hatch)
    
    # Adiciona valores nas barras (rotacionado para não sobrepor título)
    for bar, value in zip(bars, values):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                f'{value:.4f}', ha='center', va='bottom', fontsize=9, rotation=90)
    
    display_prefix = normalize_title_text(prefix)
    title = f'Clustering Quality Metrics - {display_prefix.title()} - {algorithm.upper()}'
    if n_clusters is not None:
        title += f' - Clusters: {n_clusters}'
    plt.title(title)
    plt.ylabel('Score')
    plt.grid(axis='y', alpha=0.3)
    
    output_path = os.path.join(output_dir, f'quality_metrics_{prefix}_{algorithm}.png')
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()
    print(f"Quality metrics plot saved to {output_path}")


def plot_quality_metrics_comparison(metrics_csv_path, output_dir=None, prefix=None, algorithms=None, n_clusters=None):
    """Plot a grouped comparison of the three quality metrics for multiple algorithms."""
    if output_dir is None:
        output_dir = os.path.dirname(metrics_csv_path)
    if prefix is None:
        prefix = os.path.splitext(os.path.basename(metrics_csv_path))[0].replace('Metricas_de_qualidade_', '')

    if not os.path.exists(metrics_csv_path):
        print(f"Error: metrics file not found at {metrics_csv_path}")
        return

    try:
        df = pd.read_csv(metrics_csv_path)
    except Exception as e:
        print(f"Error reading metrics file {metrics_csv_path}: {e}")
        return

    if df.empty:
        print(f"Error: metrics file {metrics_csv_path} is empty.")
        return

    supported_algorithms = ['K-Means', 'SOM', 'BIRCH']
    if algorithms is None:
        algorithms = supported_algorithms

    df = df[df['Algorithm'].isin(algorithms)].copy()
    if df.empty:
        print(f"Error: no metrics found for algorithms {algorithms} in {metrics_csv_path}.")
        return

    # Keep the last row for each algorithm in case the file has multiple entries
    df = df.groupby('Algorithm', sort=False).last().reindex(algorithms).dropna(axis=0, how='all')

    metrics = ['Silhouette Score', 'Davies-Bouldin Index', 'Quantization Error']
    x = np.arange(len(metrics))
    bar_width = 0.2
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
    hatches = ['/', '\\', 'x']

    plt.figure(figsize=(12, 6))
    for idx, algorithm in enumerate(df.index):
        row = df.loc[algorithm]
        values = [float(row.get(metric, 0) or 0) for metric in metrics]
        positions = x + idx * bar_width
        bars = plt.bar(positions, values, width=bar_width, label=algorithm, color=colors[idx % len(colors)], alpha=0.8, hatch=hatches[idx % len(hatches)])
        for bar, value in zip(bars, values):
            bar.set_hatch(hatches[idx % len(hatches)])
            # Place text above the bar, horizontal (0 degrees) for better readability
            plt.text(bar.get_x() + bar.get_width() / 2., bar.get_height() + 0.01,
                     f'{value:.4f}', ha='center', va='bottom', fontsize=8)

    plt.xticks(x + bar_width * (len(df.index) - 1) / 2, metrics)
    plt.ylabel('Score')
    display_prefix = normalize_title_text(prefix)
    title = f'Comparativo de Métricas de Qualidade - {display_prefix.title()}'
    #title = f'Comparison of quality metrics - {display_prefix.title()}'
    if n_clusters is not None:
        title += f' - Clusters: {n_clusters}'
    plt.title(title)
    plt.legend()
    plt.grid(axis='y', alpha=0.3)

    output_path = os.path.join(output_dir, f'quality_metrics_comparison_{prefix}.png')
    plt.savefig(output_path, dpi=100, bbox_inches='tight')
    plt.close()
    print(f"Comparative metrics plot saved to {output_path}")

# pip install minisom

# python3 -m venv venv
# source ./venv/bin/activate

# python3 7_SOM_individual.py 94

def run_all(file_rawdata_name, file_rawdata, output_prefix, n_clusters):
    #CAMINHO DE SAÍDA (para salvar os resultados)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_name = extract_folder_name(file_rawdata)
    results_dir = os.path.join(script_dir, '..', 'Results', folder_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization', str(n_clusters))
    create_clusterization_results(cluster_output_dir)

    if not os.path.exists(file_rawdata_name):
        print(f"Error: Input file not found at {file_rawdata_name}")
        return

    # Lê e limpa os dados
    data = pd.read_csv(file_rawdata_name, header=None)
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

    if len(data_cleaned) < 8:
        print(f"Warning: Not enough data for SOM clustering (needs at least 8 rows, found {len(data_cleaned)}). Skipping.")
        return

    coords = data_cleaned.iloc[:, [2, 3]].values

    # Converter para float e limpar NaN/inf
    print(f"[SOM] coords shape before cleaning: {coords.shape}")
    print(f"[SOM] coords dtype: {coords.dtype}")
    
    # Converter para float (força conversão segura)
    try:
        coords = coords.astype(float)
    except (ValueError, TypeError) as e:
        print(f"[SOM] Erro ao converter coords para float: {e}")
        print(f"[SOM] Primeiras linhas de coords: {coords[:5]}")
        return
    
    # Remover NaN e inf
    valid_mask = ~(np.isnan(coords).any(axis=1) | np.isinf(coords).any(axis=1))
    coords_clean = coords[valid_mask]
    print(f"[SOM] coords shape after cleaning: {coords_clean.shape}")
    print(f"[SOM] Removidas {coords.shape[0] - coords_clean.shape[0]} linhas com NaN/inf")
    
    if coords_clean.shape[0] == 0:
        print(f"[SOM] All coordinates are NaN/inf. Skipping SOM training.")
        return
    
    coords = coords_clean
    
    # Atualizar data_cleaned para refletir limpeza
    data_cleaned = data_cleaned[valid_mask]
    
    # Salva as coordenadas usadas no diretório de saída correto
    output_csv_path = os.path.join(cluster_output_dir, f'clusters_som_{output_prefix}.csv')
    df_coords = data_cleaned.iloc[:, [2, 3]].copy()
    df_coords.insert(0, 'Index', range(1, len(df_coords) + 1))  # começa por 1
    df_coords.to_csv(output_csv_path, index=False, header=None)
    print(f"Coordinates saved to {output_csv_path}")

    # Carrega hiperparâmetros
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    sigma = read_field_from_json(hyperparam_path, "sigma_SOM")
    learning_rate = read_field_from_json(hyperparam_path, "learning_rate_SOM")
    ephocs = read_field_from_json(hyperparam_path, "ephocs_SOM")
    # som_x = read_field_from_json(hyperparam_path,"som_x")
    som_x = n_clusters
    som_y = read_field_from_json(hyperparam_path,"som_y")

    som = MiniSom(som_x, som_y, coords.shape[1], sigma, learning_rate)
    som.random_weights_init(coords)
    som.train_random(coords, ephocs)
    
    hiper_content = [
        f"Hyper SOM sigma {sigma}",
        f"Hyper SOM learning_rate {learning_rate}",
        f"Hyper SOM ephocs {ephocs}"
    ]
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    cluster_map = {i: som.winner(coord) for i, coord in enumerate(coords)}
    clusters = np.array([cluster_map[i][0] for i in range(len(coords))])

    json_path = f'scripts/Data_preparation/language_{read_field_from_json(hyperparam_path, "language")}.json'
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Após treinar o SOM
    # Salva os centroides (pesos dos neurônios) em CSV
    centroids = som.get_weights().reshape(-1, coords.shape[1])  # shape: (som_x*som_y, 2)
    output_centroids_neurons = os.path.join(cluster_output_dir, f'centroids_som_neurons_{output_prefix}.csv')
    df_neurons = pd.DataFrame(centroids, columns=['Longitude', 'Latitude']).reset_index().rename(columns={'index': 'Index'})
    df_neurons['Index'] = df_neurons['Index'] + 1  # começa por 1
    df_neurons.to_csv(output_centroids_neurons, index=False, header=None)
    print(f"Neuron weights saved to {output_centroids_neurons}")

    # Calcula os centroides reais dos clusters (média dos pontos atribuídos a cada neurônio)
    cluster_assignments = [som.winner(coord) for coord in coords]
    unique_neurons = list(set(cluster_assignments))
    centroids_real = []
    for neuron in unique_neurons:
        points = np.array([coords[i] for i in range(len(coords)) if cluster_assignments[i] == neuron])
        if len(points) > 0:
            centroids_real.append(points.mean(axis=0))
    centroids_real = np.array(centroids_real)

    # Salva os centroides reais dos clusters
    output_centroids_csv = os.path.join(cluster_output_dir, f'centroids_{som_x}_som_{output_prefix}.csv')
    df_centroids_real = pd.DataFrame(centroids_real, columns=['Longitude', 'Latitude']).reset_index().rename(columns={'index': 'Index'})
    df_centroids_real['Index'] = df_centroids_real['Index'] + 1  # começa por 1
    df_centroids_real.to_csv(output_centroids_csv, index=False, header=None)
    print(f"Centroids saved to {output_centroids_csv}")

    # Salvar mapeamento ponto -> centróide (id linear do neurônio)
    # calcula id linear do neurônio para cada ponto (0-based -> +1)
    labels = [ (n[0] * som_y + n[1]) + 1 for n in cluster_assignments ]
    df_points = data_cleaned.reset_index(drop=True).copy()
    df_map = pd.DataFrame({
        'id_centroid': labels,
        'id_animal': df_points.iloc[:, 0].values,
        'timestamp': df_points.iloc[:, 1].values,  # coluna 1 é o timestamp
        'latitude_animal': df_points.iloc[:, 3].values,
        'longitude_animal': df_points.iloc[:, 2].values
    })
    map_file = os.path.join(cluster_output_dir, f'points_som_mapping_{som_x}_{output_prefix}.csv')
    df_map.to_csv(map_file, index=False)
    print(f"Point->centroid mapping saved to {map_file}")

    # Metrics (Antigas comentadas)
    metrics_antigas = calculate_quality_metrics(df_points.iloc[:, 0].values, labels)

    erro_quantizacao = som.quantization_error(coords)
    erro_topologico = som.topographic_error(coords)

    # Novas métricas: Silhouette Score e Davies-Bouldin Index
    unique_labels = np.unique(labels)
    if len(unique_labels) > 1:
        silhouette = silhouette_score(coords, labels)
        dbi = davies_bouldin_score(coords, labels)
    else:
        silhouette = None
        dbi = None

    metrics = {
        "Purity": metrics_antigas['Purity'],
        "Entropy": metrics_antigas['Entropy'],
        "F-Measure": metrics_antigas['F-Measure'],
        "Partition Coefficient (PC)": metrics_antigas['PC'],
        "Quantization Error": erro_quantizacao,
        "Topographic Error": erro_topologico,
        "Silhouette Score": silhouette,
        "Davies-Bouldin Index": dbi
    }

    print("\n--- Resultados de Qualidade da Clusterização (Run All) ---")
    print(f"Purity:      {metrics_antigas['Purity']:.4f}")
    print(f"Entropy:     {metrics_antigas['Entropy']:.4f}")
    print(f"F-Measure:   {metrics_antigas['F-Measure']:.4f}")
    print(f"Partition Coeff (PC): {metrics_antigas['PC']:.4f}")
    print(f"Erro de Quantização: {erro_quantizacao:.4f}")
    print(f"Erro Topológico: {erro_topologico:.4f}")
    if silhouette is not None:
        print(f"Silhouette Score: {silhouette:.4f}")
        print(f"Davies-Bouldin Index: {dbi:.4f}")
    else:
        print("Silhouette Score: N/A (menos de 2 clusters)")
        print("Davies-Bouldin Index: N/A (menos de 2 clusters)")
    
    metrics['Algorithm'] = 'SOM'
    metrics_file = os.path.join(cluster_output_dir, f'Metricas_de_qualidade_{output_prefix}.csv')
    
    if os.path.exists(metrics_file):
        existing_df = pd.read_csv(metrics_file)
        new_df = pd.DataFrame([metrics])
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = pd.DataFrame([metrics])
        
    final_df.to_csv(metrics_file, index=False)
    print(f"Metrics saved to {metrics_file}")

    plot_quality_metrics_comparison(metrics_csv_path=os.path.join(cluster_output_dir, f'Metricas_de_qualidade_{output_prefix}.csv'), output_dir=cluster_output_dir, prefix=output_prefix, algorithms=['KMeans', 'SOM', 'BIRCH'], n_clusters=N_CLUSTERS_SOM)

    #plot_quality_metrics_local(silhouette, dbi, erro_quantizacao, cluster_output_dir, output_prefix, 'som')
    
    # Opcional: Salvar em arquivo txt também
    results_path_txt = os.path.join(cluster_output_dir, f'metrics_{output_prefix}.txt')
    with open(results_path_txt, "w") as f:
        for k, v in metrics.items():
            if k != 'Algorithm':
                f.write(f"{k}: {v}\n")

    # Plota e salva o gráfico
    plt.figure(figsize=(10, 6))
    plt.scatter(coords[:, 0], coords[:, 1], c=clusters, cmap='viridis', label='Data Points', alpha=0.7)

    # Adiciona centroides reais ao gráfico
    plt.scatter(centroids[:, 0], centroids[:, 1], color='red', marker='x', s=100, label='Centroids')

    plt.title(f"{lang['grafico_SOM_individual']} - Clusters: {len(centroids)} - Centroids: {len(centroids)} - {output_prefix.capitalize()}")
    plt.xlabel(lang["xlabel_SOM_individual"])
    plt.ylabel(lang["ylabel_SOM_individual"])
    plt.legend()
    plt.grid(True)

    output_png_path = os.path.join(cluster_output_dir, f'som_{output_prefix}.png')
    plt.savefig(output_png_path)
    plt.close()
    print(f"Plot saved to {output_png_path}")

def run(current_animal, file_rawdata_name):
    # CAMINHO DE ENTRADA (para ler os dados)
    input_results_dir = results_folder(file_rawdata_name)
    input_file_path = os.path.join(input_results_dir, f'map_{current_animal}.csv')

    # CAMINHO DE SAÍDA (para salvar os resultados)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_name = extract_folder_name(file_rawdata_name)
    results_dir = os.path.join(script_dir, '..', 'Results', folder_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
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

    # keep full slice so we still have id column for mapping
    data_selected_full = data_cleaned.iloc[100:108]
    data_selected = data_selected_full[[2, 3]]
    coords = data_selected.values

    if coords.shape[0] == 0:
        print(f"Error for animal {current_animal}: No valid coordinates left for clustering after slicing.")
        return

    # Salva as coordenadas usadas no diretório de saída correto
    output_csv_path = os.path.join(cluster_output_dir, f'clusters_som_{current_animal}.csv')
    df_out = data_selected.copy()
    df_out.insert(0, 'Index', range(1, len(df_out) + 1))  # começa por 1
    df_out.to_csv(output_csv_path, index=False, header=None)
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
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')
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
    plt.scatter(coords[:, 0], coords[:, 1], c=clusters, cmap='viridis', label='Data Points', alpha=0.7)
    
    plt.title(lang["grafico_SOM_individual"])
    plt.xlabel(lang["xlabel_SOM_individual"])
    plt.ylabel(lang["ylabel_SOM_individual"])
    plt.legend()
    plt.grid(True)

    output_png_path = os.path.join(cluster_output_dir, f'onca_{current_animal}_som.png')
    plt.savefig(output_png_path)
    plt.close() # Fecha a figura
    print(f"Plot saved to {output_png_path}")

    # salvar mapeamento ponto -> centróide para o slice usado
    winners = [som.winner(coord) for coord in coords]
    labels_slice = [ (w[0] * 8 + w[1]) + 1 for w in winners ]  # aqui usamos 8 pois som foi criado como 8x8 no modo run
    df_slice = data_selected_full.reset_index(drop=True).copy()
    df_map_slice = pd.DataFrame({
        'id_centroid': labels_slice,
        'id_animal': df_slice.iloc[:, 0].values,
        'timestamp': df_slice.iloc[:, 1].values,  # coluna 1 é o timestamp
        'latitude_animal': df_slice.iloc[:, 3].values,
        'longitude_animal': df_slice.iloc[:, 2].values
    })
    map_file_slice = os.path.join(cluster_output_dir, f'points_som_mapping_{current_animal}.csv')
    df_map_slice.to_csv(map_file_slice, index=False)
    print(f"Point->centroid mapping (slice) saved to {map_file_slice}")

    # INSERÇÃO DAS MÉTRICAS (Antigas comentadas)
    metrics_antigas = calculate_quality_metrics(df_map_slice['id_animal'], df_map_slice['id_centroid'])

    erro_quantizacao = som.quantization_error(coords)
    erro_topologico = som.topographic_error(coords)

    # Novas métricas: Silhouette Score e Davies-Bouldin Index
    unique_labels_slice = np.unique(df_map_slice['id_centroid'])
    if len(unique_labels_slice) > 1:
        silhouette_slice = silhouette_score(coords, df_map_slice['id_centroid'])
        dbi_slice = davies_bouldin_score(coords, df_map_slice['id_centroid'])
    else:
        silhouette_slice = None
        dbi_slice = None

    metrics = {
        "Purity": metrics_antigas['Purity'],
        "Entropy": metrics_antigas['Entropy'],
        "F-Measure": metrics_antigas['F-Measure'],
        "Partition Coefficient (PC)": metrics_antigas['PC'],
        "Quantization Error": erro_quantizacao,
        "Topographic Error": erro_topologico,
        "Silhouette Score": silhouette_slice,
        "Davies-Bouldin Index": dbi_slice
    }

    print("\n--- Resultados de Qualidade da Clusterização ---")
    print(f"Purity:      {metrics_antigas['Purity']:.4f}")
    print(f"Entropy:     {metrics_antigas['Entropy']:.4f}")
    print(f"F-Measure:   {metrics_antigas['F-Measure']:.4f}")
    print(f"Partition Coeff (PC): {metrics_antigas['PC']:.4f}")
    print(f"Erro de Quantização: {erro_quantizacao:.4f}")
    print(f"Erro Topológico: {erro_topologico:.4f}")
    if silhouette_slice is not None:
        print(f"Silhouette Score: {silhouette_slice:.4f}")
        print(f"Davies-Bouldin Index: {dbi_slice:.4f}")
    else:
        print("Silhouette Score: N/A (menos de 2 clusters)")
        print("Davies-Bouldin Index: N/A (menos de 2 clusters)")
    
    metrics['Algorithm'] = 'SOM'
    metrics_csv_path = os.path.join(cluster_output_dir, f'Metricas_de_qualidade_{current_animal}.csv')
    if os.path.exists(metrics_csv_path):
        existing_df = pd.read_csv(metrics_csv_path)
        final_df = pd.concat([existing_df, pd.DataFrame([metrics])], ignore_index=True)
    else:
        final_df = pd.DataFrame([metrics])
    final_df.to_csv(metrics_csv_path, index=False)
    print(f"Metrics saved to {metrics_csv_path}")
    plot_quality_metrics_local(silhouette_slice, dbi_slice, erro_quantizacao, cluster_output_dir, current_animal, 'som', n_clusters=len(unique_labels_slice))

    # Opcional: Salvar em arquivo
    results_path = os.path.join(cluster_output_dir, f'metrics_{current_animal}.txt')
    with open(results_path, "w") as f:
        for k, v in metrics.items():
            f.write(f"{k}: {v}\n")

def run_mock():
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    run(current_animal, file_rawdata_name)

# --- LEGACY CODE ---
# The following code was replaced to standardize the legend with kmeans style.

# In run_all():
#     for cluster_id in np.unique(clusters):
#         cluster_points = coords[clusters == cluster_id]
#         plt.scatter(cluster_points[:, 0], cluster_points[:, 1], label=f'Cluster {cluster_id + 1}', alpha=0.7)

# In run():
#     for cluster_id in np.unique(clusters):
#         cluster_points = coords[clusters == cluster_id]
#         plt.scatter(cluster_points[:, 0], cluster_points[:, 1], label=f'Centroíde {cluster_id}', alpha=0.7)