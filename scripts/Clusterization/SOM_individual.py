import numpy as np
import pandas as pd
import sys
import matplotlib.pyplot as plt
import json
from minisom import MiniSom  # Import MiniSom for SOM
import os
from sklearn.metrics.cluster import contingency_matrix
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

# pip install minisom

# python3 -m venv venv
# source ./venv/bin/activate

# python3 7_SOM_individual.py 94

def run_all(file_rawdata_name, file_rawdata, output_prefix):
    #CAMINHO DE SAÍDA (para salvar os resultados)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_name = extract_folder_name(file_rawdata)
    results_dir = os.path.join(script_dir, '..', 'Results', folder_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
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
    som_x = read_field_from_json(hyperparam_path,"som_x")
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
    output_centroids_csv = os.path.join(cluster_output_dir, f'centroids_som_{output_prefix}.csv')
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
    map_file = os.path.join(cluster_output_dir, f'points_som_mapping_{output_prefix}.csv')
    df_map.to_csv(map_file, index=False)
    print(f"Point->centroid mapping saved to {map_file}")

    # Metrics (Antigas comentadas)
    metrics_antigas = calculate_quality_metrics(df_points.iloc[:, 0].values, labels)

    erro_quantizacao = som.quantization_error(coords)
    erro_topologico = som.topographic_error(coords)

    metrics = {
        "Purity": metrics_antigas['Purity'],
        "Entropy": metrics_antigas['Entropy'],
        "F-Measure": metrics_antigas['F-Measure'],
        "Partition Coefficient (PC)": metrics_antigas['PC'],
        "Quantization Error": erro_quantizacao,
        "Topographic Error": erro_topologico
    }

    print("\n--- Resultados de Qualidade da Clusterização (Run All) ---")
    print(f"Purity:      {metrics_antigas['Purity']:.4f}")
    print(f"Entropy:     {metrics_antigas['Entropy']:.4f}")
    print(f"F-Measure:   {metrics_antigas['F-Measure']:.4f}")
    print(f"Partition Coeff (PC): {metrics_antigas['PC']:.4f}")
    print(f"Erro de Quantização: {erro_quantizacao:.4f}")
    print(f"Erro Topológico: {erro_topologico:.4f}")
    
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

    metrias = {
        "Purity": metrics_antigas['Purity'],
        "Entropy": metrics_antigas['Entropy'],
        "F-Measure": metrics_antigas['F-Measure'],
        "Partition Coefficient (PC)": metrics_antigas['PC'],
        "Quantization Error": erro_quantizacao,
        "Topographic Error": erro_topologico
    }

    print("\n--- Resultados de Qualidade da Clusterização ---")
    print(f"Purity:      {metrics_antigas['Purity']:.4f}")
    print(f"Entropy:     {metrics_antigas['Entropy']:.4f}")
    print(f"F-Measure:   {metrics_antigas['F-Measure']:.4f}")
    print(f"Partition Coeff (PC): {metrics_antigas['PC']:.4f}")
    print(f"Erro de Quantização: {erro_quantizacao:.4f}")
    print(f"Erro Topológico: {erro_topologico:.4f}")
    
    # Opcional: Salvar em arquivo
    results_path = os.path.join(cluster_output_dir, f'metrics_{current_animal}.txt')
    with open(results_path, "w") as f:
        for k, v in metrias.items():
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