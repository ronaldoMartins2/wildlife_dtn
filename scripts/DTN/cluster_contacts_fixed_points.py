import pandas as pd
from geopy.distance import geodesic
import os
import sys

from Common.utils import (
    results_folder,
    read_field_from_json
)

CLUSTERING_TYPES = ['som', 'birch', 'kmeans']
PROCESSING_MODES = ['nbeats', 'nhits', 'rawdata']

def run(current_animal, file_rawdata_name):
    limit_distance_m = 250
    
    # Extração do nome do animal e definição de caminhos
    animal_name = os.path.splitext(os.path.basename(file_rawdata_name))[0]
    results_dir = results_folder(file_rawdata_name)
    cluster_dir = results_dir # os.path.join(results_dir, 'Clusterization')

    print(f"\n=== Iniciando Processamento: {animal_name} ===")
    print(f"Raio de contato: {limit_distance_m}m")

    # 1. Verificação da pasta de clusters
    if not os.path.exists(cluster_dir):
        print(f"ERRO: Pasta de clusters não encontrada: {cluster_dir}")
        return

    # 2. Leitura dos dados de movimentação (GPS)
    csv_path = os.path.join(results_dir, f'map_{animal_name}_all_animals.csv')
    
    if not os.path.exists(csv_path):
        print(f"ERRO: Arquivo de movimentação não encontrado: {csv_path}")
        return

    # Carregando o DataFrame de movimentação
    movement_df = pd.read_csv(csv_path, header=None, names=['id', 'timestamp', 'longitude', 'latitude'])

    # 3. Iteração por tipo de algoritmo e modo de processamento
    for clustering_type in CLUSTERING_TYPES:
        for interpolation_type in INTERPOLATION_TYPES:
            
            #csv_path = os.path.join(results_dir, f'map_interpolation_merged_{tangara}_{interpolation_type}.csv')

            #if os.path.exists(csv_path):
            #        df = pd.read_csv( csv_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
            #else:
            #    print(f"Arquivo não encontrado: {csv_path}")
            #    return

            print(f"Caminho do CSV={csv_path}")
            arquivo_clusters = os.path.join(results_dir, 'Clusterization', f'centroids_{clustering_type}_{interpolation_type}.csv')
            if os.path.exists(arquivo_clusters):
                df_clusters = pd.read_csv(arquivo_clusters, header=None)
            else:
                print(f"Arquivo não encontrado: {arquivo_clusters}")
                return

            print(f"[+] Calculando contatos: {clustering_type} | Modo: {mode}")
            
            try:
                cluster_df = pd.read_csv(cluster_file_path, header=None, names=['cluster_id', 'longitude', 'latitude'])
            except Exception as e:
                print(f"[-] Erro ao ler {cluster_file_name}: {e}")
                continue

            contacts_list = []

            # --- Processamento Geográfico ---
            for _, animal_point in movement_df.iterrows():
                animal_coords = (animal_point['latitude'], animal_point['longitude'])
                
                for _, cluster_point in cluster_df.iterrows():
                    cluster_coords = (cluster_point['latitude'], cluster_point['longitude'])
                    
                    try:
                        # Cálculo da distância geodésica
                        distance = geodesic(animal_coords, cluster_coords).meters
                        
                        if distance <= limit_distance_m:
                            contacts_list.append({
                                'timestamp': animal_point['timestamp'],
                                'animal_id': animal_point['id'],
                                'lat_animal': animal_coords[0],
                                'lon_animal': animal_coords[1],
                                'lat_cluster': cluster_coords[0],
                                'lon_cluster': cluster_coords[1],
                                'cluster_id': cluster_point['cluster_id'],
                                'distance_m': round(distance, 2)
                            })
                    except ValueError:
                        continue

            # --- Salvamento dos Arquivos de Saída ---
            if contacts_list:
                output_df = pd.DataFrame(contacts_list)
                dtn_dir = os.path.join(results_dir, 'DTN')
                os.makedirs(dtn_dir, exist_ok=True)

            nome_saida = f"contatos_{os.path.splitext(os.path.basename(csv_path))[0]}_" \
                        f"{os.path.splitext(os.path.basename(arquivo_clusters))[0]}_{distancia_limite_m}.csv"
            
            # Salvar na pasta DTN
            output_path = os.path.join(dtn_dir, nome_saida)
            df_contatos.to_csv(output_path, index=False)

            print(f"Arquivo gerado: {output_path}")

def uniplemented(current_animal, file_rawdata_name, output_prefix):

    # --- Parâmetros ---
    results_dir = results_folder( file_rawdata_name )

    data_prep_dir = os.path.join(results_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    distancia_limite_m = read_field_from_json(hyperparam_path, "distancia_limite_contatos")
    print(f"Distância limite de contatos: {distancia_limite_m} metros")

    # --- Arquivos de entrada ---
    #arquivo_onca = sys.argv[1]
    #arquivo_clusters = sys.argv[2]

    csv_path = os.path.join(results_dir, f'map_{current_animal}.csv')
    df = pd.read_csv( csv_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    arquivo_clusters = os.path.join(results_dir, 'Clusterization', f'clusters_som_{output_prefix}.csv')

    # --- Leitura e padronização ---
    #df_onca = pd.read_csv(arquivo_onca, header=None)
    df_onca = df

    df_clusters = pd.read_csv(arquivo_clusters, header=None)

    #df_onca.columns = ['id', 'timestamp', 'longitude', 'latitude']
    df_clusters.columns = ['cluster_id','longitude', 'latitude']

    # --- Processar contatos ---
    contatos = []

    for _, ponto_onca in df_onca.iterrows():
        coord_onca = (ponto_onca['latitude'], ponto_onca['longitude'])
        for _, cluster in df_clusters.iterrows():
            coord_cluster = (cluster['latitude'], cluster['longitude'])
            try:
                distancia = geodesic(coord_onca, coord_cluster).meters
                if distancia <= distancia_limite_m:
                    contatos.append({
                        'timestamp': ponto_onca['timestamp'],
                        'lat_onca': coord_onca[0],
                        'lon_onca': coord_onca[1],
                        'lat_cluster': coord_cluster[0],
                        'lon_cluster': coord_cluster[1],
                        'cluster_id': cluster['cluster_id'],
                        'distancia_m': round(distancia, 2)
                    })
            except ValueError:
                continue

    # --- Salvar resultado ---
    df_contatos = pd.DataFrame(contatos)

    nome_saida = f"contatos_{os.path.splitext(os.path.basename(csv_path))[0]}_" \
                f"{os.path.splitext(os.path.basename(arquivo_clusters))[0]}_{distancia_limite_m}.csv"
    df_contatos.to_csv(nome_saida, index=False)

    print(f"Arquivo gerado: {nome_saida}")

