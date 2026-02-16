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
    limit_distance_m = 400
    
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
        for mode in PROCESSING_MODES:
            
            cluster_file_name = f'centroids_{clustering_type}_{mode}.csv'
            cluster_file_path = os.path.join(cluster_dir, cluster_file_name)

            # Condição caso as interpolações ainda não existam
            if not os.path.exists(cluster_file_path):
                if mode != 'rawdata':
                    print(f"[i] Pulando {mode}: resultados de interpolação não encontrados.")
                else:
                    print(f"[-] Atenção: Arquivo base {cluster_file_name} não encontrado.")
                continue

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

                # Nome do arquivo de saída com a distância como inteiro
                output_file_name = f"contacts_{animal_name}_{clustering_type}_{mode}_{limit_distance_m}m.csv"
                output_path = os.path.join(dtn_dir, output_file_name)
                
                output_df.to_csv(output_path, index=False)
                print(f"    -> Sucesso: {len(contacts_list)} contatos salvos em {output_file_name}")
            else:
                print(f"    -> Info: Nenhum contato encontrado para {clustering_type} + {mode}.")

if __name__ == "__main__":
    # Exemplo de uso:
    # run("jaguar_01", "path/to/jaguar_01.csv")
    pass