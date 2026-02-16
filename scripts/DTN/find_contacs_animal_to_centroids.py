import os
import pandas as pd
import numpy as np
from geopy.distance import geodesic
from datetime import datetime
from Common.utils import results_folder

# --- CONFIGURAÇÕES ---
LIMIT_DISTANCE_M = int(250) 
BASE_DATE = datetime(2014, 3, 14, 4, 0) #
ANIMAL_OFFSET = 93 #
CENTROID_START_OFFSET = 7 #

CLUSTERING_TYPES = ['som', 'birch', 'kmeans']
PROCESSING_MODES = ['nbeats', 'nhits', 'rawdata']
CLUSTER_COUNTS = [8, 16, 32]

def run(animal_id_str, file_rawdata_name):
    """
    animal_id_str: ID da onça (ex: '93')
    file_rawdata_name: nome do arquivo original
    """
    results_dir = results_folder(file_rawdata_name)
    cluster_dir = results_dir #os.path.join(results_dir, 'Clusterization')
    
    # Busca o arquivo de movimentação individual
    animal_path = os.path.join(results_dir, f'map_{animal_id_str}.csv') # os.path.join(results_dir, f'map_{animal_id_str}_all_animals.csv')
    
    if not os.path.exists(animal_path):
        print(f"[-] Arquivo de movimentação não encontrado para o animal {animal_id_str} em: {animal_path}")
        return

    try:
        mapped_id = int(animal_id_str) - ANIMAL_OFFSET
        print(f"\n>>> Processando Animal: {animal_id_str} (ID Rede: {mapped_id})")
    except ValueError:
        print(f"[-] Erro: '{animal_id_str}' não é um ID de animal válido para conversão.")
        return

    # 1. Carregamento do GPS do animal
    try:
        # Formato de data fixo para evitar warnings e erros de cálculo
        df_animal = pd.read_csv(animal_path, header=None, names=['animal_id', 'timestamp', 'lon', 'lat'])
        df_animal['timestamp'] = pd.to_datetime(df_animal['timestamp'], format="%m/%d/%y %H:%M", errors='coerce')
        df_animal['lat'] = pd.to_numeric(df_animal['lat'], errors='coerce')
        df_animal['lon'] = pd.to_numeric(df_animal['lon'], errors='coerce')
        df_animal.dropna(subset=['timestamp', 'lon', 'lat'], inplace=True)
        print(f"    [OK] Dados de GPS carregados: {len(df_animal)} pontos válidos.")
    except Exception as e:
        print(f"    [!] Erro ao ler coordenadas do animal {animal_id_str}: {e}")
        return

    # 2. Iteração pelos arquivos de cluster
    for clustering_type in CLUSTERING_TYPES:
        for mode in PROCESSING_MODES:
            # Lista arquivos possíveis de centroids
            possible_files = [f'centroids_{clustering_type}_{mode}.csv']
            for count in CLUSTER_COUNTS:
                possible_files.append(f'centroids_{count}_{clustering_type}_{mode}.csv')
            
            for cluster_filename in possible_files:
                cluster_path = os.path.join(cluster_dir, cluster_filename)
                
                if not os.path.exists(cluster_path):
                    continue

                print(f"    [+] Comparando com: {cluster_filename}...", end='\r')
                
                try:
                    df_clusters = pd.read_csv(cluster_path, header=None, names=['cluster_id', 'lon', 'lat'])
                except Exception:
                    continue

                contacts_list = []

                # 3. Cálculo Geográfico (Latitude, Longitude)
                for _, p_animal in df_animal.iterrows():
                    for _, p_cluster in df_clusters.iterrows():
                        try:
                            # Ordem correta para o Geopy: (Lat, Lon)
                            dist = geodesic((p_animal['lat'], p_animal['lon']), 
                                           (p_cluster['lat'], p_cluster['lon'])).meters
                            
                            if dist <= LIMIT_DISTANCE_M:
                                contacts_list.append({
                                    'timestamp': p_animal['timestamp'],
                                    'cluster_id': p_cluster['cluster_id']
                                })
                        except Exception:
                            continue

                if not contacts_list:
                    continue

                print(f"    [*] Encontrados {len(contacts_list)} contatos geográficos com {cluster_filename}")

                # 4. Formatação e Mapeamento de IDs para o simulador
                df_res = pd.DataFrame(contacts_list)
                # Cálculo do tempo em segundos desde BASE_DATE
                df_res['id'] = (df_res['timestamp'] - BASE_DATE).dt.total_seconds().astype(int)
                df_res['conn'] = 'CONN'
                df_res['for'] = mapped_id
                # Mapeamento do Centroide (ID + Offset)
                df_res['to'] = df_res['cluster_id'].astype(int) + CENTROID_START_OFFSET
                df_res['state'] = 'up'

                # 5. Lógica de Eventos DOWN - 13.8h - 49680s (raw_data)
                df_down = df_res.copy()
                df_down['id'] = df_down['id'] + 49680
                df_down['state'] = 'down'

                final_df = pd.concat([df_res, df_down], ignore_index=True)
                final_df.sort_values(by=['id', 'state'], ascending=[True, False], inplace=True)

                # 6. Salvamento
                out_dir = os.path.join(results_dir, 'contacts')
                os.makedirs(out_dir, exist_ok=True)
                
                output_name = f"down_contact_{animal_id_str}_{os.path.splitext(cluster_filename)[0]}.csv"
                output_path = os.path.join(out_dir, output_name)
                
                final_df[['id', 'conn', 'for', 'to', 'state']].to_csv(output_path, index=False)
                print(f"    [SALVO] {output_name}")

if __name__ == "__main__":
    pass