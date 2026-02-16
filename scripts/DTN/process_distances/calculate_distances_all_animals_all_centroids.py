import pandas as pd
from geopy.distance import geodesic
import glob
import os
import time

# --- CONFIGURAÇÕES ---
ANIMAL_FILE = 'scripts\\Results\\jaguar_mamiraua\\map_jaguar_mamiraua_all_animals.csv'
CENTROID_PATTERN = 'scripts\\Results\\jaguar_mamiraua\\centroids_*_rawdata.csv'
OUTPUT_FOLDER = 'scripts\\Results\\jaguar_mamiraua\\distances_all_to_all'
SUMMARY_FILE = 'scripts\\Results\\jaguar_mamiraua\\distances_all_to_all\\stats_all_to_all.csv' # Nome do arquivo de resumo

def process_clustering_scenarios():
    
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    # Lista para armazenar as estatísticas de cada arquivo
    statistics_list = []

    # --- 1. CARREGAR E LIMPAR DADOS DOS ANIMAIS ---
    print(f"Loading animal data: {ANIMAL_FILE}")
    try:
        # Carrega assumindo 4 colunas: ID, Timestamp, Long, Lat
        df_animals = pd.read_csv(
            ANIMAL_FILE, 
            header=None, 
            names=['animal_id', 'timestamp', 'animal_lon', 'animal_lat'],
            dtype={'animal_id': str}
        )
        
        initial_total = len(df_animals)
        
        # Limpeza e validação
        df_animals['animal_lat'] = pd.to_numeric(df_animals['animal_lat'], errors='coerce')
        df_animals['animal_lon'] = pd.to_numeric(df_animals['animal_lon'], errors='coerce')
        
        # Filtra coordenadas válidas
        df_clean = df_animals.dropna(subset=['animal_lat', 'animal_lon'])
        df_clean = df_clean[
            (df_clean['animal_lat'] >= -90) & (df_clean['animal_lat'] <= 90) &
            (df_clean['animal_lon'] >= -180) & (df_clean['animal_lon'] <= 180)
        ].copy()
        
        removed_count = initial_total - len(df_clean)
        if removed_count > 0:
            print(f"WARNING: {removed_count} invalid rows removed from animals.")
        
        df_clean['_key'] = 1

    except Exception as e:
        print(f"Fatal error reading animal file: {e}")
        return

    # --- 2. ENCONTRAR ARQUIVOS DE CENTROIDES ---
    centroid_files = glob.glob(CENTROID_PATTERN)
    print(f"Found {len(centroid_files)} centroid files.")
    
    if len(centroid_files) == 0:
        print("No centroid file found.")
        return

    # --- 3. LOOP DE PROCESSAMENTO ---
    for cent_file in centroid_files:
        start_time = time.time()
        print(f"\nProcessing: {cent_file} ...")
        
        try:
            # Extrair info do nome do arquivo (Ex: centroids_8_kmeans_rawdata.csv)
            # Divide por '_' -> ['centroids', '8', 'kmeans', 'rawdata.csv']
            name_parts = os.path.basename(cent_file).split('_')
            
            # Tenta pegar K e Algo se o nome seguir o padrão exato
            try:
                num_clusters = name_parts[1]
                algorithm = name_parts[2]
            except IndexError:
                num_clusters = 'unknown'
                algorithm = 'unknown'

            # Ler Centroides
            df_cent = pd.read_csv(
                cent_file, 
                header=None, 
                names=['centroid_id', 'centroid_lon', 'centroid_lat']
            )
            
            # Limpeza dos centroides
            df_cent['centroid_lat'] = pd.to_numeric(df_cent['centroid_lat'], errors='coerce')
            df_cent['centroid_lon'] = pd.to_numeric(df_cent['centroid_lon'], errors='coerce')
            df_cent = df_cent.dropna()
            df_cent['_key'] = 1
            
            # Merge (Produto Cartesiano)
            df_merged = pd.merge(df_clean, df_cent, on='_key').drop('_key', axis=1)
            
            # Função de Distância
            def calculate_distance(row):
                try:
                    return geodesic(
                        (row['animal_lat'], row['animal_lon']),
                        (row['centroid_lat'], row['centroid_lon'])
                    ).meters
                except ValueError:
                    return None

            df_merged['distance_meters'] = df_merged.apply(calculate_distance, axis=1)
            
            # --- CALCULAR ESTATÍSTICAS ---
            # Remove falhas de calculo antes de estatística
            valid_distances = df_merged['distance_meters'].dropna()
            
            if not valid_distances.empty:
                dist_min = valid_distances.min()
                dist_max = valid_distances.max()
                dist_mean = valid_distances.mean()
                dist_std = valid_distances.std() # Desvio padrão (opcional, útil)
            else:
                dist_min = dist_max = dist_mean = dist_std = 0

            # Adiciona ao dicionário de resumo
            statistics_list.append({
                'source_file': os.path.basename(cent_file),
                'algorithm': algorithm,
                'num_clusters': num_clusters,
                'min_distance_m': dist_min,
                'max_distance_m': dist_max,
                'mean_distance_m': dist_mean,
                'std_deviation_m': dist_std
            })

            # --- SALVAR ARQUIVO INDIVIDUAL ---
            base_name = os.path.basename(cent_file).replace('centroids_', '').replace('_rawdata.csv', '')
            output_name = os.path.join(OUTPUT_FOLDER, f"distances_{base_name}.csv")
            
            final_columns = [
                'animal_id', 'timestamp', 'animal_lat', 'animal_lon',
                'centroid_id', 'centroid_lat', 'centroid_lon', 'distance_meters'
            ]
            
            df_merged[final_columns].to_csv(output_name, index=False)
            print(f"-> Saved detailed: {output_name} ({time.time() - start_time:.1f}s)")
            
        except Exception as e:
            print(f"ERROR in file {cent_file}: {e}")

    # --- 4. SALVAR RESUMO FINAL ---
    if statistics_list:
        df_summary = pd.DataFrame(statistics_list)
        df_summary.to_csv(SUMMARY_FILE, index=False)
        print(f"\nStatistical summary saved successfully at: {SUMMARY_FILE}")
        print(df_summary[['algorithm', 'num_clusters', 'mean_distance_m']]) # Mostra prévia
    else:
        print("\nNo statistics generated.")

if __name__ == "__main__":
    process_clustering_scenarios()