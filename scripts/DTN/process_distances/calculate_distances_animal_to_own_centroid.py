import pandas as pd
from geopy.distance import geodesic
import os
import re

# --- CONFIGURAÇÕES ---
INPUT_FOLDER = 'scripts\\Results\\jaguar_mamiraua'
OUTPUT_FOLDER = 'scripts\\Results\\jaguar_mamiraua\\distances_to_own_centroid'
SUMMARY_FILE_NAME = 'stats_animal_to_own_centroid.csv'

def normalize_id(series):
    """
    Padroniza IDs para remover diferenças entre '1', '1.0' e 1.
    """
    numerics = pd.to_numeric(series, errors='coerce')
    integers = numerics.fillna(-1).astype(int)
    strings = integers.astype(str).str.strip()
    return strings

def extract_file_info(filename):
    parts = filename.split('_')
    k = '?'
    algo = '?'
    for part in parts:
        if part.isdigit():
            k = part
        if part.lower() in ['kmeans', 'birch', 'som']:
            algo = part.lower()
    return k, algo

def find_pairs(folder):
    if not os.path.exists(folder):
        print(f"ERROR: The folder {folder} does not exist.")
        return []

    files = os.listdir(folder)
    
    # --- FILTRO MAIS RIGOROSO ---
    # Só aceita arquivos que terminam EXATAMENTE em '_rawdata.csv'
    # Isso evita pegar 'Copy of...', '(1).csv' ou arquivos processados anteriormente
    
    centroid_files = [
        f for f in files 
        if f.startswith('centroids_') 
        and f.endswith('_bilstm.csv') 
        and 'distances' not in f 
    ]
    
    point_files = [
        f for f in files 
        if 'points' in f 
        and 'mapping' in f 
        and f.endswith('_bilstm.csv') 
        and 'distances' not in f
    ]
    
    pairs = []
    print(f"Valid files identified: {len(centroid_files)} centroids and {len(point_files)} points.")
    
    # Verifica duplicatas de lógica (Ex: dois arquivos para k=8, algo=kmeans)
    processed_keys = set()

    for c_file in centroid_files:
        k_c, algo_c = extract_file_info(c_file)
        key = (k_c, algo_c)
        
        # Evita processar a mesma combinação duas vezes se houver arquivos com nomes parecidos
        if key in processed_keys:
            print(f"Ignoring duplicate for: {algo_c}-{k_c} ({c_file})")
            continue

        match = None
        for p_file in point_files:
            k_p, algo_p = extract_file_info(p_file)
            if k_c == k_p and algo_c == algo_p and k_c != '?' and algo_c != '?':
                match = p_file
                break
        
        if match:
            pairs.append((os.path.join(folder, c_file), os.path.join(folder, match)))
            processed_keys.add(key)
        else:
            print(f"WARNING: Pair not found for centroid {c_file}")
            
    return pairs

def calculate_distances(centroids_path, points_path, dest_folder):
    points_filename = os.path.basename(points_path)
    
    # 1. Carregar Centroides
    try:
        df_centroids = pd.read_csv(centroids_path, header=None, names=['centroid_id', 'centroid_lon', 'centroid_lat'])
        df_centroids['centroid_lat'] = pd.to_numeric(df_centroids['centroid_lat'], errors='coerce')
        df_centroids['centroid_lon'] = pd.to_numeric(df_centroids['centroid_lon'], errors='coerce')
        df_centroids = df_centroids.dropna()
        df_centroids['centroid_id'] = normalize_id(df_centroids['centroid_id'])
    except Exception as e:
        print(f"Error reading {os.path.basename(centroids_path)}: {e}")
        return None

    # 2. Carregar Pontos
    try:
        df_points = pd.read_csv(points_path)
        total_rows = len(df_points)
        
        # Renomeando colunas para manter consistência em inglês
        # Assumindo que o CSV original tem 'latitude_animal' e 'longitude_animal'
        if 'latitude_animal' in df_points.columns:
            df_points.rename(columns={'latitude_animal': 'animal_lat', 'longitude_animal': 'animal_lon'}, inplace=True)
            
        df_points['animal_lat'] = pd.to_numeric(df_points['animal_lat'], errors='coerce')
        df_points['animal_lon'] = pd.to_numeric(df_points['animal_lon'], errors='coerce')
        
        # Filtro Lat/Lon
        df_points = df_points.dropna(subset=['animal_lat', 'animal_lon'])
        df_points = df_points[
            (df_points['animal_lat'] >= -90) & (df_points['animal_lat'] <= 90) &
            (df_points['animal_lon'] >= -180) & (df_points['animal_lon'] <= 180)
        ]
        
        # Mapeando coluna de ID do cluster no arquivo de pontos (normalmente 'id_centroid' ou similar)
        if 'id_centroid' in df_points.columns:
            df_points.rename(columns={'id_centroid': 'centroid_id'}, inplace=True)
            
        df_points['centroid_id'] = normalize_id(df_points['centroid_id'])
        
    except Exception as e:
        print(f"Error reading {points_filename}: {e}")
        return None

    # 3. Merge
    df_merged = pd.merge(df_points, df_centroids, on='centroid_id', how='left')

    # 4. Calcular Distâncias
    def get_dist(row):
        if pd.isna(row['centroid_lat']): return None
        try:
            return geodesic((row['animal_lat'], row['animal_lon']), 
                            (row['centroid_lat'], row['centroid_lon'])).meters
        except: return None

    # Otimização: calcula apenas onde temos dados de centroide
    valid_mask = df_merged['centroid_lat'].notna()
    df_merged.loc[valid_mask, 'distance_meters'] = df_merged[valid_mask].apply(get_dist, axis=1)
    
    # Estatísticas
    k, algo = extract_file_info(os.path.basename(centroids_path))
    valid_distances = df_merged['distance_meters'].dropna()
    
    stats = {
        'points_file': points_filename,
        'algorithm': algo,
        'clusters': k,
        'mean': valid_distances.mean() if len(valid_distances) > 0 else 0,
        'min': valid_distances.min() if len(valid_distances) > 0 else 0,
        'max': valid_distances.max() if len(valid_distances) > 0 else 0,
        'std': valid_distances.std() if len(valid_distances) > 0 else 0,
        'valid_points': len(valid_distances),
        'data_loss': total_rows - len(valid_distances)
    }

    # Salvar
    output_name = points_filename.replace('.csv', '_distances.csv')
    output_path = os.path.join(dest_folder, output_name)
    
    cols = ['animal_id', 'timestamp', 'animal_lat', 'animal_lon', 
            'centroid_id', 'centroid_lat', 'centroid_lon', 'distance_meters']
    
    # Garante que só selecionamos colunas que existem no dataframe
    existing_cols = [c for c in cols if c in df_merged.columns]
    
    df_merged[existing_cols].to_csv(output_path, index=False)
    
    print(f"-> Processed: {algo}-{k} | Valid: {len(valid_distances)} | Saved: {output_name}")
    return stats

# --- MAIN ---
if __name__ == "__main__":
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        
    print(f"Starting in: {INPUT_FOLDER}")
    pairs = find_pairs(INPUT_FOLDER)
    
    statistics_list = []
    
    if not pairs:
        print("No pairs found. Check if files end with '_bilstm.csv'.")
    else:
        for c_file, p_file in pairs:
            res = calculate_distances(c_file, p_file, OUTPUT_FOLDER)
            if res:
                statistics_list.append(res)
                
    if statistics_list:
        df_res = pd.DataFrame(statistics_list).sort_values(by=['algorithm', 'clusters'])
        summary_path = os.path.join(OUTPUT_FOLDER, SUMMARY_FILE_NAME)
        df_res.to_csv(summary_path, index=False)
        print(f"\nSummary saved at: {summary_path}")
        # Mostra prévia limpa
        print(df_res[['algorithm', 'clusters', 'mean', 'valid_points']].to_string(index=False))