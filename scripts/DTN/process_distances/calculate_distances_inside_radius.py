import pandas as pd
import os
import glob

# --- CONFIGURAÇÕES ---
# Defina aqui a pasta onde estão os arquivos *_distances.csv gerados pelo script anterior
INPUT_FOLDER = 'scripts\\Results\\jaguar_mamiraua\\distances_to_own_centroid'
DISTANCE_THRESHOLD_METERS = 250
OUTPUT_FILE = f'scripts\\Results\\jaguar_mamiraua\\distances_to_own_centroid\\distances_inside_{DISTANCE_THRESHOLD_METERS}_meters.csv'

def extract_file_info(filename):
    """
    Extrai algoritmo e clusters do nome do arquivo para enriquecer os dados filtrados.
    """
    parts = filename.split('_')
    k = '?'
    algo = '?'
    # Lógica simples para identificar K e Algoritmo nos nomes padronizados
    for part in parts:
        if part.isdigit():
            k = part
        if part.lower() in ['kmeans', 'birch', 'som']:
            algo = part.lower()
    return k, algo

def filter_and_consolidate():
    if not os.path.exists(INPUT_FOLDER):
        print(f"Error: Input folder '{INPUT_FOLDER}' does not exist.")
        return

    # Procura todos os arquivos terminados em _distances.csv
    pattern = os.path.join(INPUT_FOLDER, '*_distances.csv')
    files = glob.glob(pattern)
    
    if not files:
        print("No '_distances.csv' files found in the folder.")
        return

    print(f"Found {len(files)} files.")
    print(f"Starting filtering for distances < {DISTANCE_THRESHOLD_METERS} meters...\n")
    
    filtered_dfs = []
    total_rows_read = 0
    
    for file_path in files:
        try:
            filename = os.path.basename(file_path)
            
            # Lê o arquivo
            df = pd.read_csv(file_path)
            total_rows_read += len(df)
            
            # Verifica se a coluna de distância existe
            if 'distance_meters' not in df.columns:
                print(f"WARNING: File {filename} has no 'distance_meters' column. Skipped.")
                continue
                
            # --- O FILTRO ACONTECE AQUI ---
            df_filtered = df[df['distance_meters'] < DISTANCE_THRESHOLD_METERS].copy()
            
            if not df_filtered.empty:
                # Extrai metadados do nome do arquivo para identificar a origem no consolidado
                k, algo = extract_file_info(filename)
                
                # Adiciona colunas identificadoras
                df_filtered['algorithm'] = algo
                df_filtered['clusters'] = k
                df_filtered['source_file'] = filename
                
                filtered_dfs.append(df_filtered)
                # print(f"-> {filename}: {len(df_filtered)} rows selected.")
            
        except Exception as e:
            print(f"Error processing {os.path.basename(file_path)}: {e}")

    # Consolidação Final
    if filtered_dfs:
        print("\nConsolidating data...")
        df_final = pd.concat(filtered_dfs, ignore_index=True)
        
        # Ordenar para facilitar leitura (usando nomes de coluna em inglês dos scripts anteriores)
        cols_order = [
            'algorithm', 'clusters', 'animal_id', 'distance_meters', 
            'timestamp', 'animal_lat', 'animal_lon', 
            'centroid_id', 'centroid_lat', 'centroid_lon', 'source_file'
        ]
        
        # Seleciona apenas colunas que existem no dataframe final
        existing_cols = [c for c in cols_order if c in df_final.columns]
        df_final = df_final[existing_cols]
        
        df_final.to_csv(OUTPUT_FILE, index=False)
        
        print("=" * 40)
        print("PROCESSING COMPLETE")
        print("=" * 40)
        print(f"Total rows analyzed: {total_rows_read}")
        print(f"Total rows filtered (kept): {len(df_final)}")
        print(f"File saved at: {OUTPUT_FILE}")
        
        # Mostra um resumo da distribuição dos dados filtrados
        print("\nSummary of records found per scenario:")
        print(df_final.groupby(['algorithm', 'clusters']).size().to_string())
        
    else:
        print(f"\nNo records found with distance less than {DISTANCE_THRESHOLD_METERS} meters in any file.")

if __name__ == "__main__":
    filter_and_consolidate()