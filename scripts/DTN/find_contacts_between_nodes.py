import os
import sys
import pandas as pd
from geopy.distance import geodesic
from datetime import datetime
from Common.utils import results_folder

DISTANCE_GOAL_KM = 250 # Distância de alcance entre uma onça e outra?
BASE_DATE = datetime(2014, 3, 14, 4, 0)

# ids dos centroids [0-7, 8-16, 8-24, 8-40]

def run(first_animal, second_animal, file_rawdata_name):
    results_dir = results_folder(file_rawdata_name)
    path_1 = os.path.join(results_dir, f'map_{first_animal}.csv')
    path_2 = os.path.join(results_dir, f'map_{second_animal}.csv')
    path_out = os.path.join(results_dir, 'contacts', f'contact_{first_animal}_{second_animal}.csv')

    print(f"Processando par {first_animal} e {second_animal}...")

    # --- Carregamento e Limpeza ---
    cols = ['animal_id', 'timestamp', 'lon', 'lat']
    try:
        df1 = pd.read_csv(path_1, header=None, names=cols, usecols=['timestamp', 'lon', 'lat'])
        df2 = pd.read_csv(path_2, header=None, names=cols, usecols=['timestamp', 'lon', 'lat'])
    except FileNotFoundError:
        print(f"Arquivos de mapa não encontrados para {first_animal} ou {second_animal}. Pulando.")
        return

    df1['timestamp'] = pd.to_datetime(df1['timestamp'], format="%m/%d/%y %H:%M", errors='coerce')
    df2['timestamp'] = pd.to_datetime(df2['timestamp'], format="%m/%d/%y %H:%M", errors='coerce')

    clean_dfs = [] 
    for df in [df1, df2]:
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        df.dropna(subset=['timestamp', 'lon', 'lat'], inplace=True)
        # Filtro de validade GPS
        df = df[(df['lat'] >= -90) & (df['lat'] <= 90) & (df['lon'] >= -180) & (df['lon'] <= 180)]
        clean_dfs.append(df)
    
    df1, df2 = clean_dfs[0], clean_dfs[1]

    # --- Cruzamento e Distância ---
    merged = pd.merge(df1, df2, on='timestamp', suffixes=('_1', '_2'))
    if merged.empty:
        print("Nenhum horário coincidente.")
        return

    def calculate_distance(row):
        return geodesic((row['lat_1'], row['lon_1']), (row['lat_2'], row['lon_2'])).meters

    merged['distance_m'] = merged.apply(calculate_distance, axis=1)
    contacts = merged[merged['distance_m'] < DISTANCE_GOAL_KM].copy()

    if contacts.empty:
        print(f"Nenhum contato < {DISTANCE_GOAL_KM}m.")
        return

    # --- Preparação Final ---
    contacts['id'] = (contacts['timestamp'] - BASE_DATE).dt.total_seconds()
    contacts['id'] = contacts['id'].astype(int)
    contacts['conn'] = 'CONN'
    contacts['for'] = first_animal
    contacts['to'] = second_animal
    contacts['state'] = 'up'

    # Salvar CSV (Isso é o que o Script 2 vai ler)
    cols_to_save = ['id', 'conn', 'for', 'to', 'state']
    contacts[cols_to_save].to_csv(path_out, index=False)
    print(f"CSV gerado: {path_out}")
