import os
import pandas as pd
from geopy.distance import geodesic
import sys

from Common.utils import (
    create_clusterization_results,
    append_variables_to_file,
    results_folder,
    read_field_from_json
)

def detect_datetime_format(datetime_str):
    """
    Detecta automaticamente o formato da data a partir de uma string de exemplo.
    """
    formats = [
        '%Y-%m-%d %H:%M:%S',   # 2014-03-14 04:00:00
        '%Y-%m-%d %H:%M',      # 2014-03-14 04:00
        '%m/%d/%y %H:%M',      # 03/14/14 04:00
        '%m/%d/%Y %H:%M:%S',   # 03/14/2014 04:00:00
        '%d/%m/%y %H:%M',      # 14/03/14 04:00
        '%d-%m-%Y %H:%M:%S',   # 14-03-2014 04:00:00
    ]
    
    for fmt in formats:
        try:
            pd.to_datetime(datetime_str, format=fmt)
            return fmt
        except:
            continue
    
    return None

def process_files(file1, file2, file_number_onca1, file_number_onca2, file_rawdata_name):
    suffix1 = f'__{file_number_onca1}'
    suffix2 = f'__{file_number_onca2}'

    results_dir = results_folder(file_rawdata_name)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    # Read CONTACT_DISTANCE from hyperparameters
    CONTACT_DISTANCE = read_field_from_json(hyperparam_path, 'distancia_limite_contatos')
    try:
        CONTACT_DISTANCE = float(CONTACT_DISTANCE)
    except Exception:
        CONTACT_DISTANCE = 400.0

    # Load data
    df1 = pd.read_csv(file1, header=None, names=['ID', 'Datetime', 'Longitude', 'Latitude'])
    df2 = pd.read_csv(file2, header=None, names=['ID', 'Datetime', 'Longitude', 'Latitude'])

    print(f"\nProcessando {file1}")
    print(f"Exemplo df1 Datetime: {df1['Datetime'].iloc[0] if len(df1) > 0 else 'vazio'}")
    print(f"\nProcessando {file2}")
    print(f"Exemplo df2 Datetime: {df2['Datetime'].iloc[0] if len(df2) > 0 else 'vazio'}")

    # Detectar formato de data automaticamente
    fmt1 = detect_datetime_format(str(df1['Datetime'].iloc[0])) if len(df1) > 0 else None
    fmt2 = detect_datetime_format(str(df2['Datetime'].iloc[0])) if len(df2) > 0 else None

    print(f"Formato detectado df1: {fmt1}")
    print(f"Formato detectado df2: {fmt2}")

    # Convert to datetime com formato detectado
    if fmt1:
        df1['Datetime'] = pd.to_datetime(df1['Datetime'], format=fmt1, errors='coerce')
    else:
        df1['Datetime'] = pd.to_datetime(df1['Datetime'], errors='coerce')

    if fmt2:
        df2['Datetime'] = pd.to_datetime(df2['Datetime'], format=fmt2, errors='coerce')
    else:
        df2['Datetime'] = pd.to_datetime(df2['Datetime'], errors='coerce')

    # Debug: check for NaT values
    nat_count_df1 = df1['Datetime'].isna().sum()
    nat_count_df2 = df2['Datetime'].isna().sum()
    print(f"df1: {len(df1)} rows, {nat_count_df1} NaT values")
    print(f"df2: {len(df2)} rows, {nat_count_df2} NaT values")
    
    # Drop rows with NaT in Datetime
    df1 = df1.dropna(subset=['Datetime'])
    df2 = df2.dropna(subset=['Datetime'])

    print(f"After dropping NaT - df1: {len(df1)}, df2: {len(df2)}")

    # Merge datasets on datetime
    merged = pd.merge(df1, df2, on='Datetime', suffixes=(suffix1, suffix2))

    print("Merged columns:", list(merged.columns))
    print(f"Rows df1: {len(df1)}, df2: {len(df2)}, merged: {len(merged)}")
    if len(merged) > 0:
        print("Merged sample:\n", merged.head().to_string())
    else:
        print("⚠️ Merge vazio — verifique se os DataFrames têm timestamps coincidentes.")

    # Compute geodesic distance
    distances = []
    for _, row in merged.iterrows():
        coord1 = (row[f'Latitude{suffix1}'], row[f'Longitude{suffix1}'])
        coord2 = (row[f'Latitude{suffix2}'], row[f'Longitude{suffix2}'])
        distance = geodesic(coord1, coord2).meters
        distances.append(distance)

    merged['Distance'] = distances

    # Filter by distance
    filtered = merged[merged['Distance'] < CONTACT_DISTANCE]
    print(f"Filtered contacts count: {len(filtered)} (CONTACT_DISTANCE={CONTACT_DISTANCE})")

    create_clusterization_results('Results/DTN')

    # Construir nome de arquivo corretamente
    file_name = f'{int(CONTACT_DISTANCE)}_'

    if 'merged' in file1:
        file_name += 'sim_'
    else:
        file_name += 'nao_'

    if 'nbeats' in file1:
        file_name += 'nbeats_'
    elif 'nhits' in file1:
        file_name += 'nhits_'
    else:
        file_name += 'nao_'

    if 'som' in file1:
        number_centroids = read_field_from_json(hyperparam_path, 'som_x')
        file_name += f'som_{number_centroids}.txt'
    elif 'birch' in file1:
        number_centroids = read_field_from_json(hyperparam_path, 'BIRCH_NCLUSTERS')
        file_name += f'birch_{number_centroids}.txt'
    elif 'kmeans' in file1:
        number_centroids = read_field_from_json(hyperparam_path, 'n_clusters_kmeans')
        file_name += f'kmeans_{number_centroids}.txt'
    else:
        file_name += 'nao.txt'

    #output_DTN_filename = os.path.join(results_dir, file_name)
    output_DTN_filename = os.path.join(results_dir, f'contacts_DTN.txt')

    # Garantir diretório e criar arquivo vazio se não existir
    os.makedirs(results_dir, exist_ok=True)
    if not os.path.exists(output_DTN_filename):
        with open(output_DTN_filename, 'w') as f:
            pass
        print(f"✓ Arquivo criado: {output_DTN_filename}")

    # Se não houver contatos, salvar arquivos de debug e retornar
    if filtered.empty:
        debug_merged = os.path.join(results_dir, f'debug_merged_{file_number_onca1}_{file_number_onca2}.csv')
        debug_filtered = os.path.join(results_dir, f'debug_filtered_{file_number_onca1}_{file_number_onca2}.csv')
        merged.to_csv(debug_merged, index=False)
        filtered.to_csv(debug_filtered, index=False)
        print(f"⚠️ Nenhum contato encontrado. Arquivos de debug salvos: {debug_merged}, {debug_filtered}")
        return

    # Generate output with up and down events
    for i, row in filtered.iterrows():
        up = f"{i*5} CONN {row[f'ID{suffix1}']} {row[f'ID{suffix2}']} up"
        down = f"{(i*5) + 5} CONN {row[f'ID{suffix1}']} {row[f'ID{suffix2}']} down"
        print(up)
        print(down)
        append_variables_to_file(up, down, output_DTN_filename)

    # Export to CSV
    output_filename = os.path.join(results_dir, f'contacts_{file_number_onca1}_{file_number_onca2}.csv')
    filtered.to_csv(output_filename, index=False)
    print(f"\nFiltered contacts saved to '{output_filename}'")

def run(file_number_onca1, file_number_onca2, file_rawdata_name):
    results_dir = results_folder(file_rawdata_name)

    # Interpolação NBEATS
    file_path_1 = os.path.join(results_dir, f'Interpolation/map_{file_number_onca1}_interpolation_nbeats.csv')
    file_path_2 = os.path.join(results_dir, f'Interpolation/map_{file_number_onca2}_interpolation_nbeats.csv')
    
    file_path_3 = os.path.join(results_dir, f'Interpolation/map_{file_number_onca1}_interpolation_nhits.csv')
    file_path_4 = os.path.join(results_dir, f'Interpolation/map_{file_number_onca2}_interpolation_nhits.csv')

    # Rodar o script
    process_files(file_path_1, file_path_2, file_number_onca1, file_number_onca2, file_rawdata_name)

def run_mock():
    file_number_onca1 = sys.argv[1]
    file_number_onca2 = sys.argv[2]
    run(file_number_onca1, file_number_onca2)

