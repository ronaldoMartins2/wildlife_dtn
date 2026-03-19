import pandas as pd
from geopy.distance import geodesic
import os
import sys

from Common.utils import (
    results_folder,
    read_field_from_json
)

CLUSTERING_TYPES = ['som', 'birch', 'kmeans']
INTERPOLATION_TYPES = ['nbeats', 'nhits']

def run(current_animal, file_rawdata_name):

    tangara = file_rawdata_name.split('.')[-2]
    tangara = tangara.split('/')[-1]

    # --- Parâmetros ---
    results_dir = results_folder( file_rawdata_name )

    data_prep_dir = os.path.join(os.path.dirname(__file__), '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    valor_lido = read_field_from_json(hyperparam_path, "distancia_limite_contatos")

    if valor_lido is None:
        print(f"Distância limite de contatos não encontrada no arquivo de hiperparâmetros")
        return
    
    try:
        distancia_limite_m = float(valor_lido)
    except ValueError:
        print(f"Valor inválido para distância limite de contatos: {valor_lido}")
        return

    print(f"Distância limite de contatos: {distancia_limite_m} metros")

    # --- Arquivos de entrada ---
    #arquivo_onca = sys.argv[1]
    #arquivo_clusters = sys.argv[2]

    csv_path = os.path.join(results_dir, f'map_{tangara}_all_animals.csv')
    if os.path.exists(csv_path):
        df = pd.read_csv( csv_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    else:
        print(f"Arquivo não encontrado: {csv_path}")
        return

    for clustering_type in CLUSTERING_TYPES:
        for interpolation_type in INTERPOLATION_TYPES:
            arquivo_clusters = os.path.join(results_dir, 'Clusterization', f'centroids_{clustering_type}_{interpolation_type}.csv')
            if os.path.exists(arquivo_clusters):
                df_clusters = pd.read_csv(arquivo_clusters, header=None)
            else:
                print(f"Arquivo não encontrado: {arquivo_clusters}")
                return

            # --- Leitura e padronização ---
            #df_onca = pd.read_csv(arquivo_onca, header=None)

            df_onca = df
            df_onca.columns = ['id', 'timestamp', 'longitude', 'latitude']
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

            # Criar diretório DTN se não existir
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

    nome_saida = f"contatos_{os.path.splitext(os.path.basename(arquivo_onca))[0]}_" \
                f"{os.path.splitext(os.path.basename(arquivo_clusters))[0]}.csv"
    df_contatos.to_csv(nome_saida, index=False)

    print(f"Arquivo gerado: {nome_saida}")

