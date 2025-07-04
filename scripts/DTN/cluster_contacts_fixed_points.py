import pandas as pd
from geopy.distance import geodesic
import os
import sys
# --- Parâmetros ---
distancia_limite_m = 400  # metros

# --- Arquivos de entrada ---
arquivo_onca = sys.argv[1]
arquivo_clusters = sys.argv[2]

# --- Leitura e padronização ---
df_onca = pd.read_csv(arquivo_onca, header=None)
df_clusters = pd.read_csv(arquivo_clusters, header=None)

df_onca.columns = ['id', 'timestamp', 'longitude', 'latitude']
df_clusters.columns = ['longitude', 'latitude', 'cluster_id']

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
