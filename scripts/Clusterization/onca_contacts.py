import pandas as pd
from geopy.distance import geodesic
import sys

#python3 onca_contacts.py 94 birch_clusters_map_94(nome do arquivo csv)

# Definir o raio de contato em metros
RAIO_CONTATO_METROS = 50
numero_onca = sys.argv[1]
tree_csv = sys.argv[2]
# Carregar os dados
df_onca = pd.read_csv(f"../Data_preparation/map_{numero_onca}.csv", header=None)
df_trees = pd.read_csv(f"{tree_csv}.csv", header=None)

# Definir os nomes das colunas corretamente
df_onca.columns = ['ID', 'Datetime', 'Longitude', 'Latitude']
df_trees.columns = ['Longitude', 'Latitude', 'Label']

df_onca['Datetime'] = pd.to_datetime(df_onca['Datetime'], format='%m/%d/%y %H:%M', errors='coerce')

# Verificar e corrigir valores inválidos
def is_valid_latitude(lat):
    return -90 <= lat <= 90

def is_valid_longitude(lon):
    return -180 <= lon <= 180

# Remover valores inválidos
df_onca = df_onca[df_onca['Latitude'].apply(is_valid_latitude) & df_onca['Longitude'].apply(is_valid_longitude)]
df_trees = df_trees[df_trees['Latitude'].apply(is_valid_latitude) & df_trees['Longitude'].apply(is_valid_longitude)]

# Criar lista para armazenar os contatos
contatos = []

# Verificar proximidade entre a onça e as árvores
for _, onca in df_onca.iterrows():
    onca_pos = (onca['Latitude'], onca['Longitude'])
    
    for _, tree in df_trees.iterrows():
        tree_pos = (tree['Latitude'], tree['Longitude'])
        
        # Calcular a distância entre a onça e a árvore
        distancia = geodesic(onca_pos, tree_pos).meters
        
        if distancia <= RAIO_CONTATO_METROS:
            contatos.append([onca['Datetime'], onca['Latitude'], onca['Longitude']])
            break  # Evita múltiplas entradas para a mesma onça no mesmo tempo

# Converter para DataFrame
df_contatos = pd.DataFrame(contatos, columns=['Datetime', 'Latitude', 'Longitude'])

# Verificar se df_contatos não está vazio antes de tentar formatar
if not df_contatos.empty:
    df_contatos['Datetime'] = df_contatos['Datetime'].dt.strftime('%m/%d/%y %H:%M')

# Salvar o arquivo de saída
output_file = f"./Contactos_{numero_onca}_Metodo.csv"
df_contatos.to_csv(output_file, index=False, header=None)

print(f"Arquivo de contatos salvo em: {output_file}")
