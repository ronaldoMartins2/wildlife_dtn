import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from math import radians, cos, sin, asin, sqrt

# Função Haversine para cálculo de distância real (km)
def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon, dlat = lon2 - lon1, lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * asin(sqrt(a)) * 6371

# Configurações de Mapeamento
# Animal 93 -> Nó 0, 94 -> Nó 1, etc.
ID_MAP = {93: 0, 94: 1, 95: 2, 96: 3, 97: 4, 98: 5, 99: 6, 100: 7}

def gerar_grafico_distancia(arquivo_map, id_animal_a, id_animal_b):
    df = pd.read_csv(arquivo_map, header=None, names=['id', 'ts', 'lon', 'lat'])
    df['ts'] = pd.to_datetime(df['ts'])
    df['node_id'] = df['id'].map(ID_MAP)

    # Interpolação para alinhar os tempos (janelas de 12h para suavizar)
    time_index = pd.date_range(start=df['ts'].min(), end=df['ts'].max(), freq='12H')
    
    def preparar_no(node_id):
        temp = df[df['node_id'] == node_id].drop_duplicates('ts').set_index('ts')
        temp = temp.reindex(temp.index.union(time_index)).sort_index()
        temp[['lon', 'lat']] = temp[['lon', 'lat']].interpolate(method='time')
        return temp.reindex(time_index)

    no_a = preparar_no(id_animal_a)
    no_b = preparar_no(id_animal_b)

    # Cálculo da distância
    distancias = [haversine(row_a['lon'], row_a['lat'], row_b['lon'], row_b['lat']) 
                  for (_, row_a), (_, row_b) in zip(no_a.iterrows(), no_b.iterrows())]

    # Plotagem
    plt.figure(figsize=(12, 5))
    plt.plot(time_index, distancias, label=f'Distância Nó {id_animal_a} - Nó {id_animal_b}')
    plt.axhline(y=1.0, color='r', linestyle='--', label='Limiar de Contato (1km)')
    plt.title(f'Evolução da Distância de Contatos: Nó {id_animal_a} vs Nó {id_animal_b}')
    plt.ylabel('Distância (km)')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.show()

# Exemplo de uso:
# gerar_grafico_distancia('map_jaguar_mamiraua_all_animals.csv', 0, 3) # Nó 0 (93) e Nó 3 (96)
if __name__ == "__main__":
    for i in range(1, 8):
        gerar_grafico_distancia(r'scripts\Results\jaguar_mamiraua\map_jaguar_mamiraua_all_animals.csv', 0, i)