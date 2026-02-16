import pandas as pd
import folium
from folium import plugins
import itertools
from geopy.distance import geodesic 
from datetime import timedelta

# --- CONFIGURAÇÕES ---
FREQUENCIA_INTERPOLACAO = '1h'
LIMITE_DISTANCIA_METROS = 250 
LIMITE_GAP_HORAS = 1 
VELOCIDADE_DO_PLAYER = 100 

print("1. Processando dados...")
# Ajuste o caminho se necessário
arquivo_csv = 'map_jaguar_mamiraua_all_animals.csv'
try:
    df = pd.read_csv(arquivo_csv, header=None)
except FileNotFoundError:
    df = pd.read_csv(r'scripts\Results\jaguar_mamiraua\map_jaguar_mamiraua_all_animals.csv', header=None)

df.columns = ['id', 'timestamp', 'longitude', 'latitude']

# Limpeza e Formatação
Q1 = df[['latitude', 'longitude']].quantile(0.25)
Q3 = df[['latitude', 'longitude']].quantile(0.75)
IQR = Q3 - Q1
mask = ((df[['latitude', 'longitude']] >= (Q1 - 1.5 * IQR)) & 
        (df[['latitude', 'longitude']] <= (Q3 + 1.5 * IQR))).all(axis=1)
df = df[mask].copy()
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%m/%d/%y %H:%M')

# Cores
lista_cores = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'cadetblue', 'darkgreen', 'darkblue', 'black', 'gray']
color_map = {
    'red': '#FF0000', 'blue': '#0000FF', 'green': '#008000', 'purple': '#800080', 
    'orange': '#FFA500', 'darkred': '#8B0000', 'cadetblue': '#5F9EA0', 
    'darkgreen': '#006400', 'darkblue': '#00008B', 'black': '#000000', 'gray': '#808080'
}
animal_colors = {uid: lista_cores[i % len(lista_cores)] for i, uid in enumerate(df['id'].unique())}

# --- FUNÇÃO DE DURAÇÃO (HORAS TOTAIS) ---
# Usar horas reduz a magnitude do número e mantém precisão suficiente para visualização
def get_duration_hours(delta):
    hours = int(delta.total_seconds() / 3600)
    if hours < 1: hours = 1
    return f"PT{hours}H"

# --- 2. INTERPOLAÇÃO ---
print("2. Interpolando trajetórias...")
trajetos_interpolados = {}
datas_finais = {}

for animal_id in df['id'].unique():
    d = df[df['id'] == animal_id].set_index('timestamp').sort_index()
    # Data final exata do animal
    datas_finais[animal_id] = d.index.max()
    
    d = d[~d.index.duplicated(keep='first')]
    d_resampled = d[['latitude', 'longitude']].resample(FREQUENCIA_INTERPOLACAO).mean()
    d_interpolated = d_resampled.interpolate(method='time', limit=LIMITE_GAP_HORAS).dropna()
    
    if not d_interpolated.empty:
        trajetos_interpolados[animal_id] = d_interpolated

# --- 3. GERAÇÃO GEOJSON ---
print("3. Construindo animação...")
features = []

for animal_id, data in trajetos_interpolados.items():
    cor_nome = animal_colors[animal_id]
    cor_hex = color_map.get(cor_nome, '#3388ff')
    data_final_real = datas_finais[animal_id]
    
    data_reset = data.reset_index()
    
    for i in range(len(data_reset)):
        row = data_reset.iloc[i]
        tempo_atual = row['timestamp']
        
        # Segurança: Não gerar nada além da data final
        if tempo_atual > data_final_real:
            continue
            
        time_str = tempo_atual.strftime('%Y-%m-%dT%H:%M:%S')
        
        # 1. CABEÇA (Ponto) - Dura 1 hora
        feature_point = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [row['longitude'], row['latitude']]
            },
            'properties': {
                'time': time_str,
                'duration': 'PT1H',
                'style': {'color': cor_hex},
                'icon': 'circle',
                'iconstyle': {
                    'fillColor': cor_hex, 'fillOpacity': 1, 'stroke': 'true',
                    'color': 'white', 'weight': 1, 'radius': 6
                },
                'popup': f"Animal {animal_id}"
            }
        }
        features.append(feature_point)
        
        # 2. RASTRO (Linha)
        if i > 0:
            row_prev = data_reset.iloc[i-1]
            tempo_restante = data_final_real - tempo_atual
            
            # Só desenha se ainda houver tempo de vida
            if tempo_restante.total_seconds() > 0:
                duracao_rastro = get_duration_hours(tempo_restante)
                
                feature_line = {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'LineString',
                        'coordinates': [
                            [row_prev['longitude'], row_prev['latitude']],
                            [row['longitude'], row['latitude']]
                        ]
                    },
                    'properties': {
                        'time': time_str,
                        'duration': duracao_rastro, # Segmento expira junto com o animal
                        'style': {
                            'color': cor_hex, 'weight': 3, 'opacity': 0.6
                        }
                    }
                }
                features.append(feature_line)

# B. INTERAÇÕES
print("   Adicionando contatos...")
for id1, id2 in itertools.combinations(trajetos_interpolados.keys(), 2):
    traj1 = trajetos_interpolados[id1]
    traj2 = trajetos_interpolados[id2]
    
    merged = pd.merge(traj1, traj2, left_index=True, right_index=True, suffixes=('_1', '_2'))
    if merged.empty:
        continue

    for tempo, row in merged.iterrows():
        ponto1 = (row['latitude_1'], row['longitude_1'])
        ponto2 = (row['latitude_2'], row['longitude_2'])
        dist = geodesic(ponto1, ponto2).meters
        
        if dist <= LIMITE_DISTANCIA_METROS:
            time_str = tempo.strftime('%Y-%m-%dT%H:%M:%S')
            
            # Link de Contato (Tracejado)
            feature_link = {
                'type': 'Feature',
                'geometry': {
                    'type': 'LineString',
                    'coordinates': [
                        [row['longitude_1'], row['latitude_1']],
                        [row['longitude_2'], row['latitude_2']]
                    ]
                },
                'properties': {
                    'time': time_str,
                    'duration': 'PT1H',
                    'style': {'color': 'black', 'weight': 2, 'dashArray': '5,5', 'opacity': 0.8},
                    'popup': f"Contato {id1}-{id2} ({dist:.0f}m)"
                }
            }
            features.append(feature_link)
            
            # Flash
            mid_lon = (row['longitude_1'] + row['longitude_2']) / 2
            mid_lat = (row['latitude_1'] + row['latitude_2']) / 2
            feature_flash = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [mid_lon, mid_lat]
                },
                'properties': {
                    'time': time_str,
                    'duration': 'PT1H',
                    'icon': 'circle',
                    'iconstyle': {
                        'fillColor': 'yellow', 'fillOpacity': 1, 'stroke': 'false', 'radius': 6
                    }
                }
            }
            features.append(feature_flash)

# --- 4. MAPA ---
print("4. Finalizando mapa...")
center_lat = df['latitude'].mean()
center_lon = df['longitude'].mean()
m = folium.Map(location=[center_lat, center_lon], zoom_start=11, tiles='OpenStreetMap')

plugins.TimestampedGeoJson(
    {'type': 'FeatureCollection', 'features': features},
    period='PT1H',
    add_last_point=False, 
    auto_play=False,
    loop=False,
    max_speed=VELOCIDADE_DO_PLAYER,
    loop_button=True,
    date_options='DD/MM/YYYY HH:mm',
    time_slider_drag_update=True
).add_to(m)

html_instructions = """
<div style="position: fixed; bottom: 50px; left: 50px; width: 280px; 
     background-color: white; border:2px solid grey; z-index:9999; font-size:14px;
     padding: 10px; border-radius: 10px; opacity: 0.9; font-family: sans-serif;">
     <b>🎥 Simulação Final</b><br>
     <small>Os rastros expiram individualmente.</small>
</div>
"""
m.get_root().html.add_child(folium.Element(html_instructions))

output_file = 'simulacao_mamiraua.html'
m.save(output_file)
print(f"Concluído! Mapa salvo em: {output_file}")