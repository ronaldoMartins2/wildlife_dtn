import pandas as pd
import folium
from folium import plugins
import itertools
from geopy.distance import geodesic 
from datetime import timedelta
import os

# --- 1. CONFIGURAÇÕES GERAIS ---
FREQUENCIA_INTERPOLACAO = '1h'
LIMITE_DISTANCIA_METROS = 250   # Raio de comunicação entre animais
LIMITE_GAP_HORAS = 1 
VELOCIDADE_DO_PLAYER = 100 

# --- CONFIGURAÇÃO DAS TORRES ---
RAIO_ALCANCE_TORRE = 250
CONFIG_TORRES = [
    {'arquivo': r'scripts\Results\jaguar_mamiraua\centroids_8_kmeans_rawdata.csv', 'nome': '📡 K-means (8)', 'cor': '#FFD700'},
    {'arquivo': r'scripts\Results\jaguar_mamiraua\centroids_8_som_rawdata.csv', 'nome': '📡 SOM (8)', 'cor': '#FF00FF'},
    {'arquivo': r'scripts\Results\jaguar_mamiraua\centroids_8_birch_rawdata.csv', 'nome': '📡 BIRCH (8)', 'cor': '#00FFFF'},
    {'arquivo': r'scripts\Results\jaguar_mamiraua\centroids_16_kmeans_rawdata.csv', 'nome': '📡 K-means (16)', 'cor': '#32CD32'},
    {'arquivo': r'scripts\Results\jaguar_mamiraua\centroids_16_som_rawdata.csv', 'nome': '📡 SOM (16)', 'cor': '#8B4513'},
    {'arquivo': r'scripts\Results\jaguar_mamiraua\centroids_16_birch_rawdata.csv', 'nome': '📡 BIRCH (16)', 'cor': '#FF7F50'},
    {'arquivo': r'scripts\Results\jaguar_mamiraua\centroids_32_kmeans_rawdata.csv', 'nome': '📡 K-means (32)', 'cor': '#4682B4'},
    {'arquivo': r'scripts\Results\jaguar_mamiraua\centroids_32_som_rawdata.csv', 'nome': '📡 SOM (32)', 'cor': '#808000'},
    {'arquivo': r'scripts\Results\jaguar_mamiraua\centroids_32_birch_rawdata.csv', 'nome': '📡 BIRCH (32)', 'cor': '#BA55D3'},   
]

print("1. Carregando dados dos animais...")
arquivo_animais = 'map_jaguar_mamiraua_all_animals.csv'
try:
    if os.path.exists(arquivo_animais):
        df = pd.read_csv(arquivo_animais, header=None)
    else:
        df = pd.read_csv(r'scripts\Results\jaguar_mamiraua\map_jaguar_mamiraua_all_animals.csv', header=None)
    df.columns = ['id', 'timestamp', 'longitude', 'latitude']
except Exception as e:
    print(f"ERRO: Não achei o arquivo de animais. {e}")
    df = pd.DataFrame(columns=['id', 'timestamp', 'longitude', 'latitude'])

# --- LIMPEZA DE DADOS (NOVA LÓGICA) ---
if not df.empty:
    print(f"   Dados originais: {len(df)} linhas.")
    
    # 1. Garante que lat/lon sejam números (transforma erros em NaN)
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    
    # 2. Remove linhas onde lat/lon são NaN (Vazios ou erro de leitura)
    df = df.dropna(subset=['latitude', 'longitude'])
    
    # 3. Filtro Físico: Mantém apenas coordenadas válidas no planeta Terra
    # Lat: -90 a 90 / Lon: -180 a 180
    mask_validos = (
        (df['latitude'] >= -90) & (df['latitude'] <= 90) &
        (df['longitude'] >= -180) & (df['longitude'] <= 180)
    )
    df = df[mask_validos].copy()
    
    print(f"   Dados após limpeza física: {len(df)} linhas.")

    # Formatação de data
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%m/%d/%y %H:%M')

# Cores e Mapa Base
lista_cores = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'cadetblue', 'darkgreen', 'darkblue', 'black', 'gray']
color_map = {
    'red': '#FF0000', 'blue': '#0000FF', 'green': '#008000', 'purple': '#800080', 
    'orange': '#FFA500', 'darkred': '#8B0000', 'cadetblue': '#5F9EA0', 
    'darkgreen': '#006400', 'darkblue': '#00008B', 'black': '#000000', 'gray': '#808080'
}

if not df.empty:
    animal_colors = {uid: lista_cores[i % len(lista_cores)] for i, uid in enumerate(df['id'].unique())}
    center_lat = df['latitude'].mean()
    center_lon = df['longitude'].mean()
else:
    center_lat, center_lon = -3.0, -64.9 

m = folium.Map(location=[center_lat, center_lon], zoom_start=11, tiles='OpenStreetMap')

# --- 2. PLOTAR TORRES (CAMADAS) ---
print("2. Processando arquivos de torres...")

if not CONFIG_TORRES:
    print("   (Nenhum arquivo de torre configurado)")

for cfg in CONFIG_TORRES:
    file_path = cfg['arquivo']
    layer_name = cfg['nome']
    color = cfg['cor']
    
    if os.path.exists(file_path):
        try:
            # LER CSV SEM CABEÇALHO (ID, LON, LAT)
            df_torres = pd.read_csv(file_path, header=None)
            df_torres.columns = ['id', 'longitude', 'latitude'] 
            
            # Cria camada (Group)
            fg_torres = folium.FeatureGroup(name=layer_name, show=False)
            
            for _, row in df_torres.iterrows():
                lat, lon = row['latitude'], row['longitude']
                tid = row['id']
                
                # Ícone
                folium.Marker(
                    location=[lat, lon],
                    # Ícone branco com o símbolo colorido para destacar na floresta
                    icon=folium.Icon(color='white', icon_color=color, icon='tower-cell', prefix='fa'),
                    tooltip=f"Torre {tid} ({layer_name})"
                ).add_to(fg_torres)
                
                # Raio
                folium.Circle(
                    location=[lat, lon],
                    radius=RAIO_ALCANCE_TORRE,
                    color=color, fill_color=color,
                    weight=1, fill=True, fill_opacity=0.2,
                    popup=f"Torre {tid}<br>Raio: {RAIO_ALCANCE_TORRE}m"
                ).add_to(fg_torres)
                
            fg_torres.add_to(m)
            print(f"   -> {layer_name}: {len(df_torres)} torres.")
            
        except Exception as e:
            print(f"   ERRO ao ler {file_path}: {e}")
    else:
        print(f"   ERRO: Arquivo não encontrado: {file_path}")

# --- 3. SIMULAÇÃO ANIMAIS ---
print("3. Preparando simulação...")

def get_duration_hours(delta):
    hours = int(delta.total_seconds() / 3600)
    if hours < 1: hours = 1
    return f"PT{hours}H"

features = []
if not df.empty:
    trajetos_interpolados = {}
    datas_finais = {}

    # Interpolação
    for animal_id in df['id'].unique():
        d = df[df['id'] == animal_id].set_index('timestamp').sort_index()
        datas_finais[animal_id] = d.index.max()
        d = d[~d.index.duplicated(keep='first')]
        d_resampled = d[['latitude', 'longitude']].resample(FREQUENCIA_INTERPOLACAO).mean()
        d_interpolated = d_resampled.interpolate(method='time', limit=LIMITE_GAP_HORAS).dropna()
        if not d_interpolated.empty:
            trajetos_interpolados[animal_id] = d_interpolated

    # Construção GeoJSON
    for animal_id, data in trajetos_interpolados.items():
        cor_nome = animal_colors[animal_id]
        cor_hex = color_map.get(cor_nome, '#3388ff')
        data_final_real = datas_finais[animal_id]
        data_reset = data.reset_index()
        
        for i in range(len(data_reset)):
            row = data_reset.iloc[i]
            tempo_atual = row['timestamp']
            
            if tempo_atual > data_final_real: continue
            
            time_str = tempo_atual.strftime('%Y-%m-%dT%H:%M:%S')
            
            # Ponto (Cabeça)
            features.append({
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [row['longitude'], row['latitude']]},
                'properties': {
                    'time': time_str, 'duration': 'PT1H',
                    'style': {'color': cor_hex},
                    'icon': 'circle',
                    'iconstyle': {'fillColor': cor_hex, 'fillOpacity': 1, 'stroke': 'true', 'color': 'white', 'weight': 1, 'radius': 6},
                    'popup': f"Animal {animal_id}"
                }
            })
            
            # Rastro
            if i > 0:
                row_prev = data_reset.iloc[i-1]
                tempo_restante = data_final_real - tempo_atual
                if tempo_restante.total_seconds() > 0:
                    features.append({
                        'type': 'Feature',
                        'geometry': {'type': 'LineString', 'coordinates': [[row_prev['longitude'], row_prev['latitude']], [row['longitude'], row['latitude']]]},
                        'properties': {
                            'time': time_str,
                            'duration': get_duration_hours(tempo_restante),
                            'style': {'color': cor_hex, 'weight': 3, 'opacity': 0.6}
                        }
                    })

    # Contatos
    for id1, id2 in itertools.combinations(trajetos_interpolados.keys(), 2):
        traj1 = trajetos_interpolados[id1]
        traj2 = trajetos_interpolados[id2]
        merged = pd.merge(traj1, traj2, left_index=True, right_index=True, suffixes=('_1', '_2'))
        if merged.empty: continue

        for tempo, row in merged.iterrows():
            ponto1 = (row['latitude_1'], row['longitude_1'])
            ponto2 = (row['latitude_2'], row['longitude_2'])
            dist = geodesic(ponto1, ponto2).meters
            
            if dist <= LIMITE_DISTANCIA_METROS:
                time_str = tempo.strftime('%Y-%m-%dT%H:%M:%S')
                features.append({
                    'type': 'Feature',
                    'geometry': {'type': 'LineString', 'coordinates': [[row['longitude_1'], row['latitude_1']], [row['longitude_2'], row['latitude_2']]]},
                    'properties': {
                        'time': time_str, 'duration': 'PT1H',
                        'style': {'color': 'black', 'weight': 2, 'dashArray': '5,5', 'opacity': 0.8},
                        'popup': f"Contato {id1}-{id2}"
                    }
                })
                features.append({
                    'type': 'Feature',
                    'geometry': {'type': 'Point', 'coordinates': [(row['longitude_1']+row['longitude_2'])/2, (row['latitude_1']+row['latitude_2'])/2]},
                    'properties': {
                        'time': time_str, 'duration': 'PT1H',
                        'icon': 'circle', 'iconstyle': {'fillColor': 'yellow', 'fillOpacity': 1, 'stroke': 'false', 'radius': 6}
                    }
                })

# --- 4. EXPORTAR ---
if features:
    plugins.TimestampedGeoJson(
        {'type': 'FeatureCollection', 'features': features},
        period='PT1H', add_last_point=False, auto_play=False, loop=False,
        max_speed=VELOCIDADE_DO_PLAYER, loop_button=True,
        date_options='DD/MM/YYYY HH:mm', time_slider_drag_update=True
    ).add_to(m)

# Controle de Camadas
folium.LayerControl(collapsed=False).add_to(m)

html_instructions = """
<div style="position: fixed; bottom: 50px; left: 50px; width: 280px; 
     background-color: white; border:2px solid grey; z-index:9999; font-size:14px;
     padding: 10px; border-radius: 10px; opacity: 0.9; font-family: sans-serif;">
     <b>🎮 Simulação Completa</b><br>
     <small>Filtro: Apenas coords inválidas removidas.</small><br>
     <small>1. Dê o Play.</small><br>
     <small>2. Ligue as camadas de Torres.</small>
</div>
"""
m.get_root().html.add_child(folium.Element(html_instructions))

output_file = 'simulacao_completa_torres.html'
m.save(output_file)
print(f"Concluído! Mapa salvo em: {output_file}")