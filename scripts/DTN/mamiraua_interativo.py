import pandas as pd
import folium
from folium import plugins
import itertools
from geopy.distance import geodesic 
import os

# --- CONFIGURAÇÕES ---
VELOCIDADE_FORMIGAS = 3000  # (Maior = Mais lento)
MODO_VISUALIZACAO = 'AMBOS' # 'ESTATICO', 'ANIMADO' ou 'AMBOS'

FREQUENCIA_INTERPOLACAO = '1h'
LIMITE_DISTANCIA_METROS = 250 
LIMITE_GAP_HORAS = 1 

print("1. Carregando dados...")
arquivo_csv = 'map_jaguar_mamiraua_all_animals.csv'
try:
    if os.path.exists(arquivo_csv):
        df = pd.read_csv(arquivo_csv, header=None)
    else:
        df = pd.read_csv(r'scripts\Results\jaguar_mamiraua\map_jaguar_mamiraua_all_animals.csv', header=None)
    df.columns = ['id', 'timestamp', 'longitude', 'latitude']
except Exception as e:
    print(f"ERRO CRÍTICO: {e}")
    exit()

# --- LIMPEZA DE DADOS (LÓGICA FÍSICA) ---
# Substituímos a lógica de IQR (quartis) pela validação de coordenadas reais
print(f"   Total de linhas brutas: {len(df)}")

# 1. Garante que lat/lon sejam números
df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

# 2. Remove vazios
df = df.dropna(subset=['latitude', 'longitude'])

# 3. Mantém apenas o que cabe no planeta Terra
mask_validos = (
    (df['latitude'] >= -90) & (df['latitude'] <= 90) &
    (df['longitude'] >= -180) & (df['longitude'] <= 180)
)
df = df[mask_validos].copy()

print(f"   Linhas válidas após limpeza: {len(df)}")

# Formatação de data
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%m/%d/%y %H:%M')

# Cores
lista_cores = ['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'cadetblue', 'darkgreen', 'darkblue', 'black', 'gray']
animal_colors = {uid: lista_cores[i % len(lista_cores)] for i, uid in enumerate(df['id'].unique())}

# --- 2. INTERPOLAÇÃO ---
info_animais = {}
trajetos_interpolados = {}

print("2. Processando trajetórias...")
for animal_id in df['id'].unique():
    d_raw = df[df['id'] == animal_id]
    info_animais[animal_id] = f"{d_raw['timestamp'].min().strftime('%d/%m/%y')} a {d_raw['timestamp'].max().strftime('%d/%m/%y')}"

    d = df[df['id'] == animal_id].set_index('timestamp').sort_index()
    d = d[~d.index.duplicated(keep='first')]
    d_resampled = d[['latitude', 'longitude']].resample(FREQUENCIA_INTERPOLACAO).mean()
    d_interpolated = d_resampled.interpolate(method='time', limit=LIMITE_GAP_HORAS).dropna()
    
    if not d_interpolated.empty:
        trajetos_interpolados[animal_id] = d_interpolated

# --- 3. GERAÇÃO DO MAPA ---
print("3. Construindo mapa...")
if not df.empty:
    center_lat = df['latitude'].mean()
    center_lon = df['longitude'].mean()
else:
    center_lat, center_lon = 0, 0

m = folium.Map(location=[center_lat, center_lon], zoom_start=11, tiles='OpenStreetMap')

# --- A. CAMADAS DE INTERAÇÃO ---
print("   Calculando interações...")
for id1, id2 in itertools.combinations(trajetos_interpolados.keys(), 2):
    traj1 = trajetos_interpolados[id1]
    traj2 = trajetos_interpolados[id2]
    
    merged = pd.merge(traj1, traj2, left_index=True, right_index=True, suffixes=('_1', '_2'))
    if merged.empty:
        continue

    group_name = f"⚡ Contato: {id1} & {id2}"
    fg_interacao = folium.FeatureGroup(name=group_name, show=True)
    
    tem_interacao = False
    for tempo, row in merged.iterrows():
        ponto1 = (row['latitude_1'], row['longitude_1'])
        ponto2 = (row['latitude_2'], row['longitude_2'])
        dist = geodesic(ponto1, ponto2).meters
        
        if dist <= LIMITE_DISTANCIA_METROS:
            tem_interacao = True
            cor1, cor2 = animal_colors[id1], animal_colors[id2]
            
            # Link Visual
            folium.PolyLine([ponto1, ponto2], color='black', weight=2, opacity=0.8, dash_array='5, 10').add_to(fg_interacao)

            # Marcadores e Áreas
            folium.CircleMarker(ponto1, radius=4, color='white', weight=1, fill=True, fill_color=cor1, fill_opacity=1, popup=f"{id1}").add_to(fg_interacao)
            folium.Circle(ponto1, radius=LIMITE_DISTANCIA_METROS, color=cor1, weight=1, fill=False, opacity=0.2).add_to(fg_interacao)
            
            folium.CircleMarker(ponto2, radius=4, color='white', weight=1, fill=True, fill_color=cor2, fill_opacity=1, popup=f"{id2}").add_to(fg_interacao)
            folium.Circle(ponto2, radius=LIMITE_DISTANCIA_METROS, color=cor2, weight=1, fill=False, opacity=0.2).add_to(fg_interacao)
            
            mid = [(ponto1[0]+ponto2[0])/2, (ponto1[1]+ponto2[1])/2]
            folium.Marker(mid, icon=folium.Icon(color='gray', icon='flash', prefix='glyphicon'),
                          popup=f"Contato {id1}-{id2}<br>{tempo}").add_to(fg_interacao)

    if tem_interacao:
        fg_interacao.add_to(m)

# --- B. CAMADAS DE TRAJETO ---
print("   Desenhando trajetos...")
for animal_id, data in trajetos_interpolados.items():
    points = list(zip(data['latitude'], data['longitude']))
    if not points:
        continue
    
    cor = animal_colors[animal_id]
    
    criar_estatico = MODO_VISUALIZACAO in ['ESTATICO', 'AMBOS']
    criar_animado = MODO_VISUALIZACAO in ['ANIMADO', 'AMBOS']

    # 1. Estático
    if criar_estatico:
        nome_layer = f"🐾 Trajeto {animal_id}"
        if criar_animado: nome_layer += " (Fixo)"
        
        fg = folium.FeatureGroup(name=nome_layer, show=False)
        folium.PolyLine(points, color=cor, weight=3, opacity=0.5, tooltip=f"Animal {animal_id}").add_to(fg)
        folium.CircleMarker(points[0], radius=4, color='green', fill=True, popup="Início").add_to(fg)
        folium.CircleMarker(points[-1], radius=4, color='red', fill=True, popup="Fim").add_to(fg)
        fg.add_to(m)

    # 2. Animado (LENTO)
    if criar_animado:
        nome_layer = f"〰️ Fluxo {animal_id}"
        
        fg = folium.FeatureGroup(name=nome_layer, show=False)
        plugins.AntPath(
            locations=points,
            color=cor,
            pulse_color='white',
            delay=VELOCIDADE_FORMIGAS, 
            weight=4,
            opacity=0.7,
            dash_array=[10, 20],
            tooltip=f"Direção {animal_id}"
        ).add_to(fg)
        fg.add_to(m)

# --- CONTROLES ---
folium.LayerControl(collapsed=False).add_to(m)

# Painel Info
html_info = """
<div style="position: fixed; bottom: 50px; left: 50px; width: 300px; max-height: 400px;
     background-color: white; border:2px solid #333; z-index:9999; font-size:13px; font-family: sans-serif;
     padding: 10px; opacity: 0.95; border-radius: 8px; box-shadow: 4px 4px 10px rgba(0,0,0,0.3); overflow-y:auto;">
     <h4 style="margin-top:0; border-bottom:1px solid #ccc; padding-bottom:5px;">📅 Monitoramento</h4>
     <ul style="list-style-type:none; padding-left:0;">
"""
for aid, intervalo in info_animais.items():
    cor = animal_colors[aid]
    html_info += f'<li style="margin-bottom:5px;"><span style="color:{cor}; font-size:18px;">●</span> <b>Animal {aid}:</b> <br><span style="color:#555; margin-left:18px;">{intervalo}</span></li>'
html_info += "</ul></div>"

m.get_root().html.add_child(folium.Element(html_info))

output_file = 'mapa_mamiraua_raio.html'
m.save(output_file)
print(f"Concluído! Mapa salvo em: {output_file}")