import pandas as pd
import folium

# 1. Carregar os dados
file_path = 'scripts\\Results\\jaguar_mamiraua\\map_jaguar_mamiraua_all_animals.csv'
df = pd.read_csv(file_path, header=None)
df.columns = ['id', 'timestamp', 'longitude', 'latitude']

# 2. Limpeza de Dados (Remoção de Outliers via IQR)
# Calcula os quartis para Latitude e Longitude
Q1 = df[['latitude', 'longitude']].quantile(0.25)
Q3 = df[['latitude', 'longitude']].quantile(0.75)
IQR = Q3 - Q1

# Define os limites (1.5x o intervalo interquartil)
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Filtra apenas os dados que estão dentro dos limites
mask = ((df[['latitude', 'longitude']] >= lower_bound) & 
        (df[['latitude', 'longitude']] <= upper_bound)).all(axis=1)

df_clean = df[mask].copy()

# Converter timestamp para garantir a ordem correta do trajeto
df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])

print(f"Dados originais: {len(df)} pontos")
print(f"Dados limpos: {len(df_clean)} pontos")
print(f"Foram removidos {len(df) - len(df_clean)} outliers.")

# 3. Gerar o Mapa Interativo
# Centralizar o mapa na média das coordenadas limpas
center_lat = df_clean['latitude'].mean()
center_lon = df_clean['longitude'].mean()

m = folium.Map(location=[center_lat, center_lon], zoom_start=11, tiles='OpenStreetMap')

# Lista de cores para diferenciar os animais
colors = [
    'red', 'blue', 'green', 'purple', 'orange', 'darkred',
    'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue',
    'darkpurple', 'pink', 'lightblue', 'lightgreen', 'gray', 'black'
]

animal_ids = df_clean['id'].unique()

for i, animal_id in enumerate(animal_ids):
    # Filtrar dados do animal específico e ordenar por tempo
    animal_data = df_clean[df_clean['id'] == animal_id].sort_values('timestamp')
    
    # Criar lista de tuplas (lat, lon) para o folium
    points = list(zip(animal_data['latitude'], animal_data['longitude']))
    
    if not points:
        continue
        
    # Escolher cor cíclica
    color = colors[i % len(colors)]
    
    # Adicionar a linha da trajetória
    folium.PolyLine(
        points, 
        color=color, 
        weight=2.5, 
        opacity=0.8, 
        tooltip=f'Animal {animal_id}'
    ).add_to(m)
    
    # Adicionar marcadores de Início (pequeno) e Fim (maior)
    folium.CircleMarker(
        points[0], radius=3, color=color, fill=True, fill_opacity=1, 
        popup=f'ID {animal_id}: Início ({animal_data.iloc[0]["timestamp"]})'
    ).add_to(m)
    
    folium.CircleMarker(
        points[-1], radius=5, color=color, fill=True, fill_opacity=1, 
        popup=f'ID {animal_id}: Fim ({animal_data.iloc[-1]["timestamp"]})'
    ).add_to(m)

# 4. Salvar o arquivo
output_file = 'mapa_mamiraua_limpo.html'
m.save(output_file)
print(f"Mapa salvo com sucesso em: {output_file}")