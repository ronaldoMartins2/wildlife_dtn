import sys
import os

# Pega o caminho absoluto da pasta onde o script está
current_dir = os.path.dirname(os.path.abspath(__file__))
# Sobe dois níveis (de DTN para scripts, de scripts para a raiz wildlife_dtn)
root_dir = os.path.abspath(os.path.join(current_dir, '..'))

# Adiciona a raiz ao sistema de busca do Python
if root_dir not in sys.path:
    sys.path.append(root_dir)

# import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from geopy.distance import geodesic
from datetime import datetime
from Common.utils import (results_folder, create_combinations, get_list_animals)

# --- CONFIGURAÇÕES ---
# Definimos o passo (ex: a cada 100 metros) para não sobrecarregar o gráfico
STEP_METERS = 100 
BASE_DATE = datetime(2014, 3, 14, 4, 0)

def run(first_animal, second_animal, file_rawdata_name):
    results_dir = results_folder(file_rawdata_name)
    path_1 = os.path.join(results_dir, f'map_{first_animal}.csv')
    path_2 = os.path.join(results_dir, f'map_{second_animal}.csv')
    
    # --- Carregamento e Limpeza ---
    cols = ['animal_id', 'timestamp', 'lon', 'lat']
    try:
        df1 = pd.read_csv(path_1, header=None, names=cols, usecols=['timestamp', 'lon', 'lat'])
        df2 = pd.read_csv(path_2, header=None, names=cols, usecols=['timestamp', 'lon', 'lat'])
    except FileNotFoundError:
        print(f"Arquivos não encontrados para {first_animal} ou {second_animal}.")
        return

    for df in [df1, df2]:
        df['timestamp'] = pd.to_datetime(df['timestamp'], format="%m/%d/%y %H:%M", errors='coerce')
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        df.dropna(subset=['timestamp', 'lon', 'lat'], inplace=True)

    # --- Cruzamento de Dados ---
    merged = pd.merge(df1, df2, on='timestamp', suffixes=('_1', '_2'))
    if merged.empty:
        print("Nenhum horário coincidente encontrado para calcular distâncias.")
        return

    # --- Cálculo de Distâncias (Geodesic) ---
    print(f"Calculando distâncias para {len(merged)} pontos coincidentes...")
    # Calculamos a distância real uma única vez para todos os pares de coordenadas
    merged['distance_m'] = merged.apply(
        lambda row: geodesic((row['lat_1'], row['lon_1']), (row['lat_2'], row['lon_2'])).meters, 
        axis=1
    )

    # --- Lógica de Varredura do Raio (Gráfico) ---
    max_dist_found = merged['distance_m'].max()
    # Criamos o eixo X do zero até a distância máxima encontrada
    distancias_eixo_x = np.arange(0, max_dist_found + STEP_METERS, STEP_METERS)
    contatos_eixo_y = []

    print("Gerando curva de contatos...")
    for raio in distancias_eixo_x:
        # Conta quantos registros estão dentro do raio atual
        total_contatos = (merged['distance_m'] <= raio).sum()
        contatos_eixo_y.append(total_contatos)

    # --- Plotagem ---
    plt.figure(figsize=(12, 7))
    plt.plot(distancias_eixo_x, contatos_eixo_y, color='#2ecc71', linewidth=2)
    
    # Estilização
    plt.title(f'Acúmulo de Contatos: {first_animal} vs {second_animal}', fontsize=14)
    plt.xlabel('Distância Limite (Metros)', fontsize=12)
    plt.ylabel('Número Total de Contatos', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    
    # Marcar opcionalmente o ponto de 250m original se ele existir no range
    if max_dist_found > 250:
        contatos_250 = (merged['distance_m'] <= 250).sum()
        plt.scatter(250, contatos_250, color='red', zorder=5, label='Ponto de Corte (250m)')
        plt.legend()

    # Salvar e Mostrar
    plot_filename = f'graph_dist_vs_contacts_{first_animal}_{second_animal}.png'
    plot_path = os.path.join(results_dir, plot_filename)
    plt.savefig(plot_path, dpi=300)
    print(f"Gráfico de linha gerado com sucesso: {plot_path}")
    plt.show()

    # Se você ainda precisar salvar o CSV original de 250m:
    # contacts_original = merged[merged['distance_m'] < 250].copy()
    # ... código de salvamento ...

if __name__ == "__main__":
    list_animals = get_list_animals(r"rawdata/jaguar_mamiraua.csv", r"rawdata/jaguar_columns.json")
    pairs = create_combinations(list_animals)

    for pair in pairs:
        run(pair[0], pair[1], r"rawdata/jaguar_mamiraua.csv")