import sys
import os

# Ajuste de Path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, '..'))
if root_dir not in sys.path:
    sys.path.append(root_dir)

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from geopy.distance import geodesic
from datetime import datetime
from Common.utils import (results_folder, create_combinations, get_list_animals)

STEP_METERS = 100 

def run_consolidado(file_rawdata_name):
    results_dir = results_folder(file_rawdata_name)
    list_animals = get_list_animals(file_rawdata_name, r"rawdata/jaguar_columns.json")
    pairs = create_combinations(list_animals)
    
    todas_as_distancias = []

    print(f"Processando {len(pairs)} pares de animais...")

    for first_animal, second_animal in pairs:
        path_1 = os.path.join(results_dir, f'map_{first_animal}.csv')
        path_2 = os.path.join(results_dir, f'map_{second_animal}.csv')
        
        try:
            # Forçamos os nomes das colunas para garantir a ordem
            df1 = pd.read_csv(path_1, header=None, names=['animal_id', 'timestamp', 'lon', 'lat'])
            df2 = pd.read_csv(path_2, header=None, names=['animal_id', 'timestamp', 'lon', 'lat'])
        except FileNotFoundError:
            continue

        for df in [df1, df2]:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format="%m/%d/%y %H:%M", errors='coerce')
            df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
            df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
            
            # --- FILTRO CRÍTICO: Remove latitudes e longitudes impossíveis ---
            df.dropna(subset=['timestamp', 'lon', 'lat'], inplace=True)
            mask = (df['lat'] >= -90) & (df['lat'] <= 90) & \
                   (df['lon'] >= -180) & (df['lon'] <= 180)
            df.query("@mask", inplace=True)

        merged = pd.merge(df1, df2, on='timestamp', suffixes=('_1', '_2'))
        
        if not merged.empty:
            # Usamos um try/except aqui para ignorar pontos específicos que ainda deem erro
            def safe_geodesic(row):
                try:
                    # geopy usa (latitude, longitude)
                    return geodesic((row['lat_1'], row['lon_1']), (row['lat_2'], row['lon_2'])).meters
                except ValueError:
                    return None

            distancias_par = merged.apply(safe_geodesic, axis=1)
            # Removemos eventuais erros de cálculo antes de adicionar à lista global
            todas_as_distancias.extend(distancias_par.dropna().tolist())

    # --- Gerar Gráfico Consolidado ---
    todas_as_distancias = np.array(todas_as_distancias)
    max_dist = todas_as_distancias.max()
    
    # Eixo X: do zero até a maior distância encontrada no dataset todo
    eixo_x = np.arange(0, max_dist + STEP_METERS, STEP_METERS)
    # Eixo Y: contagem acumulada
    eixo_y = [ (todas_as_distancias <= raio).sum() for raio in eixo_x ]

    plt.figure(figsize=(12, 7))
    plt.plot(eixo_x, eixo_y, color='#2c3e50', linewidth=2, label='Total Acumulado (Todos os Pares)')
    plt.fill_between(eixo_x, eixo_y, color='#2c3e50', alpha=0.1)

    # Destaque para os 250m
    if max_dist >= 250:
        contatos_250 = (todas_as_distancias <= 250).sum()
        plt.scatter(250, contatos_250, color='red', zorder=5)
        plt.annotate(f'250m: {contatos_250} contatos', (250, contatos_250), 
                     textcoords="offset points", xytext=(10,10), ha='left', color='red', weight='bold')
        plt.axvline(x=250, color='red', linestyle='--', alpha=0.5)

    plt.title(f'Evolução Total de Contatos vs. Distância (Dataset Completo)', fontsize=14)
    plt.xlabel('Distância Limite (Metros)', fontsize=12)
    plt.ylabel('Número Total de Contatos', fontsize=12)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend()

    # Salvar gráfico único
    output_path = os.path.join(results_dir, 'grafico_total_contatos_geodesic.png')
    plt.savefig(output_path, dpi=300)
    print(f"\nSucesso! Gráfico consolidado salvo em: {output_path}")
    plt.show()

if __name__ == "__main__":
    run_consolidado(r"rawdata/jaguar_mamiraua.csv")