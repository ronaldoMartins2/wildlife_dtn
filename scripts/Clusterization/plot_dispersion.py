# plot_dispersion.py

import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import json
from Common.utils import (
    create_clusterization_results,
    results_folder,
    read_field_from_json
)

def run(current_animal, file_rawdata_name):
    
    # --- CAMINHOS DE ENTRADA E SAÍDA ---
    # Define o caminho para LER os dados de entrada
    input_results_dir = results_folder(file_rawdata_name)
    input_file_path = os.path.join(input_results_dir, f'map_{current_animal}.csv')

    # Define o caminho para SALVAR os resultados
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_main_dir = os.path.join(script_dir, '..', 'Results')
    cluster_output_dir = os.path.join(output_main_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    # --- LEITURA E LIMPEZA DOS DADOS ---
    if not os.path.exists(input_file_path):
        print(f"Error [Dispersion Plot]: Input file not found at {input_file_path}")
        return

    data = pd.read_csv(input_file_path, header=None)
    
    # Limpa e formata as colunas de coordenadas
    data.iloc[:, 2] = pd.to_numeric(data.iloc[:, 2].replace({',': ''}, regex=True), errors='coerce')
    data.iloc[:, 3] = pd.to_numeric(data.iloc[:, 3].replace({',': ''}, regex=True), errors='coerce')

    # Remove linhas com coordenadas inválidas
    data_cleaned = data.dropna(subset=[2, 3])
    data_cleaned = data_cleaned[(data_cleaned.iloc[:, 2] != 0) & (data_cleaned.iloc[:, 3] != 0)]
    data_cleaned = data_cleaned[
        (data_cleaned.iloc[:, 2] >= -180) & (data_cleaned.iloc[:, 2] <= 180) &
        (data_cleaned.iloc[:, 3] >= -90) & (data_cleaned.iloc[:, 3] <= 90)
    ]

    coords = data_cleaned.iloc[:, [2, 3]].values

    if coords.shape[0] == 0:
        print(f"Warning for animal {current_animal} [Dispersion Plot]: No valid coordinates left to plot.")
        return

    # --- GERAÇÃO DO GRÁFICO DE DISPERSÃO ---
    # Carrega textos do gráfico (título, eixos) de acordo com o idioma
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    # Cria e personaliza o gráfico
    plt.figure(figsize=(10, 8))
    # Plota longitude (coluna 0 de coords) no eixo X e latitude (coluna 1) no eixo Y
    plt.scatter(coords[:, 0], coords[:, 1], alpha=0.6, s=15)
    
    title_key = "grafico_dispersao_titulo" # Sugestão de nova chave para o arquivo de idioma
    title_text = lang.get(title_key, f"Gráfico de Dispersão para a Onça")
    plt.title(f"{title_text} {current_animal}")
    
    plt.xlabel(lang.get("xlabel_dispersao", "Longitude"))
    plt.ylabel(lang.get("ylabel_dispersao", "Latitude"))
    plt.grid(True)
    plt.gca().set_aspect('equal', adjustable='box') # Ajusta a escala para uma visualização geograficamente correta

    # Salva o gráfico como imagem
    output_file_png = os.path.join(cluster_output_dir, f'onca_{current_animal}_dispersao.png')
    plt.savefig(output_file_png)
    plt.close()
    print(f"Plot de dispersão salvo em: {output_file_png}")

def run_mock():
    if len(sys.argv) < 3:
        print("Uso: python3 plot_dispersion.py <animal_id> <raw_data_name>")
        sys.exit(1)
    
    current_animal = sys.argv[1]
    file_rawdata_name = sys.argv[2]
    run(current_animal, file_rawdata_name)

if __name__ == "__main__":
    run_mock()