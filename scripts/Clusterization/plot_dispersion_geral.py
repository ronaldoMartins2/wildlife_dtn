import pandas as pd
import matplotlib.pyplot as plt
import os
import json
import sys
from Common.utils import create_clusterization_results, read_field_from_json

def run_geral():
    """
    Este script carrega a base de dados original de onças, usa um arquivo JSON
    para definir as colunas, limpa os dados e gera um único gráfico de dispersão.
    """
    
    # --- CAMINHOS DE ENTRADA E SAÍDA (AUTOMÁTICOS) ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Navega duas pastas acima (de /scripts/Clusterization para /wildlife_dtn) e entra na pasta /rawdata
    rawdata_dir = os.path.join(script_dir, '..', '..', 'rawdata') # <<< Caminho relativo e automático
    
    input_csv_path = os.path.join(rawdata_dir, 'jaguar_mamiraua.csv')
    input_json_path = os.path.join(rawdata_dir, 'jaguar_columns.json')

    # Define o caminho para SALVAR o gráfico de saída
    output_main_dir = os.path.join(script_dir, '..', 'Results')
    cluster_output_dir = os.path.join(output_main_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    # --- LEITURA E PREPARAÇÃO DOS DADOS ---
    # Verifica se os arquivos necessários existem
    if not os.path.exists(input_csv_path) or not os.path.exists(input_json_path):
        print("ERRO: Um dos arquivos de entrada não foi encontrado.")
        print(f"Verificando CSV em: {input_csv_path}")
        print(f"Verificando JSON em: {input_json_path}")
        return

    # 1. Carrega o DICIONÁRIO de colunas do arquivo JSON
    with open(input_json_path, 'r') as f:
        columns_map = json.load(f)
    
    # <<< CORREÇÃO 1: Extrai a LISTA DE VALORES para usar como nomes de colunas >>>
    column_names = list(columns_map.values())
    print(f"Colunas carregadas do JSON: {column_names}")
    # 2. Carrega o CSV usando os nomes das colunas
    data = pd.read_csv(input_csv_path, header=None, names=column_names)
    print(f"Total de {len(data)} registros lidos do arquivo CSV.")

    id_col = columns_map['ID']
    lon_col = columns_map['LONGITUDE']
    lat_col = columns_map['LATITUDE']

    columns_to_keep = [id_col, lon_col, lat_col]

    data = data[columns_to_keep]

    if lon_col not in data.columns or lat_col not in data.columns:
        print(f"ERRO: As colunas '{lon_col}' e/ou '{lat_col}' não foram encontradas no arquivo.")
        print("Por favor, ajuste os nomes das colunas no script.")
        return

    # --- LIMPEZA DOS DADOS ---
    data[lon_col] = pd.to_numeric(data[lon_col], errors='coerce')
    data[lat_col] = pd.to_numeric(data[lat_col], errors='coerce')

    data_cleaned = data.dropna(subset=[lon_col, lat_col])
    data_cleaned = data_cleaned[(data_cleaned[lon_col] != 0) & (data_cleaned[lat_col] != 0)]
    data_cleaned = data_cleaned[
        (data_cleaned[lon_col] >= -180) & (data_cleaned[lon_col] <= 180) &
        (data_cleaned[lat_col] >= -90) & (data_cleaned[lat_col] <= 90)
    ]

    print(f"{len(data_cleaned)} registros válidos restantes após a limpeza.")

    if data_cleaned.empty:
        print("Nenhum dado válido para plotar após a limpeza.")
        return

    # --- GERAÇÃO DO GRÁFICO ---
    # Carrega textos para o gráfico (título, eixos)
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    plt.figure(figsize=(12, 10))
    plt.scatter(data_cleaned[lon_col], data_cleaned[lat_col], alpha=0.5, s=10, label="Localizações")

    # 1. Calcula os limites dos dados com uma pequena margem (5%)
    #min_lon, max_lon = data_cleaned[lon_col].min(), data_cleaned[lon_col].max()
    #min_lat, max_lat = data_cleaned[lat_col].min(), data_cleaned[lat_col].max()
    
    #margin_lon = (max_lon - min_lon) * 0.05
    #margin_lat = (max_lat - min_lat) * 0.05
    
    # 2. Define os limites do gráfico (eixos X e Y)
    #plt.xlim(min_lon - margin_lon, max_lon + margin_lon)
    #plt.ylim(min_lat - margin_lat, max_lat + margin_lat)
    
    title_key = "grafico_dispersao_geral_titulo"
    title_text = lang.get(title_key, "Dispersão Geográfica de Todos os Animais")
    plt.title(title_text)
    
    plt.xlabel(lang.get("xlabel_dispersao_geral", "Longitude"))
    plt.ylabel(lang.get("ylabel_dispersao_geral", "Latitude"))
    plt.grid(True)
    plt.legend()
    #plt.gca().set_aspect('equal', adjustable='box')

    # --- SALVAMENTO DO RESULTADO ---
    output_file_png = os.path.join(cluster_output_dir, 'dispersao_geral_todos_animais.png')
    plt.savefig(output_file_png, dpi=300)
    plt.close()
    
    print("-" * 50)
    print("Gráfico de dispersão geral foi criado com sucesso!")
    print(f"Salvo em: {output_file_png}")
    print("-" * 50)


# --- Bloco de Execução ---
if __name__ == "__main__":
    run_geral()