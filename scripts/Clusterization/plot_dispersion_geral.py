import pandas as pd
import matplotlib.pyplot as plt
import os
import json
from Common.utils import create_clusterization_results, read_field_from_json

def extract_folder_name(file_rawdata):
    file_name = file_rawdata.split('/')
    file_name = file_name[-1].split('.')[0]
    return file_name

def plot_jaguar(file_rawdata=None):
    """
    Este script carrega a base de dados original, limpa os dados e gera
    um gráfico de dispersão para os dados da jaguatirica.
    """
    
    # --- CAMINHOS DE ENTRADA E SAÍDA (AUTOMÁTICOS) ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    rawdata_dir = os.path.join(script_dir, '..', '..', 'rawdata')
    
    base_name = extract_folder_name(file_rawdata) if file_rawdata else 'jaguar_mamiraua'
    input_csv_path = os.path.join(rawdata_dir, f'{base_name}.csv')
    # Seleciona JSON adequado
    if base_name == 'jaguar_mamiraua':
        json_name = 'jaguar_columns.json'
    elif base_name == 'tangara_mata_atlantica':
        json_name = 'tangara_columns.json'
    else:
        json_name = f'{base_name}_columns.json'
    input_json_path = os.path.join(rawdata_dir, json_name)

    output_main_dir = os.path.join(script_dir, '..', 'Results')
    results_dir = os.path.join(output_main_dir, base_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    # --- LEITURA E PREPARAÇÃO DOS DADOS ---
    if not os.path.exists(input_csv_path) or not os.path.exists(input_json_path):
        print("ERRO: Um dos arquivos de entrada não foi encontrado.")
        return

    with open(input_json_path, 'r') as f:
        columns_map = json.load(f)

    # Use os índices das colunas
    lon_col = columns_map['LONGITUDE']
    lat_col = columns_map['LATITUDE']

    data = pd.read_csv(input_csv_path, header=0)  # Usa o header do CSV

    # Limpeza: só remove linhas com valores inválidos
    data[lon_col] = pd.to_numeric(data[lon_col], errors='coerce')
    data[lat_col] = pd.to_numeric(data[lat_col], errors='coerce')
    data_cleaned = data.dropna(subset=[lon_col, lat_col])
    data_cleaned = data_cleaned[
        (data_cleaned[lon_col] >= -180) & (data_cleaned[lon_col] <= 180) &
        (data_cleaned[lat_col] >= -90) & (data_cleaned[lat_col] <= 90)
    ]

    if data_cleaned.empty:
        print("Nenhum dado válido para plotar.")
        return

    # Carrega textos do idioma
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    if language == "PT_BR":
        labels = "Localizações"
    else:
        labels = "Locations"

    plt.figure(figsize=(15, 12))
    plt.scatter(
        data_cleaned[lon_col],
        data_cleaned[lat_col],
        alpha=0.2,
        s=10,
        label=labels
    )

    # Títulos e legendas
    plt.title(lang["grafico_dispersao_geral_jaguar_titulo"])
    #plt.suptitle(lang["grafico_dispersao_geral_jaguar_subtitulo"])
    #plt.figtext(0.5, 0.01, lang["grafico_dispersao_geral_jaguar_rodape"], ha='center', fontsize=10)

    plt.xlabel(lang["xlabel_dispersao_geral"])
    plt.ylabel(lang["ylabel_dispersao_geral"])
    plt.grid(True)
    plt.legend()

    output_file_png = os.path.join(cluster_output_dir, f'dispersao_geral_{base_name}.png')
    plt.savefig(output_file_png, dpi=300)
    plt.close()
    print(f"Gráfico Jaguar salvo em: {output_file_png}")

def plot_tangara(file_rawdata=None):
    """
    Este script carrega a base de dados original, limpa os dados e gera
    um gráfico de dispersão para os dados da tangará.
    """
    
    # --- CAMINHOS DE ENTRADA E SAÍDA (AUTOMÁTICOS) ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    rawdata_dir = os.path.join(script_dir, '..', '..', 'rawdata')
    
    base_name = extract_folder_name(file_rawdata) if file_rawdata else 'tangara_mata_atlantica'
    input_csv_path = os.path.join(rawdata_dir, f'{base_name}.csv')
    if base_name == 'tangara_mata_atlantica':
        json_name = 'tangara_columns.json'
    elif base_name == 'jaguar_mamiraua':
        json_name = 'jaguar_columns.json'
    else:
        json_name = f'{base_name}_columns.json'
    input_json_path = os.path.join(rawdata_dir, json_name)

    output_main_dir = os.path.join(script_dir, '..', 'Results')
    results_dir = os.path.join(output_main_dir, base_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    # --- LEITURA E PREPARAÇÃO DOS DADOS ---
    if not os.path.exists(input_csv_path) or not os.path.exists(input_json_path):
        print("ERRO: Um dos arquivos de entrada não foi encontrado.")
        return

    with open(input_json_path, 'r') as f:
        columns_map = json.load(f)

    # Use os índices das colunas
    lon_col = columns_map['LONGITUDE']
    lat_col = columns_map['LATITUDE']

    data = pd.read_csv(input_csv_path, header=0)  # Usa o header do CSV

    # Limpeza: só remove linhas com valores inválidos
    data[lon_col] = pd.to_numeric(data[lon_col], errors='coerce')
    data[lat_col] = pd.to_numeric(data[lat_col], errors='coerce')
    data_cleaned = data.dropna(subset=[lon_col, lat_col])
    data_cleaned = data_cleaned[
        (data_cleaned[lon_col] >= -180) & (data_cleaned[lon_col] <= 180) &
        (data_cleaned[lat_col] >= -90) & (data_cleaned[lat_col] <= 90)
    ]

    if data_cleaned.empty:
        print("Nenhum dado válido para plotar.")
        return

    # Carrega textos do idioma
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    if language == "PT_BR":
        labels = "Localizações"
    else:
        labels = "Locations"

    plt.figure(figsize=(15, 12))
    plt.scatter(
        data_cleaned[lon_col],
        data_cleaned[lat_col],
        alpha=0.2,
        s=10,
        label=labels
    )
    
    plt.title(lang["grafico_dispersao_geral_tangara_titulo"])
    
    #plt.suptitle(lang["grafico_dispersao_geral_tangara_subtitulo"])
    #plt.figtext(0.5, 0.01, lang["grafico_dispersao_geral_tangara_rodape"], ha='center', fontsize=10)
    plt.xlabel(lang["xlabel_dispersao_geral"])
    plt.ylabel(lang["ylabel_dispersao_geral"])
    plt.grid(True)
    plt.legend()
    output_file_png = os.path.join(cluster_output_dir, f'dispersao_geral_{base_name}.png')
    plt.savefig(output_file_png, dpi=300)
    plt.close()
    print(f"Gráfico Tangara salvo em: {output_file_png}")

def run_all_dispersion(file_rawdata=None):
    '''
    if file_rawdata:
        base_name = extract_folder_name(file_rawdata)
        if base_name.startswith('jaguar'):
            plot_jaguar(file_rawdata)
        elif base_name.startswith('tangara'):
            plot_tangara(file_rawdata)
        else:
            # Tenta ambos com base no nome detectado
            plot_jaguar(file_rawdata)
    else:
        plot_jaguar()
        plot_tangara()
    '''
    
    plot_jaguar('jaguar_mamiraua')
    plot_tangara('tangara_mata_atlantica')

if __name__ == "__main__":
    run_all_dispersion()