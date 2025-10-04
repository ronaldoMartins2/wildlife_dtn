import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import json
from Common.utils import (
    create_clusterization_results,
    read_field_from_json
)

# plot_kmeans_som_birch_mean_shift 

# python3 -m venv venv
# source ./venv/bin/activate

# python3 plot_kmeans_som_birch_mean_shift.py 94 

def run(current_animal):
    # --- CONSTRUÇÃO CORRETA DOS CAMINHOS ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Navega para o diretório pai ('..') e depois entra em 'Results' e 'Clusterization'
    results_dir = os.path.join(script_dir, '..', 'Results', 'Clusterization')
    
    # Garante que a pasta de destino para o gráfico exista
    create_clusterization_results(results_dir)

    # Define os caminhos completos para cada arquivo de dados
    kmeans_data_file = os.path.join(results_dir, f"clusters_kmeans_{current_animal}.csv")
    som_data_file = os.path.join(results_dir, f"clusters_som_{current_animal}.csv")
    birch_data_file = os.path.join(results_dir, f"clusters_birch_map_{current_animal}.csv")
    mean_shift_data_file = os.path.join(results_dir, f"clusters_mean_shift_map_{current_animal}.csv")

    # --- LEITURA DOS DADOS COM VERIFICAÇÃO ---
    data_sources = {
        "K-Means": kmeans_data_file,
        "SOM": som_data_file,
        "BIRCH": birch_data_file,
        "Mean-Shift": mean_shift_data_file
    }
    
    loaded_data = {}
    for name, path in data_sources.items():
        # AQUI ESTÁ A VERIFICAÇÃO: só tenta ler o arquivo se ele existir
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, header=None)
                if df.empty:
                    print(f"Aviso: Arquivo de dados para '{name}' está vazio para o animal {current_animal}. Pulando plotagem.")
                    continue

                if df.shape[1] == 2:
                    df.columns = ['longitude', 'latitude']
                elif df.shape[1] == 3:
                    df.columns = ['longitude', 'latitude', 'label']
                loaded_data[name] = df
            except Exception as e:
                print(f"Erro ao ler o arquivo {path}: {e}")
        else:
            print(f"Aviso: Arquivo de dados para '{name}' não encontrado para o animal {current_animal}. Pulando plotagem.")

    if not loaded_data:
        print(f"Nenhum dado de clusterização encontrado para o animal {current_animal}. Não é possível gerar o gráfico.")
        return

    # --- GERAÇÃO DO GRÁFICO ---
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    plt.figure(figsize=(12, 8))

    # Plota os dados de cada algoritmo que foi carregado com sucesso
    if "K-Means" in loaded_data:
        plt.scatter(loaded_data["K-Means"]['longitude'], loaded_data["K-Means"]['latitude'], label='K-Means', marker='o', s=100)
    if "SOM" in loaded_data:
        plt.scatter(loaded_data["SOM"]['longitude'], loaded_data["SOM"]['latitude'], label='SOM', marker='x', s=100)
    if "BIRCH" in loaded_data:
        plt.scatter(loaded_data["BIRCH"]['longitude'], loaded_data["BIRCH"]['latitude'], label='BIRCH', marker='s', s=50, alpha=0.7)
    if "Mean-Shift" in loaded_data:
        plt.scatter(loaded_data["Mean-Shift"]['longitude'], loaded_data["Mean-Shift"]['latitude'], label='Mean-Shift', marker='^', s=50, alpha=0.7)

    plt.xlabel(lang["xlabel_kmeans_individual"])
    plt.ylabel(lang["ylabel_kmeans_individual"])
    
    title_key = "grafico_comparacao_cluster"
    title_text = lang.get(title_key, "Comparativo de Clusterização para a Onça")
    plt.title(f"{title_text} {current_animal}")
    
    plt.legend()
    plt.grid(True)
    
    output_file_name = os.path.join(results_dir, f'onca_{current_animal}_clusterization_comparison.png')
    plt.savefig(output_file_name)
    plt.close()
    print(f"Gráfico comparativo salvo em: {output_file_name}")

def run_mock():
    current_animal = sys.argv [1]

    run( current_animal )