import pandas as pd
import matplotlib.pyplot as plt
import os

from Common.utils import (
    results_folder
)

def run(current_animal, file_rawdata_name):


    # === CONFIGURAÇÕES ===
    #csv_path = "scripts/Data_preparation/map_jaguar_mamiraua.csv"  # Atualize se necessário
    results_dir = results_folder( file_rawdata_name )
    
    csv_path = os.path.join(results_dir, f'map_{current_animal}.csv')

    df = pd.read_csv( csv_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])

    timestamp_col = "Timestamp"  # Altere para o nome exato da coluna de tempo, ex: 'datetime'
    #output_dir = "scripts/Results/jaguar_mamiraua"
    output_dir = results_dir
    os.makedirs(output_dir, exist_ok=True)

    # === CARREGAR E PROCESSAR DADOS ===
    try:
        #df = pd.read_csv(csv_path)
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        df = df.sort_values(timestamp_col)
        df['gap_seconds'] = df[timestamp_col].diff().dt.total_seconds()
        gaps = df['gap_seconds'].dropna()
    except Exception as e:
        print(f"Erro ao processar os dados: {e}")
        exit()

    # === CÁLCULO DO GAP MÉDIO ===
    gap_medio = gaps.mean()
    print(f"Média de tempo entre registros: {gap_medio:.2f} segundos")

    # === HISTOGRAMA ===
    hist_path = os.path.join(output_dir, f"histograma_gaps_animal_{current_animal}.png")
    plt.figure(figsize=(10, 6))
    plt.hist(gaps, bins=50, color='skyblue', edgecolor='black')
    plt.title("Histograma dos Gaps entre Registros")
    plt.xlabel("Intervalo de tempo (segundos)")
    plt.ylabel("Frequência")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(hist_path)
    print(f"Histograma salvo em: {hist_path}")

    # === hiperparameters.txt ===
    hiper_file = os.path.join(output_dir, "hiperparameters.txt")
    append_text = f"\nAnimal {current_animal} - Gap médio (s): {gap_medio:.2f}\nHistograma salvo em: {hist_path}\n"

    with open(hiper_file, "a") as f:
        f.write(append_text)

    print(f"Informações adicionadas em: {hiper_file}")

