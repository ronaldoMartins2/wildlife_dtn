import argparse
import pathlib
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import glob

r"""
Script to generate a SINGLE combined Cumulative Distribution Function (CDF) plot.
All scenarios (Algorithm + K) are plotted in the same figure for global comparison.

python scripts\DTN\process_distances\plot_cdf.py --input-dir scripts\Results\jaguar_mamiraua\distances_all_to_all --output-dir scripts\Results\jaguar_mamiraua\distances_all_to_all

python scripts\DTN\process_distances\plot_cdf.py --input-dir scripts\Results\jaguar_mamiraua\distances_to_own_centroid --output-dir scripts\Results\jaguar_mamiraua\distances_to_own_centroid
"""

# ==========================
# CONFIGURAÇÃO DE ESTILO
# ==========================
# Cores para distinguir os Algoritmos
COLORS = {
    "BIRCH": "#1f77b4",   # Azul
    "K-Means": "#ff7f0e", # Laranja
    "SOM": "#2ca02c"      # Verde
}

# Estilos de linha para distinguir o número de clusters (K)
# Adapte conforme os Ks que você usa no seu projeto
LINE_STYLES = {
    8: '-',      # Linha Sólida
    16: '--',    # Tracejado
    32: ':',     # Pontilhado
    64: '-.'     # Traço-ponto
}

def pretty_method_name(m):
    if pd.isna(m): return "Unknown"
    m = str(m).lower()
    if m == "kmeans": return "K-Means"
    if m == "birch":  return "BIRCH"
    if m == "som":    return "SOM"
    return m.capitalize()

def extract_metadata(filename):
    """
    Extrai algoritmo e K do nome do arquivo.
    """
    parts = filename.split('_')
    k = -1
    algo = 'Unknown'
    
    lower_name = filename.lower()
    if 'kmeans' in lower_name: algo = 'kmeans'
    elif 'birch' in lower_name: algo = 'birch'
    elif 'som' in lower_name: algo = 'som'
    
    for part in parts:
        if part.isdigit():
            k = int(part)
            break 
            
    return algo, k

def load_detailed_distances(folder_path):
    """
    Carrega todos os dados e retorna uma lista plana de dicionários para facilitar o plot.
    """
    all_series = [] # Lista de tuplas: (algo, k, array_distancias)
    
    files = glob.glob(os.path.join(folder_path, "*.csv"))
    print(f"Scanning {len(files)} files in {folder_path}...")
    
    count_files = 0
    for file_path in files:
        if "stats_" in os.path.basename(file_path) or "resumo" in os.path.basename(file_path):
            continue
            
        try:
            # Ler apenas a coluna necessária economiza memória
            df = pd.read_csv(file_path, usecols=['distance_meters'])
            distances = df['distance_meters'].dropna().values
            
            if len(distances) == 0:
                continue
                
            algo, k = extract_metadata(os.path.basename(file_path))
            
            if k == -1 or algo == 'Unknown':
                continue

            all_series.append({
                'algo': algo,
                'k': k,
                'distances': distances
            })
            count_files += 1
            
        except Exception as e:
            # Pode ocorrer se a coluna 'distance_meters' não existir no CSV
            # print(f"Skipping {os.path.basename(file_path)}: {e}")
            pass
            
    print(f"Successfully loaded data from {count_files} files.")
    return all_series

def plot_cdf(all_series, output_file, x_limit=None):
    """
    Plota todas as séries no mesmo gráfico.
    """
    plt.figure(figsize=(12, 7))
    
    # Ordena para garantir que a legenda fique organizada (ex: K-Means 8, depois K-Means 16...)
    # Ordena primeiro por Algo, depois por K
    all_series.sort(key=lambda x: (x['algo'], x['k']))
    
    for item in all_series:
        algo = item['algo']
        k = item['k']
        distances = item['distances']
        
        # Prepara dados CDF
        sorted_data = np.sort(distances)
        yvals = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
        
        # Configuração Visual
        algo_pretty = pretty_method_name(algo)
        color = COLORS.get(algo_pretty, "#333333")
        linestyle = LINE_STYLES.get(k, '-') # Se K não estiver no dict, usa sólido padrão
        
        label_text = f"{algo_pretty} (K={k})"
        
        plt.plot(sorted_data, yvals, 
                 label=label_text, 
                 color=color, 
                 linestyle=linestyle, 
                 linewidth=2, 
                 alpha=0.8)

    # Decorações do gráfico
    plt.title("CDF Comparison: Distance to Centroid (All Scenarios)", fontsize=14)
    plt.xlabel("Distance (meters)", fontsize=12)
    plt.ylabel("Cumulative Probability", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.5)
    
    # Limite do eixo X (Zoom) se desejado, pois outliers podem esticar muito o gráfico
    if x_limit:
        plt.xlim(0, x_limit)
        plt.title(f"CDF Comparison (Zoom up to {x_limit}m)", fontsize=14)
    
    plt.legend(title="Scenario", bbox_to_anchor=(1.02, 1), loc='upper left')
    plt.tight_layout()
    
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"-> Saved plot: {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Generate a single Combined CDF plot.")
    parser.add_argument("--input-dir", required=True, help="Folder containing the detailed CSV files")
    parser.add_argument("--output-dir", required=True, help="Folder to save the plots")
    # Opcional: Limitar eixo X para ver melhor o início da curva
    parser.add_argument("--max-dist", type=float, help="Optional: Max distance (m) to show on X-axis (zoom)", default=None)
    
    args = parser.parse_args()
    
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        
    data = load_detailed_distances(args.input_dir)
    
    if not data:
        print("No valid data found.")
        return
    
    # Nome do arquivo de saída
    filename = "cdf_all_scenarios.png"
    if args.max_dist:
        filename = f"cdf_zoom_{int(args.max_dist)}m.png"
        
    output_path = os.path.join(args.output_dir, filename)
    
    plot_cdf(data, output_path, args.max_dist)
    
    print("\nProcessing complete.")

if __name__ == "__main__":
    main()