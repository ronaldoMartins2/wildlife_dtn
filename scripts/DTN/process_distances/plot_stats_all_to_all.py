import argparse
import pathlib
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

r"""
Script to plot metrics from 'stats_all_to_all.csv'.
Generates Grouped Bar Charts for Mean, Min, Max, and Std Dev distances.

python scripts\DTN\process_distances\plot_stats_all_to_all.py --input-file scripts\Results\jaguar_mamiraua\distances_all_to_all\stats_all_to_all.csv --output-dir scripts\Results\jaguar_mamiraua\distances_all_to_all                           

"""

# ==========================
# CONFIGURAÇÃO DE ESTILO
# ==========================
# Cores definidias apenas para os algoritmos existentes
COLORS = {
    "BIRCH": "#1f77b4",   # Azul
    "K-Means": "#ff7f0e", # Laranja
    "SOM": "#2ca02c"      # Verde
}

def pretty_method_name(m):
    """
    Formata o nome do algoritmo para exibição no gráfico.
    """
    if pd.isna(m): return "Unknown"
    m = str(m).lower()
    if m == "kmeans": return "K-Means"
    if m == "birch":  return "BIRCH"
    if m == "som":    return "SOM"
    return m.capitalize()

def load_data(csv_path):
    """
    Carrega o arquivo stats_all_to_all.csv e prepara os dados.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"File not found: {csv_path}")

    df = pd.read_csv(csv_path)
    
    # Padroniza nomes dos métodos
    df['method_display'] = df['algorithm'].apply(pretty_method_name)
    
    # Garante que num_clusters é numérico para ordenação correta
    df['num_clusters'] = pd.to_numeric(df['num_clusters'], errors='coerce')
    df = df.dropna(subset=['num_clusters']) # Remove entradas sem k definido
    df['num_clusters'] = df['num_clusters'].astype(int)
    
    # Ordena por método e número de clusters
    df = df.sort_values(by=['method_display', 'num_clusters'])
    
    return df

def make_grouped_barplot(df, value_column, title, ylabel, output_path):
    """
    Gera o gráfico de barras agrupadas.
    Eixo X: Número de clusters.
    Grupos: Algoritmos.
    """
    methods = sorted(df["method_display"].unique())
    clusters = sorted(df["num_clusters"].unique())
    
    # Prepara estrutura de dados para o matplotlib
    data = {m: [] for m in methods}
    
    for k in clusters:
        df_k = df[df["num_clusters"] == k]
        for m in methods:
            val = df_k.loc[df_k["method_display"] == m, value_column]
            if not val.empty:
                data[m].append(val.iloc[0])
            else:
                data[m].append(0)

    x = np.arange(len(clusters))  # Posições no eixo X
    width = 0.8 / len(methods)    # Largura das barras

    fig, ax = plt.subplots(figsize=(10, 6))

    # Loop para criar as barras de cada método
    for i, m in enumerate(methods):
        color = COLORS.get(m, "#7f7f7f") # Cinza se não achado
        bars = ax.bar(
            x + i * width, 
            data[m], 
            width, 
            label=m, 
            color=color, 
            zorder=3,
            alpha=0.9,
            edgecolor='white'
        )

        # Anotações (valores acima das barras)
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.annotate(
                    f'{height:.1f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), 
                    textcoords="offset points",
                    ha='center', va='bottom',
                    fontsize=8, fontweight='bold', color='#333333'
                )

    # Configurações visuais do gráfico
    ax.set_xlabel("Number of Centroids (K)", fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(title, fontsize=14, pad=20)
    
    ax.set_xticks(x + width * (len(methods) - 1) / 2)
    ax.set_xticklabels(clusters)
    
    ax.grid(True, axis='y', linestyle='--', alpha=0.5, zorder=0)
    
    # Legenda fora do gráfico
    ax.legend(title="Algorithm", loc="upper left", bbox_to_anchor=(1, 1))

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"-> Plot saved: {output_path}")

def main():
    # Configuração de argumentos da linha de comando
    parser = argparse.ArgumentParser(description="Plot stats from All-to-All analysis.")
    parser.add_argument("--input-file", required=True, help="Path to stats_all_to_all.csv")
    parser.add_argument("--output-dir", required=True, help="Folder to save the plots")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Carrega dados
    try:
        df = load_data(args.input_file)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    print(f"Generating plots for {len(df)} scenarios...")

    # 1. Distância Média
    make_grouped_barplot(
        df, 
        value_column="mean_distance_m",
        title="Mean Distance (All Animals to All Centroids)",
        ylabel="Mean Distance (m)",
        output_path=os.path.join(args.output_dir, "plot_all_to_all_mean.png")
    )

    # 2. Distância Mínima (Adicionado)
    make_grouped_barplot(
        df, 
        value_column="min_distance_m",
        title="Minimum Distance (All Animals to All Centroids)",
        ylabel="Min Distance (m)",
        output_path=os.path.join(args.output_dir, "plot_all_to_all_min.png")
    )

    # 3. Distância Máxima
    make_grouped_barplot(
        df, 
        value_column="max_distance_m",
        title="Max Distance (All Animals to All Centroids)",
        ylabel="Max Distance (m)",
        output_path=os.path.join(args.output_dir, "plot_all_to_all_max.png")
    )

    # 4. Desvio Padrão
    make_grouped_barplot(
        df, 
        value_column="std_deviation_m",
        title="Std. Deviation (All Animals to All Centroids)",
        ylabel="Standard Deviation (m)",
        output_path=os.path.join(args.output_dir, "plot_all_to_all_std.png")
    )

    print("\nDone.")

if __name__ == "__main__":
    main()