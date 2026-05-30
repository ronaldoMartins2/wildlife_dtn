import argparse
import pathlib
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

r"""
Script to plot metrics from 'stats_animal_to_own_centroid.csv'.
Generates Grouped Bar Charts for Mean, Min, Max, Std Dev and Valid Points.

python scripts\DTN\process_distances\plot_stats_to_own_centroid.py --input-file scripts\Results\jaguar_mamiraua\distances_to_own_centroid\stats_animal_to_own_centroid.csv --output-dir scripts\Results\jaguar_mamiraua\distances_to_own_centroid
"""

# ==========================
# CONFIGURAÇÃO DE ESTILO
# ==========================
COLORS = {
    "BIRCH": "#1f77b4",   # Azul
    "K-Means": "#ff7f0e", # Laranja
    "SOM": "#2ca02c"      # Verde
}

def pretty_method_name(m):
    """
    Formata o nome do algoritmo para exibição.
    """
    if pd.isna(m): return "Unknown"
    m = str(m).lower()
    if m == "kmeans": return "K-Means"
    if m == "birch":  return "BIRCH"
    if m == "som":    return "SOM"
    return m.capitalize()

def load_data(csv_path):
    """
    Lê o arquivo de resumo e trata tipos de dados.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"File not found: {csv_path}")

    df = pd.read_csv(csv_path)
    
    # Padroniza nomes
    df['method_display'] = df['algorithm'].apply(pretty_method_name)
    
    # Garante que clusters é inteiro
    df['clusters'] = pd.to_numeric(df['clusters'], errors='coerce')
    df = df.dropna(subset=['clusters'])
    df['clusters'] = df['clusters'].astype(int)
    
    # Ordena
    df = df.sort_values(by=['method_display', 'clusters'])
    
    return df

def make_grouped_barplot(df, value_column, title, ylabel, output_path):
    """
    Função genérica para plotar barras agrupadas.
    """
    methods = sorted(df["method_display"].unique())
    clusters = sorted(df["clusters"].unique())
    
    data = {m: [] for m in methods}
    
    for k in clusters:
        df_k = df[df["clusters"] == k]
        for m in methods:
            val = df_k.loc[df_k["method_display"] == m, value_column]
            if not val.empty:
                data[m].append(val.iloc[0])
            else:
                data[m].append(0)

    x = np.arange(len(clusters))
    width = 0.8 / len(methods)

    fig, ax = plt.subplots(figsize=(10, 6))

    hatches_config = {
        "BIRCH": "//",
        "K-Means": "\\\\",
        "SOM": "//\\\\"
    }

    for i, m in enumerate(methods):
        hatch_style = hatches_config.get(m, "")
        color = COLORS.get(m, "#7f7f7f")
        bars = ax.bar(
            x + i * width, 
            data[m], 
            width, 
            label=m, 
            color=color, 
            zorder=3,
            alpha=0.9,
            edgecolor='black',
            hatch=hatch_style
        )

        # Rótulos de valor
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                # Ajuste de formatação dependendo da magnitude do valor
                # Se for contagem (ex: 5000) usa inteiro, se for distância (ex: 12.5) usa float
                label_fmt = '{:.0f}' if height > 1000 else '{:.1f}'
                ax.annotate(
                    label_fmt.format(height),
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), 
                    textcoords="offset points",
                    ha='center', va='bottom',
                    fontsize=8, fontweight='bold', color='#333333'
                )

    ax.set_xlabel("Number of Clusters (K)", fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(title, fontsize=14, pad=20)
    
    ax.set_xticks(x + width * (len(methods) - 1) / 2)
    ax.set_xticklabels(clusters)
    
    ax.grid(True, axis='y', linestyle='--', alpha=0.5, zorder=0)
    ax.legend(title="Algorithm", loc="upper left", bbox_to_anchor=(1, 1))

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"-> Plot saved: {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Plot stats from Animal-to-Own-Centroid analysis.")
    parser.add_argument("--input-file", required=True, help="Path to stats_animal_to_own_centroid.csv")
    parser.add_argument("--output-dir", required=True, help="Folder to save the plots")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    try:
        df = load_data(args.input_file)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    print(f"Generating plots for {len(df)} scenarios...")

    # 1. Distância Média
    make_grouped_barplot(
        df, 
        value_column="mean",
        title="Mean Distance to Assigned Centroid",
        ylabel="Mean Distance (m)",
        output_path=os.path.join(args.output_dir, "plot_own_centroid_mean.png")
    )

    # 2. Distância Mínima
    make_grouped_barplot(
        df, 
        value_column="min",
        title="Minimum Distance to Assigned Centroid",
        ylabel="Min Distance (m)",
        output_path=os.path.join(args.output_dir, "plot_own_centroid_min.png")
    )

    # 3. Distância Máxima (ADICIONADO AGORA)
    make_grouped_barplot(
        df, 
        value_column="max",
        title="Maximum Distance to Assigned Centroid",
        ylabel="Max Distance (m)",
        output_path=os.path.join(args.output_dir, "plot_own_centroid_max.png")
    )

    # 4. Desvio Padrão
    make_grouped_barplot(
        df, 
        value_column="std",
        title="Std. Deviation (Distance to Assigned Centroid)",
        ylabel="Standard Deviation (m)",
        output_path=os.path.join(args.output_dir, "plot_own_centroid_std.png")
    )

    print("\nDone.")

if __name__ == "__main__":
    main()