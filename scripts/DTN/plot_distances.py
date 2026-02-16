import argparse
import pathlib
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

r"""
Reads summary files:
    distancias_resumo_kmeans_8_centroides.csv
    distancias_resumo_birch_16_centroides.csv
    distancias_resumo_som_32_centroides.csv

Generates:
    - mean_distance_by_method.png
    - min_distance_by_method.png
    - max_distance_by_method.png

Now supports REAL centroid IDs from the CSV (“centroid_id” column).
python scripts\DTN\plot_distances.py --stats-dir scripts\Results\jaguar_mamiraua --output-dir scripts\Results\jaguar_mamiraua\plots

Adaptação visual: Gera gráficos de barras agrupadas (Grouped Bar Chart)
com estilo similar aos gráficos de tempo de retorno (cores, grid, layout).
"""

# ==========================
# CONFIGURAÇÃO DE ESTILO
# ==========================
# Cores consistentes com o script anterior
COLORS = {
    "BIRCH": "#1f77b4",   # Azul
    "K-Means": "#ff7f0e", # Laranja
    "SOM": "#2ca02c"      # Verde
}

# -----------------------------------------------
# Parse method and number of centroids from name
# -----------------------------------------------
def parse_method_n(path: pathlib.Path):
    parts = path.stem.split("_")
    # Ex: distancias_resumo_kmeans_8_centroides
    # partes: [distancias, resumo, kmeans, 8, centroides]
    method = parts[2]
    n = int(parts[3])
    return method, n

def pretty_method_name(m):
    m = m.lower()
    if m == "kmeans": return "K-Means"
    if m == "birch":  return "BIRCH"
    if m == "som":    return "SOM"
    return m.capitalize()

# -----------------------------------------------
# Build summary table of the 3 metrics
# -----------------------------------------------
def build_summary(stats_dir: pathlib.Path) -> pd.DataFrame:
    files = list(stats_dir.glob("distancias_resumo_*_centroides.csv"))
    if not files:
        raise RuntimeError("Nenhum arquivo distancias_resumo_*_centroides.csv encontrado.")

    rows = []

    for f in files:
        method, n = parse_method_n(f)
        df = pd.read_csv(f)

        # ensure centroid_id is integer
        if "centroid_id" in df.columns:
            df["centroid_id"] = df["centroid_id"].astype(int)

        rows.append({
            "method_raw": method,
            "method": pretty_method_name(method),
            "n_centroids": n,
            # Agregações globais
            "mean_m": df["mean_m"].mean(),
            "min_m": df["min_m"].min(),
            "max_m": df["max_m"].max(),
        })

    summary = pd.DataFrame(rows)
    summary = summary.sort_values(["method", "n_centroids"])
    summary = summary.reset_index(drop=True)
    return summary

# -----------------------------------------------
# Create Grouped Barplot (Estilo Novo)
# -----------------------------------------------
def make_grouped_barplot(summary, value_column, title, ylabel, output_path):
    """
    Gera um gráfico de barras agrupadas:
      - Eixo X: Número de centróides (8, 16, 32)
      - Barras: Métodos (coloridos)
    """
    methods = sorted(summary["method"].unique())
    ns = sorted(summary["n_centroids"].unique())
    
    # Prepara dados para plotagem
    # data[method] = [valor_para_k8, valor_para_k16, valor_para_k32]
    data = {m: [] for m in methods}
    
    for n in ns:
        df_n = summary[summary["n_centroids"] == n]
        for m in methods:
            val = df_n.loc[df_n["method"] == m, value_column]
            if not val.empty:
                data[m].append(val.iloc[0])
            else:
                data[m].append(0) # ou np.nan

    x = np.arange(len(ns))  # posições dos grupos no eixo X
    width = 0.8 / len(methods)  # largura das barras

    fig, ax = plt.subplots(figsize=(10, 6))

    # Plota as barras para cada método
    for i, m in enumerate(methods):
        cor = COLORS.get(m, "#333333") # Cor padrão cinza se não achar no dict
        barras = ax.bar(
            x + i * width, 
            data[m], 
            width, 
            label=m, 
            color=cor, 
            zorder=3,
            alpha=0.9
        )

        # Rótulos em cima das barras
        for bar in barras:
            height = bar.get_height()
            if height > 0:
                ax.annotate(
                    f'{height:.1f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom',
                    fontsize=9, fontweight='bold'
                )

    # Formatação dos eixos e grid
    ax.set_xlabel("Number of centroids", fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(title, fontsize=14, pad=15)
    
    # Centraliza os ticks do eixo X no meio do grupo de barras
    ax.set_xticks(x + width * (len(methods) - 1) / 2)
    ax.set_xticklabels(ns)
    
    # Grid apenas horizontal e atrás das barras
    ax.grid(True, axis='y', linestyle='--', alpha=0.5, zorder=0)
    
    # Legenda
    ax.legend(title="Method", loc="upper left", bbox_to_anchor=(1, 1))

    # Ajuste de margens para a legenda caber
    plt.tight_layout()
    
    # Salvar
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"✔ Gráfico salvo: {output_path}")

# -----------------------------------------------
# EXTRA: plot por centróide real (Mantido original, mas ajustado cor)
# -----------------------------------------------
def plot_by_centroid(stats_dir, output_dir):
    files = list(stats_dir.glob("distancias_resumo_*_centroides.csv"))
    if not files:
        return

    for f in files:
        method, n = parse_method_n(f)
        df = pd.read_csv(f)
        df = df.sort_values("centroid_id")

        x = df["centroid_id"]
        mean_m = df["mean_m"]
        
        # Cor baseada no método
        met_pretty = pretty_method_name(method)
        cor = COLORS.get(met_pretty, "steelblue")

        plt.figure(figsize=(12, 6))
        plt.bar(x, mean_m, alpha=0.85, color=cor, zorder=3)
        plt.grid(True, axis='y', linestyle='--', alpha=0.5, zorder=0)
        
        plt.title(f"Mean Distance per Centroid — {met_pretty} ({n} centroids)")
        plt.xlabel("Real centroid ID")
        plt.ylabel("Mean distance (m)")
        
        plt.tight_layout()
        out = output_dir / f"mean_distance_per_centroid_{method}_{n}.png"
        plt.savefig(out, dpi=300)
        plt.close()

        print(f"✔ Gráfico por centróide salvo: {out}")

# -----------------------------------------------
# MAIN
# -----------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate plots for Task 3.B (Styled grouped bars).")
    parser.add_argument("--stats-dir", required=True, help="Directory containing distancias_resumo_*_centroides.csv")
    parser.add_argument("--output-dir", required=False, help="Output directory for plots")
    parser.add_argument("--per-centroid", action="store_true", help="Generate individual plots per centroid ID")
    args = parser.parse_args()

    stats_dir = pathlib.Path(args.stats_dir)
    out_dir = pathlib.Path(args.output_dir) if args.output_dir else stats_dir
    out_dir.mkdir(exist_ok=True, parents=True)

    summary = build_summary(stats_dir)

    # Global plots (Agora usando a função make_grouped_barplot)
    make_grouped_barplot(
        summary, 
        "mean_m", 
        "Mean Distance to Centroids", 
        "Mean distance (m)",
        out_dir / "mean_distance_by_method.png"
    )

    make_grouped_barplot(
        summary, 
        "min_m", 
        "Minimum Distance to Centroids", 
        "Minimum distance (m)",
        out_dir / "min_distance_by_method.png"
    )

    make_grouped_barplot(
        summary, 
        "max_m", 
        "Maximum Distance to Centroids", 
        "Maximum distance (m)",
        out_dir / "max_distance_by_method.png"
    )

    # Optional per‐centroid plots
    if args.per_centroid:
        plot_by_centroid(stats_dir, out_dir)

    print("\n✔ Finalizado.")

if __name__ == "__main__":
    main()