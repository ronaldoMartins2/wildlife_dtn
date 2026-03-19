import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np

"""
Gera gráficos e tabelas comparativas para as métricas derivadas de
tempo_retorno_vizinhanca.py:

1) Mean return time (h)  [mean_ret_h]
2) Mean residence time per visit (h)  [mean_res_h]
3) Mean number of visits  [n_visitas]

Para cada combinação (método, número de centróides) listada em CONFIG.

Entrada:
    - Vários CSVs gerados por tempo_retorno_vizinhanca.py
      (um para cada combinação de método e nº de centróides).

Saídas (em OUT_DIR):
    - table_mean_return_time_hours.csv
    - table_mean_residence_time_hours.csv
    - table_mean_number_of_visits.csv
    - mean_return_time_barplot.png
    - mean_return_time_lineplot.png
    - mean_residence_time_barplot.png
    - mean_number_of_visits_barplot.png

Gera gráficos e tabelas comparativas com INTERVALOS DE CONFIANÇA (95%).
"""

# ==========================
# CONFIGURAÇÃO DO USUÁRIO
# ==========================

# Nome do conjunto de dados (vai pro título do gráfico)
DATASET_NAME = "Jaguars dataset"
# DATASET_NAME = "Tangaras dataset"

# Lista com TODAS as combinações (método, nº de centróides, caminho do CSV)
# Ajuste os paths para os seus arquivos reais.

animal = "jaguar_mamiraua"
# animal = "tangara_mata_atlantica"

# Ajuste conforme seus arquivos
CONFIG = [
    {"method": "K-Means",   "k": 8,  "path": rf"scripts\Results\{animal}\points_kmeans_mapping_8_centroids_rawdata_retorno_250m_tc120min_vizinhanca.csv"},
    {"method": "K-Means",   "k": 16, "path": rf"scripts\Results\{animal}\points_kmeans_mapping_16_centroids_rawdata_retorno_250m_tc120min_vizinhanca.csv"},
    {"method": "K-Means",   "k": 32, "path": rf"scripts\Results\{animal}\points_kmeans_mapping_32_centroids_rawdata_retorno_250m_tc120min_vizinhanca.csv"},

    {"method": "SOM",       "k": 8,  "path": rf"scripts\Results\{animal}\points_som_mapping_8_centroids_rawdata_retorno_250m_tc120min_vizinhanca.csv"},
    {"method": "SOM",       "k": 16, "path": rf"scripts\Results\{animal}\points_som_mapping_16_centroids_rawdata_retorno_250m_tc120min_vizinhanca.csv"},
    {"method": "SOM",       "k": 32, "path": rf"scripts\Results\{animal}\points_som_mapping_32_centroids_rawdata_retorno_250m_tc120min_vizinhanca.csv"},

    {"method": "BIRCH",     "k": 8,  "path": rf"scripts\Results\{animal}\points_birch_mapping_8_centroids_rawdata_retorno_250m_tc120min_vizinhanca.csv"},
    {"method": "BIRCH",     "k": 16, "path": rf"scripts\Results\{animal}\points_birch_mapping_16_centroids_rawdata_retorno_250m_tc120min_vizinhanca.csv"},
    {"method": "BIRCH",     "k": 32, "path": rf"scripts\Results\{animal}\points_birch_mapping_32_centroids_rawdata_retorno_250m_tc120min_vizinhanca.csv"},
]

OUT_DIR = Path(rf"scripts\Results\{animal}\figuras_tempo_retorno")

# ==========================
# FUNÇÕES DE MÉTRICAS E ESTATÍSTICA
# ==========================

def _weighted_stats(df: pd.DataFrame, col_value: str, col_weight: str):
    """
    Calcula média ponderada e intervalo de confiança (95%) aproximado.
    Retorna (media, margem_erro).
    """
    if col_value not in df.columns or col_weight not in df.columns:
        return float("nan"), float("nan")
    
    df_valid = df.dropna(subset=[col_value]).copy()
    if df_valid.empty:
        return float("nan"), float("nan")

    values = df_valid[col_value].values
    weights = df_valid[col_weight].values
    
    # Se soma dos pesos for 0, evita divisão por zero
    sum_weights = weights.sum()
    if sum_weights == 0:
        return float("nan"), float("nan")

    # Média Ponderada
    weighted_mean = np.average(values, weights=weights)

    # Variância Ponderada (aproximação para o desvio padrão da distribuição)
    variance = np.average((values - weighted_mean)**2, weights=weights)
    std_dev = np.sqrt(variance)

    # Erro Padrão da Média (SEM) e Margem de Erro (95% -> 1.96 * SEM)
    # n é o número de amostras (linhas/centróides)
    n = len(values)
    if n <= 1:
        margin_error = 0.0
    else:
        sem = std_dev / np.sqrt(n)
        margin_error = 1.96 * sem

    return weighted_mean, margin_error

def _simple_stats(df: pd.DataFrame, col_value: str):
    """
    Calcula média simples e IC 95% para colunas sem peso (ex: n_visitas).
    """
    if col_value not in df.columns:
        return float("nan"), float("nan")
    
    df_valid = df.dropna(subset=[col_value])
    if df_valid.empty:
        return float("nan"), float("nan")
    
    mean_val = df_valid[col_value].mean()
    sem_val = df_valid[col_value].sem() # Standard Error of Mean
    
    # Se sem_val for nan (ex: n=1), erro é 0
    if pd.isna(sem_val):
        margin_error = 0.0
    else:
        margin_error = 1.96 * sem_val
        
    return mean_val, margin_error


def construir_resumo(config_list):
    rows = []
    for cfg in config_list:
        path = Path(cfg["path"])
        if not path.is_file():
            print(f"⚠ Arquivo não encontrado, ignorando: {path}")
            continue

        df = pd.read_csv(path)

        if "n_visitas" not in df.columns:
            print(f"⚠ CSV sem coluna 'n_visitas': {path}")
            continue

        # 1. Mean Return Time (Ponderado) + CI
        mrh, mrh_ci = _weighted_stats(df, "mean_ret_h", "n_visitas")

        # 2. Mean Residence Time (Ponderado) + CI
        mres, mres_ci = _weighted_stats(df, "mean_res_h", "n_visitas")

        # 3. Mean Number of Visits (Simples - pois é a contagem média por nó) + CI
        mean_n_vis, mean_n_vis_ci = _simple_stats(df, "n_visitas")
        
        total_n_vis = int(df["n_visitas"].sum())

        rows.append({
            "method": cfg["method"],
            "k": cfg["k"],
            # Médias
            "mean_ret_h": mrh,
            "mean_res_h": mres,
            "mean_n_visitas": mean_n_vis,
            # Intervalos de Confiança (Margem de erro)
            "mean_ret_h_ci": mrh_ci,
            "mean_res_h_ci": mres_ci,
            "mean_n_visitas_ci": mean_n_vis_ci,
            
            "total_n_visitas": total_n_vis,
        })

    if not rows:
        raise RuntimeError("Nenhum arquivo válido encontrado na CONFIG.")

    resumo = pd.DataFrame(rows)
    resumo = resumo.sort_values(["method", "k"])
    return resumo


# ==========================
# TABELAS
# ==========================

def _salvar_tabela(resumo: pd.DataFrame, value_col: str, fname: str, out_dir: Path):
    tabela = resumo.pivot(index="method", columns="k", values=value_col)
    tabela = tabela[sorted(tabela.columns)]
    tabela_rounded = tabela.round(1)

    out_csv = out_dir / fname
    tabela_rounded.to_csv(out_csv)
    print(f"✔ Tabela salva em: {out_csv}")
    return tabela_rounded


# ==========================
# GRÁFICOS
# ==========================

def plot_barras(resumo: pd.DataFrame, value_col: str, error_col: str, ylabel: str,
                title: str, out_png: Path):
    """
    Gráfico de barras com Intervalo de Confiança (yerr).
    A legenda é posicionada fora do gráfico.
    """
    methods = sorted(resumo["method"].unique())
    ks = sorted(resumo["k"].unique())

    # coleta valores e erros para cada k por método
    data = {m: [] for m in methods}
    errors = {m: [] for m in methods}
    
    for k in ks:
        df_k = resumo[resumo["k"] == k]
        for m in methods:
            row = df_k[df_k["method"] == m]
            if not row.empty:
                val = row[value_col].iloc[0]
                err = row[error_col].iloc[0]
            else:
                val, err = float("nan"), float("nan")
            
            data[m].append(val)
            errors[m].append(err if not np.isnan(err) else 0)

    x = np.arange(len(ks))
    width = 0.8 / len(methods)

    # Aumentei a largura da figura para acomodar a legenda externa
    fig, ax = plt.subplots(figsize=(10, 6)) 

    # plota barras + rótulos
    for i, m in enumerate(methods):
        barras = ax.bar(
            x + i * width,
            data[m],
            width=width,
            yerr=errors[m],
            capsize=4,
            label=m,
            zorder=3,
            alpha=0.9
        )

        # Anotação dos valores
        for j, bar in enumerate(barras):
            h = bar.get_height()
            err = errors[m][j]
            
            if np.isnan(h):
                continue
        
            # Posição do texto: Altura da barra + Erro.
            # O xytext=(0, 5) adiciona um deslocamento de 5 pontos para cima,
            # garantindo que o texto fique acima da barra de erro.
            # y_pos = h + err 
            y_pos = h
            
            ax.annotate(
                f"{h:.1f}",
                xy=(bar.get_x() + bar.get_width() / 2, y_pos),
                xytext=(0, 5), # 5 points offset
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=9,
                clip_on=False,
                zorder=5,
                fontweight='bold',
                bbox=dict(facecolor='white', edgecolor='none', pad=3.0)
            )

    ax.set_xticks(x + width * (len(methods) - 1) / 2)
    ax.set_xticklabels(ks)
    ax.set_xlabel("Number of centroids", fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(title, fontsize=13, pad=15)
    ax.grid(True, axis="y", linestyle="--", alpha=0.5, zorder=0)

    # === MUDANÇA AQUI: Legenda fora do gráfico ===
    # bbox_to_anchor=(1, 0.5) coloca a legenda à direita, centralizada verticalmente.
    ax.legend(title="Method", loc="center left", bbox_to_anchor=(1, 0.5))

    # Ajuste dinâmico do limite Y para caber as barras de erro e rótulos
    all_vals = []
    for m in methods:
        vals = np.array(data[m])
        errs = np.array(errors[m])
        vals = np.nan_to_num(vals)
        errs = np.nan_to_num(errs)
        all_vals.extend(vals + errs)
        
    if all_vals:
        y_max = max(all_vals)
        # 15% de margem acima da barra de erro mais alta para o rótulo
        ax.set_ylim(0, y_max * 1.15) 

    # O tight_layout ajusta automaticamente para incluir a legenda externa
    fig.tight_layout()
    fig.savefig(out_png, dpi=300, bbox_inches="tight")
    print(f"✔ Gráfico salvo em: {out_png}")

# ==========================
# MAIN
# ==========================

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    resumo = construir_resumo(CONFIG)

    # 1) Tabelas
    print("\n=== Gerando Tabelas ===")
    _salvar_tabela(resumo, "mean_ret_h", "table_mean_return_time_hours.csv", OUT_DIR)
    _salvar_tabela(resumo, "mean_res_h", "table_mean_residence_time_hours.csv", OUT_DIR)
    _salvar_tabela(resumo, "mean_n_visitas", "table_mean_number_of_visits.csv", OUT_DIR)

    # 2) Gráficos de barras com legenda externa
    print("\n=== Gerando Gráficos de Barras com IC e Legenda Externa ===")
    
    plot_barras(
        resumo,
        value_col="mean_ret_h",
        error_col="mean_ret_h_ci",
        ylabel="Mean return time (h)",
        title=f"Mean return time vs. clustering method – {DATASET_NAME}",
        out_png=OUT_DIR / "mean_return_time_barplot.png",
    )

    plot_barras(
        resumo,
        value_col="mean_res_h",
        error_col="mean_res_h_ci",
        ylabel="Mean residence time per visit (h)",
        title=f"Mean residence time vs. clustering method – {DATASET_NAME}",
        out_png=OUT_DIR / "mean_residence_time_barplot.png",
    )

    plot_barras(
        resumo,
        value_col="mean_n_visitas",
        error_col="mean_n_visitas_ci",
        ylabel="Mean number of visits",
        title=f"Mean number of visits vs. clustering method – {DATASET_NAME}",
        out_png=OUT_DIR / "mean_number_of_visits_barplot.png",
    )

    # # 3) Gráficos de linhas com legenda externa
    # print("\n=== Gerando Gráficos de Linhas com Legenda Externa ===")
    # _plot_linhas_com_rotulos(resumo, "mean_ret_h", "Mean return time (h)", f"Mean return time – {DATASET_NAME}", OUT_DIR / "mean_return_time_lineplot.png")
    # _plot_linhas_com_rotulos(resumo, "mean_res_h", "Mean res time (h)", f"Mean residence time – {DATASET_NAME}", OUT_DIR / "mean_residence_time_lineplot.png")
    # _plot_linhas_com_rotulos(resumo, "mean_n_visitas", "Mean visits", f"Mean visits – {DATASET_NAME}", OUT_DIR / "mean_number_of_visits_lineplot.png")


if __name__ == "__main__":
    main()