# ==========================================================
# ONE SIMULATOR RESULTS ANALYZER
# ==========================================================
#
# This script:
# 1. Reads all MessageStatsReport files
# 2. Extracts metadata from filenames
# 3. Extracts performance metrics
# 4. Saves:
#    - metrics_by_seed.csv
#    - metrics_mean.csv
# 5. Generates plots:
#    - compare_routing
#    - compare_scenarios
#    - compare_message_sizes
#    - compare_routing_by_metric
#
# ==========================================================

import re
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# ==========================================================
# USER SETTINGS
# ==========================================================
LANGUAGE = "en"          # "en" or "pt"
USE_HATCHES = True
USE_COLORS = True
DPI = 300


# ==========================================================
# TRANSLATIONS
# ==========================================================
TRANSLATIONS = {
    "en": {
        "delivery_prob": "Delivery Probability",
        "delivery_ratio": "Delivery Ratio",
        "latency_avg": "Average Latency (s)",
        "overhead_ratio": "Overhead Ratio",
        "hopcount_avg": "Average Hop Count",

        "message_size": "Message Size (kB)",
        "routing": "Routing Protocol",
        "scenario": "Scenario",

        "compare_routing": "Routing Protocol Comparison",
        "compare_scenarios": "Scenario Comparison",
        "compare_message_sizes": "Message Size Comparison",
        "compare_routing_by_metric": "Routing Comparison by Metric",

        "processing_done": "Processing completed successfully!",
        "results_saved": "Results saved in",
    },

    "pt": {
        "delivery_prob": "Probabilidade de Entrega",
        "delivery_ratio": "Taxa de Entrega",
        "latency_avg": "Latência Média (s)",
        "overhead_ratio": "Razão de Sobrecarga",
        "hopcount_avg": "Número Médio de Saltos",

        "message_size": "Tamanho da Mensagem (kB)",
        "routing": "Protocolo de Roteamento",
        "scenario": "Cenário",

        "compare_routing": "Comparação entre Protocolos",
        "compare_scenarios": "Comparação entre Cenários",
        "compare_message_sizes": "Comparação entre Tamanhos de Mensagem",
        "compare_routing_by_metric": "Comparação entre Protocolos por Métrica",

        "processing_done": "Processamento concluído com sucesso!",
        "results_saved": "Resultados salvos em",
    }
}


def tr(key):
    return TRANSLATIONS[LANGUAGE].get(key, key)


# ==========================================================
# PATHS
# ==========================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

possible_dirs = [
    PROJECT_ROOT / "the_one" / "reports" / "jaguar_results",
    PROJECT_ROOT / "the_one" / "results" / "jaguar_results",
    PROJECT_ROOT / "reports" / "jaguar_results",
    PROJECT_ROOT / "results" / "jaguar_results",
]

INPUT_DIR = None
for d in possible_dirs:
    if d.exists():
        INPUT_DIR = d
        break

if INPUT_DIR is None:
    raise FileNotFoundError("Directory 'jaguar_results' not found.")

OUTPUT_DIR = PROJECT_ROOT / "analysis_results"
OUTPUT_DIR.mkdir(exist_ok=True)

PLOTS_DIR = OUTPUT_DIR / "plots"
PLOTS_DIR.mkdir(exist_ok=True)

CSV_BY_SEED = OUTPUT_DIR / "metrics_by_seed.csv"
CSV_MEAN = OUTPUT_DIR / "metrics_mean.csv"


# ==========================================================
# METRICS
# ==========================================================
METRICS = [
    "delivery_prob",
    "delivery_ratio",
    "latency_avg",
    "overhead_ratio",
    "hopcount_avg",
]


# ==========================================================
# HELPERS
# ==========================================================
def parse_filename(filename):
    pattern = (
        r"^(?P<prefix>.+?)_"
        r"(?P<routing>[A-Za-z0-9]+)_"
        r"(?P<seed>\d+)_"
        r"(?P<message_size>\d+)_"
        r"(?P<scenario>.+?)_"
        r"MessageStatsReport\.txt$"
    )

    match = re.match(pattern, filename)
    if not match:
        return None

    data = match.groupdict()
    data["seed"] = int(data["seed"])
    data["message_size"] = int(data["message_size"])
    return data


def parse_stats_file(filepath):
    stats = {}

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if ":" not in line:
                continue

            key, value = line.strip().split(":", 1)
            key = key.strip()
            value = value.strip()

            if value.lower() == "nan":
                stats[key] = None
                continue

            try:
                stats[key] = float(value)
            except ValueError:
                stats[key] = value

    return stats


def sanitize_filename(text):
    return re.sub(r"[^A-Za-z0-9_\-]", "_", str(text))


def save_figure(fig, filepath):
    fig.savefig(filepath, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {filepath}")


def add_value_labels(ax, fmt="{:.3f}"):
    """
    Adiciona os valores acima das barras na horizontal
    e com fonte maior.
    """
    for container in ax.containers:
        labels = []

        for bar in container:
            height = bar.get_height()

            if pd.isna(height):
                labels.append("")
            else:
                labels.append(fmt.format(height))

        ax.bar_label(
            container,
            labels=labels,
            padding=4,          # distância da barra
            fontsize=10,        # fonte maior
            rotation=0          # horizontal
        )

# ==========================================================
# LOAD DATA
# ==========================================================
files = sorted(INPUT_DIR.glob("*MessageStatsReport.txt"))

if not files:
    raise RuntimeError("No MessageStatsReport files found.")

print(f"Files found: {len(files)}")

rows = []

for filepath in files:
    metadata = parse_filename(filepath.name)

    if metadata is None:
        print(f"Skipping invalid filename: {filepath.name}")
        continue

    stats = parse_stats_file(filepath)

    created = stats.get("created", 0)
    delivered = stats.get("delivered", 0)

    rows.append({
        "prefix": metadata["prefix"],
        "routing": metadata["routing"],
        "seed": metadata["seed"],
        "message_size": metadata["message_size"],
        "scenario": metadata["scenario"],

        "delivery_prob": stats.get("delivery_prob"),
        "delivery_ratio": delivered / created if created else None,
        "latency_avg": stats.get("latency_avg"),
        "overhead_ratio": stats.get("overhead_ratio"),
        "hopcount_avg": stats.get("hopcount_avg"),
    })

if not rows:
    raise RuntimeError("No valid data extracted.")

df = pd.DataFrame(rows)

# Save per seed
df = df.sort_values(
    ["routing", "scenario", "message_size", "seed"]
).reset_index(drop=True)

df.to_csv(CSV_BY_SEED, index=False)
print(f"CSV saved: {CSV_BY_SEED}")

# Average over seeds
group_cols = ["prefix", "routing", "message_size", "scenario"]

df_mean = (
    df.groupby(group_cols, as_index=False)[METRICS]
    .mean()
)

seed_count = (
    df.groupby(group_cols, as_index=False)["seed"]
    .nunique()
    .rename(columns={"seed": "num_seeds"})
)

df_mean = df_mean.merge(seed_count, on=group_cols)
df_mean.to_csv(CSV_MEAN, index=False)
print(f"CSV saved: {CSV_MEAN}")


# ==========================================================
# GENERIC COMPARISON PLOT
# ==========================================================
def plot_comparison(
    data,
    compare_col,
    x_col,
    fixed_cols,
    output_subdir,
    title_key
):
    output_dir = PLOTS_DIR / output_subdir
    output_dir.mkdir(exist_ok=True)

    hatch_patterns = [
        "/", "\\", "|", "-", "+",
        "x", "o", "O", ".", "*"
    ]

    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    grouped = data.groupby(fixed_cols)

    for fixed_values, group_df in grouped:
        if not isinstance(fixed_values, tuple):
            fixed_values = (fixed_values,)

        compare_values = sorted(group_df[compare_col].dropna().unique())
        x_values = sorted(group_df[x_col].dropna().unique())

        if not compare_values or not x_values:
            continue

        fig, axes = plt.subplots(3, 2, figsize=(18, 12))
        axes = axes.flatten()

        n_groups = len(x_values)
        n_bars = len(compare_values)
        bar_width = 0.8 / n_bars
        x_positions = list(range(n_groups))

        for i, metric in enumerate(METRICS):
            ax = axes[i]

            for j, compare_value in enumerate(compare_values):
                subset = group_df[
                    group_df[compare_col] == compare_value
                ].sort_values(x_col)

                y_values = []
                for x_val in x_values:
                    row = subset[subset[x_col] == x_val]
                    if row.empty:
                        y_values.append(float("nan"))
                    else:
                        y_values.append(row[metric].iloc[0])

                offsets = [
                    x + (j - (n_bars - 1) / 2) * bar_width
                    for x in x_positions
                ]

                bars = ax.bar(
                    offsets,
                    y_values,
                    width=bar_width,
                    label=str(compare_value),
                    color=colors[j % len(colors)] if USE_COLORS else "white",
                    edgecolor="black",
                    linewidth=1
                )

                if USE_HATCHES:
                    hatch = hatch_patterns[j % len(hatch_patterns)]
                    for bar in bars:
                        bar.set_hatch(hatch)

            ax.set_title(tr(metric))
            ax.set_xlabel(tr(x_col))
            ax.set_ylabel(tr(metric))
            ax.set_xticks(x_positions)
            ax.set_xticklabels([str(v) for v in x_values])
            ax.grid(True, axis="y", linestyle="--", alpha=0.5)
            ax.legend(fontsize=8)

        fig.delaxes(axes[-1])

        title_parts = [
            f"{tr(col)} = {val}"
            for col, val in zip(fixed_cols, fixed_values)
        ]

        fig.suptitle(
            tr(title_key) + "\n" + " | ".join(title_parts),
            fontsize=16
        )

        plt.tight_layout(rect=[0, 0, 1, 0.96])

        filename = "_".join(
            sanitize_filename(v)
            for v in fixed_values
        ) + ".png"

        save_figure(fig, output_dir / filename)


# ==========================================================
# ROUTING COMPARISON BY INDIVIDUAL METRIC
# ==========================================================
def plot_routing_by_metric(data):
    output_dir = PLOTS_DIR / "compare_routing_by_metric"
    output_dir.mkdir(exist_ok=True)

    hatch_patterns = [
        "/", "\\", "|", "-", "+",
        "x", "o", "O", ".", "*"
    ]

    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for scenario, scenario_df in data.groupby("scenario"):

        routing_values = sorted(
            scenario_df["routing"].dropna().unique()
        )

        x_values = sorted(
            scenario_df["message_size"].dropna().unique()
        )

        n_groups = len(x_values)
        n_bars = len(routing_values)
        bar_width = 0.8 / n_bars
        x_positions = list(range(n_groups))

        for metric in METRICS:
            fig, ax = plt.subplots(figsize=(12, 7))

            for j, routing in enumerate(routing_values):
                subset = scenario_df[
                    scenario_df["routing"] == routing
                ].sort_values("message_size")

                y_values = []
                for x_val in x_values:
                    row = subset[
                        subset["message_size"] == x_val
                    ]

                    if row.empty:
                        y_values.append(float("nan"))
                    else:
                        y_values.append(row[metric].iloc[0])

                offsets = [
                    x + (j - (n_bars - 1) / 2) * bar_width
                    for x in x_positions
                ]

                bars = ax.bar(
                    offsets,
                    y_values,
                    width=bar_width,
                    label=routing,
                    color=colors[j % len(colors)] if USE_COLORS else "white",
                    edgecolor="black",
                    linewidth=1
                )

                if USE_HATCHES:
                    hatch = hatch_patterns[j % len(hatch_patterns)]
                    for bar in bars:
                        bar.set_hatch(hatch)

            # Add labels
            if metric in ["delivery_prob", "delivery_ratio"]:
                add_value_labels(ax, "{:.3f}")
            else:
                add_value_labels(ax, "{:.2f}")

            ax.set_title(
                f"{tr(metric)} - {tr('scenario')} = {scenario}"
            )

            ax.set_xlabel(tr("message_size"))
            ax.set_ylabel(tr(metric))
            ax.set_xticks(x_positions)
            ax.set_xticklabels([str(v) for v in x_values])
            ax.grid(True, axis="y", linestyle="--", alpha=0.5)
            ax.legend()

            plt.tight_layout()

            filename = (
                f"{sanitize_filename(scenario)}_"
                f"{sanitize_filename(metric)}.png"
            )

            save_figure(fig, output_dir / filename)


# ==========================================================
# GENERATE PLOTS
# ==========================================================
plot_comparison(
    data=df_mean,
    compare_col="routing",
    x_col="message_size",
    fixed_cols=["scenario"],
    output_subdir="compare_routing",
    title_key="compare_routing"
)

plot_comparison(
    data=df_mean,
    compare_col="scenario",
    x_col="message_size",
    fixed_cols=["routing"],
    output_subdir="compare_scenarios",
    title_key="compare_scenarios"
)

plot_comparison(
    data=df_mean,
    compare_col="message_size",
    x_col="message_size",
    fixed_cols=["routing", "scenario"],
    output_subdir="compare_message_sizes",
    title_key="compare_message_sizes"
)

plot_routing_by_metric(df_mean)


# ==========================================================
# FINISH
# ==========================================================
print()
print(tr("processing_done"))
print(f"{tr('results_saved')}: {OUTPUT_DIR}")