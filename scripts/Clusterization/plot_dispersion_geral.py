import pandas as pd
import matplotlib.pyplot as plt
import os
import sys
import json

# Add scripts directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Common.utils import create_clusterization_results, read_field_from_json


def _read_numeric_points(file_path):
    if not os.path.exists(file_path):
        return None

    try:
        df = pd.read_csv(file_path, header=None, dtype=str, on_bad_lines='skip')
    except Exception as exc:
        print(f"Não foi possível ler {file_path}: {exc}")
        return None

    if df.empty or df.shape[1] < 4:
        return None

    df = df.iloc[:, :4].copy()
    df.columns = ['ID', 'Timestamp', 'Longitude', 'Latitude']

    for col in ['Longitude', 'Latitude']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df.dropna(subset=['Longitude', 'Latitude'])


def _load_interpolated_points(base_name, results_dir, method='nbeats'):
    """
    Carrega os pontos interpolados para um método específico ('bilstm' ou 'nbeats').
    Procura em Results/<dataset>/Interpolation/ e em Results/<dataset>/.
    """
    method = method.lower()
    interpolation_dir = os.path.join(results_dir, 'Interpolation')
    
    candidate_files = [
        os.path.join(interpolation_dir, f'map_{base_name}_interpolation_{method}_all.csv'),
        os.path.join(results_dir, f'map_interpolation_merged_{base_name}_{method}.csv'),
        os.path.join(interpolation_dir, f'map_{base_name}_interpolation_{method}.csv'),
    ]

    for candidate in candidate_files:
        df = _read_numeric_points(candidate)
        if df is not None and not df.empty:
            return df

    if os.path.exists(interpolation_dir):
        merged_files = sorted(
            [
                os.path.join(interpolation_dir, file_name)
                for file_name in os.listdir(interpolation_dir)
                if file_name.endswith(f'_interpolation_{method}_merged.csv') or file_name.endswith(f'_interpolation_{method}.csv')
            ]
        )
        if merged_files:
            frames = []
            for file_path in merged_files:
                df = _read_numeric_points(file_path)
                if df is not None and not df.empty:
                    frames.append(df)

            if frames:
                return pd.concat(frames, ignore_index=True)

    return pd.DataFrame(columns=['ID', 'Timestamp', 'Longitude', 'Latitude'])


def _plot_scatter(data, output_path, title, label, color=None, marker='o'):
    plt.figure(figsize=(10, 8))
    scatter_kwargs = {
        'alpha': 0.5,
        's': 15,
        'label': label,
        'marker': marker
    }
    if color:
        scatter_kwargs['color'] = color
    else:
        scatter_kwargs['color'] = '#1f77b4'

    plt.scatter(
        data['Longitude'],
        data['Latitude'],
        **scatter_kwargs
    )
    plt.title(title, fontsize=12, fontweight='bold')
    plt.xlabel('Longitude', fontsize=10)
    plt.ylabel('Latitude', fontsize=10)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.gca().set_aspect('equal', adjustable='box')
    plt.legend(loc='upper right', frameon=True)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()


def extract_folder_name(file_rawdata):
    if not file_rawdata:
        return 'jaguar_mamiraua'
    file_name = file_rawdata.split('/')
    file_name = file_name[-1].split('.')[0]
    return file_name


def plot_jaguar(file_rawdata=None):
    """
    Carrega a base de dados original, limpa os dados e gera
    um gráfico de dispersão para os dados da jaguatirica (dados brutos).
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    rawdata_dir = os.path.join(script_dir, '..', '..', 'rawdata')
    
    base_name = extract_folder_name(file_rawdata)
    input_csv_path = os.path.join(rawdata_dir, f'{base_name}.csv')
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

    if not os.path.exists(input_csv_path) or not os.path.exists(input_json_path):
        print("ERRO: Um dos arquivos de entrada não foi encontrado.")
        return

    with open(input_json_path, 'r') as f:
        columns_map = json.load(f)

    lon_col = columns_map['LONGITUDE']
    lat_col = columns_map['LATITUDE']

    data = pd.read_csv(input_csv_path, header=0)

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

    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    labels = "Localizações" if language == "PT_BR" else "Locations"

    df_scatter = pd.DataFrame({
        'Longitude': data_cleaned[lon_col],
        'Latitude': data_cleaned[lat_col]
    })

    title = lang.get("grafico_dispersao_geral_jaguar_titulo", "Dispersão Geral de Coordenadas GPS (Dados Brutos)")
    output_file_png = os.path.join(cluster_output_dir, f'dispersao_geral_{base_name}.png')
    _plot_scatter(df_scatter, output_file_png, title, labels, color='#1f77b4', marker='o')
    print(f"Gráfico Jaguar salvo em: {output_file_png}")


def plot_interpolated_bilstm(file_rawdata=None):
    """Gera o gráfico de dispersão geral para os pontos interpolados via BiLSTM."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_name = extract_folder_name(file_rawdata)
    output_main_dir = os.path.join(script_dir, '..', 'Results')
    results_dir = os.path.join(output_main_dir, base_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    data = _load_interpolated_points(base_name, results_dir, method='bilstm')
    if data.empty:
        print(f"Nenhum dado interpolado por BiLSTM encontrado para {base_name}.")
        return

    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    language = read_field_from_json(hyperparam_path, 'language')
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    if language == 'PT_BR':
        label = 'Pontos Interpolados (BiLSTM)'
        title = 'Dispersão Geral - Interpolação BiLSTM'
    else:
        label = 'Interpolated Points (BiLSTM)'
        title = 'General Dispersion - BiLSTM Interpolation'

    output_file_png = os.path.join(cluster_output_dir, f'dispersao_geral_bilstm_{base_name}.png')
    _plot_scatter(data, output_file_png, title, label, color='#2ca02c', marker='o')
    print(f"Gráfico BiLSTM salvo em: {output_file_png}")


def plot_interpolated_nbeats(file_rawdata=None):
    """Gera o gráfico de dispersão geral para os pontos interpolados via N-BEATS."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_name = extract_folder_name(file_rawdata)
    output_main_dir = os.path.join(script_dir, '..', 'Results')
    results_dir = os.path.join(output_main_dir, base_name)
    cluster_output_dir = os.path.join(results_dir, 'Clusterization')
    create_clusterization_results(cluster_output_dir)

    data = _load_interpolated_points(base_name, results_dir, method='nbeats')
    if data.empty:
        print(f"Nenhum dado interpolado por N-BEATS encontrado para {base_name}.")
        return

    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    language = read_field_from_json(hyperparam_path, 'language')
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    if language == 'PT_BR':
        label = 'Pontos Interpolados (N-BEATS)'
        title = 'Dispersão Geral - Interpolação N-BEATS'
    else:
        label = 'Interpolated Points (N-BEATS)'
        title = 'General Dispersion - N-BEATS Interpolation'

    output_file_png = os.path.join(cluster_output_dir, f'dispersao_geral_nbeats_{base_name}.png')
    _plot_scatter(data, output_file_png, title, label, color='#ff7f0e', marker='s')
    print(f"Gráfico N-BEATS salvo em: {output_file_png}")


def plot_jaguar_interpolated(file_rawdata=None):
    """Gera as dispersões para cada modelo de interpolação (BiLSTM e N-BEATS)."""
    plot_interpolated_bilstm(file_rawdata)
    plot_interpolated_nbeats(file_rawdata)


def plot_tangara(file_rawdata=None):
    """
    Carrega a base de dados original, limpa os dados e gera
    um gráfico de dispersão para os dados da tangará.
    """
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

    if not os.path.exists(input_csv_path) or not os.path.exists(input_json_path):
        print("ERRO: Um dos arquivos de entrada não foi encontrado.")
        return

    with open(input_json_path, 'r') as f:
        columns_map = json.load(f)

    lon_col = columns_map['LONGITUDE']
    lat_col = columns_map['LATITUDE']

    data = pd.read_csv(input_csv_path, header=0)

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

    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
    language = read_field_from_json(hyperparam_path, "language")
    json_path = os.path.join(data_prep_dir, f'language_{language}.json')
    with open(json_path, encoding='utf-8') as f:
        lang = json.load(f)

    labels = "Localizações" if language == "PT_BR" else "Locations"

    df_scatter = pd.DataFrame({
        'Longitude': data_cleaned[lon_col],
        'Latitude': data_cleaned[lat_col]
    })

    title = lang.get("grafico_dispersao_geral_tangara_titulo", "Dispersão Geral de Coordenadas GPS (Dados Brutos)")
    output_file_png = os.path.join(cluster_output_dir, f'dispersao_geral_{base_name}.png')
    _plot_scatter(df_scatter, output_file_png, title, labels, color='#1f77b4', marker='o')
    print(f"Gráfico Tangara salvo em: {output_file_png}")


def plot_tangara_interpolated(file_rawdata=None):
    """Gera as dispersões para cada modelo de interpolação para tangara."""
    plot_interpolated_bilstm(file_rawdata)
    plot_interpolated_nbeats(file_rawdata)


def run_all_dispersion(file_rawdata=None):
    if file_rawdata:
        base_name = extract_folder_name(file_rawdata)
        if base_name.startswith('jaguar'):
            plot_jaguar(file_rawdata)
            plot_interpolated_bilstm(file_rawdata)
            plot_interpolated_nbeats(file_rawdata)
        elif base_name.startswith('tangara'):
            plot_tangara(file_rawdata)
            plot_interpolated_bilstm(file_rawdata)
            plot_interpolated_nbeats(file_rawdata)
        else:
            plot_jaguar(file_rawdata)
            plot_interpolated_bilstm(file_rawdata)
            plot_interpolated_nbeats(file_rawdata)
    else:
        plot_jaguar('jaguar_mamiraua')
        plot_interpolated_bilstm('jaguar_mamiraua')
        plot_interpolated_nbeats('jaguar_mamiraua')


if __name__ == "__main__":
    run_all_dispersion()