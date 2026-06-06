import json
import csv
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from Data_preparation.data_field import DataField
from Data_preparation.raw_data_integration import get_id_from_json

interpolations_methods = ['N_BEATS', 'N_HITS', 'BiLSTM']

#Modifiquei a ordem das funções somente. Coloquei as funções que nao dependem da variavel global sobre elas.

def get_contact_distance():
    script_dir = os.path.dirname(os.path.abspath(__file__))  
    data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
    hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')

    CONTACT_DISTANCE = read_field_from_json(hyperparam_path, "CONTACT_DISTANCE")

    return CONTACT_DISTANCE

def read_field_from_json(json_file, field_name):
    """
    Reads a specific field from a JSON file.

    Args:
    - json_file (str): Path to the JSON file.
    - field_name (str): The name of the field whose value you want to retrieve.

    Returns:
    - The value of the field from the JSON file.
    - If the field doesn't exist, returns None.
    """
    try:
        # Open and load the JSON file
        with open(json_file, 'r') as file:
            data = json.load(file)
        
        # Check if the field exists in the loaded data
        if field_name in data:
            return data[field_name]
        else:
            print(f"Field '{field_name}' not found in the JSON file.")
            return None
    except FileNotFoundError:
        print(f"File '{json_file}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding the JSON file '{json_file}'.")
        return None

TRAINNING_SET = 0.8
VALIDATION_SET = 0.1
TESTING_SET = 0.1
CONTACT_DISTANCE = get_contact_distance()

'''
def results_folder( file_rawdata_name ):

    file_name = file_rawdata_name.split('/')
    file_name = file_name[-1].split('.')[0]

    script_dir = os.path.dirname(os.path.abspath(__file__))

    script_dir = script_dir.replace('Common', '')

    results_dir = os.path.join(script_dir, f'Results/{file_name}')
    
    os.makedirs(results_dir, exist_ok=True)

    return results_dir
'''

def results_folder(file_rawdata_name):

    file_name = os.path.splitext(
        os.path.basename(file_rawdata_name)
    )[0]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_dir = script_dir.replace('Common', '')

    results_dir = os.path.join(
        script_dir,
        'Results',
        file_name
    )

    os.makedirs(results_dir, exist_ok=True)

    return results_dir

# def merge_csvs( current_animal, method, file_rawdata_name, file_rawdata_columns ):

#     # Define the results directory and file path
#     results_dir = results_folder( file_rawdata_name )

#     #file_path = os.path.join(results_dir, f'map_{current_animal}.csv')  # Path to the CSV file
#     file_path = os.path.join(results_dir, f'map_{current_animal}_outliers_less_test_only.csv')  # Path to the CSV file

#     # Check if the file exists
#     if not os.path.exists(file_path):
#         print(f"File {file_path} not found.")
#         return None

#     # Read the CSV file into a DataFrame
#     df_raw = pd.read_csv(file_path, header=None)

#     df_raw.columns = ['ID', 'DateTime', 'Longitude', 'Latitude']

#     if method == 'N_BEATS':
#         file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats.csv')  # Path to the CSV file

#     if method == 'N_HITS':
#         file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nhits.csv')  # Path to the CSV file


#     # Check if the file exists
#     if not os.path.exists(file_path):
#         print(f"File {file_path} not found.")
#         return None

#     # Read the CSV file into a DataFrame
#     df_interpolation = pd.read_csv(file_path, header=None)
#     df_interpolation.columns = ['ID', 'DateTime', 'Longitude', 'Latitude']

#     result = pd.concat([df_raw, df_interpolation], axis=0)

#     # Convert the 'DateTime' column to datetime type
#     #result['DateTime'] = pd.to_datetime(result['DateTime'], format='%d/%m/%y %H:%M')
#     mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

#     result['DateTime'] = pd.to_datetime(result['DateTime'], format=mask)

#     # Sort the DataFrame by the 'DateTime' column
#     df_sorted = result.sort_values(by='DateTime')

#     columns_to_save = ['ID', 'DateTime', 'Longitude', 'Latitude']

#     results_dir = results_folder( file_rawdata_name )

#     if method == 'N_BEATS':    
#         file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats_merged.csv')

#     if method == 'N_HITS':    
#         file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nhits_merged.csv')


#     hiper_content = []
#     hiper_content.append( f"Total merged {len(df_sorted)} método {method} animal {current_animal}" )

#     hiper_path = os.path.join(results_dir, f'hiperparameters.txt')

#     with open(hiper_path, "a") as file:
#         for line in hiper_content:
#             file.write(line + '\n')    

#     df_sorted[columns_to_save].to_csv( file_path, index=False, header=False)


def _read_and_clean(path):
    # Check if file exists and is not empty
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        print(f"File {path} not found or is empty. Skipping.")
        return pd.DataFrame(columns=['ID','DateTime','Longitude','Latitude'])

    # Read flexibly; some files may contain more than 4 columns
    try:
        df = pd.read_csv(path, header=None, dtype=str, on_bad_lines='skip')
    except pd.errors.EmptyDataError:
        print(f"File {path} is empty (EmptyDataError). Skipping.")
        return pd.DataFrame(columns=['ID','DateTime','Longitude','Latitude'])
    # Keep only the first 4 columns (ID, DateTime, Longitude, Latitude)
    if df.shape[1] < 4:
        print(f"{path}: fewer than 4 columns found. Ignored.")
        return pd.DataFrame(columns=['ID','DateTime','Longitude','Latitude'])
    df = df.iloc[:, :4].copy()
    df.columns = ['ID', 'DateTime', 'Longitude', 'Latitude']

    # Normalize blanks/whitespace to NaN
    for c in ['ID', 'DateTime', 'Longitude', 'Latitude']:
        df[c] = df[c].astype(str).str.strip()
        df[c] = df[c].replace(r'^\s*$', np.nan, regex=True)

    # Drop rows with missing essential fields
    df = df.dropna(subset=['ID', 'DateTime', 'Longitude', 'Latitude'])

    # Coerce coordinates to numeric and drop invalid
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
    df['Latitude']  = pd.to_numeric(df['Latitude'],  errors='coerce')
    df = df.dropna(subset=['Longitude', 'Latitude'])

    return df

def merge_csvs(current_animal, method, file_rawdata_name, file_rawdata_columns):
    """
    Merge raw and interpolated CSV data for a given animal.
    Cleans rows with missing/blank/whitespace values in ID, DateTime, Longitude, or Latitude,
    and ensures Longitude/Latitude are numeric. Skips saving if fewer than 10 valid rows remain.
    Saves the merged result sorted by DateTime.

    Args:
        current_animal (str): Identifier of the animal.
        method (str): Interpolation method ("N_BEATS" or "N_HITS").
        file_rawdata_name (str): Path to the raw data file (used for results folder naming).
        file_rawdata_columns (str): Path to JSON with column mapping information (for datetime mask).

    Returns:
        None
    """

    results_dir = results_folder(file_rawdata_name)
    raw_path = os.path.join(results_dir, f'map_{current_animal}.csv')

    df_raw = _read_and_clean(raw_path)

    if method == 'N_BEATS':
        interp_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats.csv')
    elif method == 'N_HITS':
        interp_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nhits.csv')
    elif method == 'BiLSTM':
        interp_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_bilstm.csv')
    else:
        print(f"Unknown method '{method}'.")
        return None

    df_interp = _read_and_clean(interp_path)

    # Concatenate and validate minimum rows (after cleaning)
    result = pd.concat([df_raw, df_interp], axis=0, ignore_index=True)

    if len(result) < 10:
        print(f"Merged dataset has fewer than 10 valid rows. Skipped.")
        return None

    # Parse datetime using mask from JSON
    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)
    result['DateTime'] = pd.to_datetime(result['DateTime'], format=mask, errors='coerce')
    result = result.dropna(subset=['DateTime'])

    if len(result) < 10:
        print(f"After DateTime parsing, fewer than 10 valid rows remain. Skipped.")
        return None

    # Sort and save
    result = result.sort_values(by='DateTime')
    columns_to_save = ['ID', 'DateTime', 'Longitude', 'Latitude']

    interpolation_path = os.path.join(results_dir, f'Interpolation')
    os.makedirs(interpolation_path, exist_ok=True)

    if method == 'N_BEATS':
        out_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats_merged.csv')
    elif method == 'BiLSTM':
        out_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_bilstm_merged.csv')
    else:
        out_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nhits_merged.csv')

    # Log
    hiper_path = os.path.join(results_dir, 'hiperparameters.txt')
    with open(hiper_path, "a") as f:
        f.write(f"Total merged {len(result)} method {method} animal {current_animal}\n")

    # Save without NaN
    result[columns_to_save].to_csv(out_path, index=False, header=False)



def read_field_from_json(json_file, field_name):
    """
    Reads a specific field from a JSON file.

    Args:
    - json_file (str): Path to the JSON file.
    - field_name (str): The name of the field whose value you want to retrieve.

    Returns:
    - The value of the field from the JSON file.
    - If the field doesn't exist, returns None.
    """
    try:
        # Open and load the JSON file
        with open(json_file, 'r') as file:
            data = json.load(file)
        
        # Check if the field exists in the loaded data
        if field_name in data:
            return data[field_name]
        else:
            print(f"Field '{field_name}' not found in the JSON file.")
            return None
    except FileNotFoundError:
        print(f"File '{json_file}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding the JSON file '{json_file}'.")
        return None

def get_list_animals(file_name, file_rawdata_columns):
    """
    Lê um CSV com pandas e retorna a lista de IDs únicos
    """
    # Lê o CSV inteiro em um DataFrame
    df = pd.read_csv(file_name)

    # Obtém a coluna correta do JSON de configuração
    col_id = get_id_from_json(file_rawdata_columns, DataField.ID)

    # Remove espaços em branco e extrai IDs únicos
    ids = df[col_id].astype(str).str.replace(' ', '').unique().tolist()

    return ids

def remove_nan_data(df, current_animal=None):
    # 1. Check if the CSV has at least 10 rows initially
    if len(df) < 10:
        print(f"Warning: CSV for animal {current_animal} has fewer than 10 rows. Skipping.")
        return pd.DataFrame()
    
    # 2. Remove rows with missing interesting data
    essential_cols = ['ID', 'Timestamp', 'Longitude', 'Latitude']
    # Replace empty strings with NaN to be dropped
    df[essential_cols] = df[essential_cols].replace(r'^\s*$', np.nan, regex=True)
    df.dropna(subset=essential_cols, inplace=True)

    # 3. Check if there are still enough rows after cleaning
    if len(df) < 10:
        print(f"Warning: After cleaning, animal {current_animal} has fewer than 10 valid rows. Skipping.")
        return pd.DataFrame()
    
    return df

def create_clusterization_results(folder_name):
    script_dir = os.path.dirname(os.path.abspath(__file__)) 
    folder_path = os.path.join(script_dir, '..', folder_name)

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder '{folder_path}' created successfully")
        return True
    else:
        print(f"Folder '{folder_path}' already exists")
        return False


def plot_cluster_quality_metrics(output_dir, output_prefix='quality_metrics_comparison'):
    """
    Create a grouped bar chart comparing Silhouette Score, Davies-Bouldin Index,
    and Quantization Error across all metric CSV files in the output directory.
    """
    if not os.path.exists(output_dir):
        print(f"Output directory {output_dir} does not exist.")
        return

    metrics_files = [f for f in os.listdir(output_dir)
                     if f.startswith('Metricas_de_qualidade_') and f.endswith('.csv')]
    if not metrics_files:
        print(f"No quality metrics CSV files found in {output_dir} to plot.")
        return

    records = []
    for metrics_file in sorted(metrics_files):
        path = os.path.join(output_dir, metrics_file)
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            print(f"Unable to read metrics file {path}: {exc}")
            continue

        if df.empty:
            continue

        file_algorithm = os.path.splitext(metrics_file)[0].replace('Metricas_de_qualidade_', '')
        if 'Algorithm' in df.columns:
            for _, row in df.iterrows():
                algorithm = str(row['Algorithm']) if not pd.isna(row['Algorithm']) else file_algorithm
                record = {'Algorithm': algorithm}
                for metric_name in ['Silhouette Score', 'Davies-Bouldin Index', 'Quantization Error']:
                    record[metric_name] = float(row[metric_name]) if metric_name in row and not pd.isna(row[metric_name]) else np.nan
                records.append(record)
        else:
            row = df.iloc[-1]
            algorithm = file_algorithm
            record = {'Algorithm': algorithm}
            for metric_name in ['Silhouette Score', 'Davies-Bouldin Index', 'Quantization Error']:
                record[metric_name] = float(row[metric_name]) if metric_name in row and not pd.isna(row[metric_name]) else np.nan
            records.append(record)

    if not records:
        print(f"No valid metric records found in files under {output_dir}.")
        return

    compare_df = pd.DataFrame(records).set_index('Algorithm')
    compare_df = compare_df[['Silhouette Score', 'Davies-Bouldin Index', 'Quantization Error']]
    compare_df = compare_df.groupby(compare_df.index).last()

    if compare_df.empty:
        print(f"No valid metric values found in metric records under {output_dir}.")
        return

    plt.figure(figsize=(10, 6))
    compare_df.plot(kind='bar', rot=0)
    plt.title('Comparação de Métricas de Qualidade de Clusterização')
    plt.xlabel('Algoritmo')
    plt.ylabel('Valor')
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()

    output_file = os.path.join(output_dir, f'{output_prefix}.png')
    plt.savefig(output_file)
    plt.close()
    print(f"Metrics comparison chart saved to {output_file}")


def create_combinations(elements):
    """
    Creates all possible pairs from the given set of elements.
    Each pair contains two different elements (no self-pairing).
    
    Args:
        elements: An iterable containing the elements
        
    Returns:
        List of tuples, where each tuple is a unique pair
    """
    
    result = []
    elements_list = list(elements)
    
    for i in range(len(elements_list)):
        for j in range(i + 1, len(elements_list)):
            result.append((elements_list[i], elements_list[j]))
    
    return result

def append_variables_to_file(up, down, filename="variables.txt"):
    """
    Append the up and down variables to a text file.
    
    Args:
        up: The up variable to append
        down: The down variable to append
        filename: Name of the file to append to (default: variables.txt)
    """
    with open(filename, "a") as file:  # "a" mode for append instead of "w" for write
        #file.write(f"up={up},down={down}\n")  # One line per pair for easier processing later

        file.write(f"{up}\n")
        file.write(f"{down}\n")

def merge_all_interpolations_nbeat(file_rawdata):
    """
    Junta todos os arquivos map_{animal}_interpolation_nbeats.csv em um único arquivo.
    """
    results_dir = results_folder(file_rawdata)
    if not os.path.exists(results_dir):
        print(f"Diretório de resultados não encontrado: {results_dir}")
        return

    interpolation_dir = os.path.join(results_dir, "Interpolation")
    if not os.path.exists(interpolation_dir):
        print(f"Diretório de interpolação não encontrado: {interpolation_dir}")
        return

    excluded_animals = {"95", "100"}
    #excluded_animals = {"93", "94", "96", "97", "98", "99",}
    files = [f for f in os.listdir(interpolation_dir)
             if f.endswith("_interpolation_nbeats.csv")
             and os.path.basename(f).split("_")[1] not in excluded_animals]

    if not files:
        print("Nenhum arquivo nbeats encontrado para merge.")
        return

    try:
        dfs = [pd.read_csv(os.path.join(interpolation_dir, f), header=None) for f in files]
        if not dfs:
            print("Nenhum dado válido encontrado nos arquivos.")
            return

        df_merged = pd.concat(dfs, ignore_index=True)
        animal_name = os.path.basename(file_rawdata).split('.')[0]
        output_path = os.path.join(interpolation_dir, f"map_{animal_name}_interpolation_nbeats_all.csv")
        df_merged.to_csv(output_path, index=False, header=False)

        print(f"Arquivo gerado: {output_path}")
        return output_path
    except Exception as e:
        print(f"Erro ao realizar merge nbeats: {e}")
        return

def merge_all_interpolations_nhits(file_rawdata):
    """
    Junta todos os arquivos map_{animal}_interpolation_nhits.csv em um único arquivo.
    """
    results_dir = results_folder(file_rawdata)
    if not os.path.exists(results_dir):
        print(f"Diretório de resultados não encontrado: {results_dir}")
        return

    interpolation_dir = os.path.join(results_dir, "Interpolation")
    if not os.path.exists(interpolation_dir):
        print(f"Diretório de interpolação não encontrado: {interpolation_dir}")
        return

    files = [f for f in os.listdir(interpolation_dir) if f.endswith("_interpolation_nhits.csv")]

    if not files:
        print("Nenhum arquivo nhits encontrado para merge.")
        return
    
    try:
        dfs = [pd.read_csv(os.path.join(interpolation_dir, f), header=None) for f in files]
        if not dfs:
            print("Nenhum dado válido encontrado nos arquivos.")
            return

        df_merged = pd.concat(dfs, ignore_index=True)
        animal_name = os.path.basename(file_rawdata).split('.')[0]
        output_path = os.path.join(interpolation_dir, f"map_{animal_name}_interpolation_nhits_all.csv")
        df_merged.to_csv(output_path, index=False, header=False)
        
        print(f"Arquivo gerado: {output_path}")
        return output_path
    except Exception as e:
        print(f"Erro ao realizar merge nhits: {e}")
        return

def merge_all_interpolations_bilstm(file_rawdata):
    """
    Junta todos os arquivos map_{animal}_interpolation_bilstm.csv em um único arquivo.
    """
    results_dir = results_folder(file_rawdata)
    if not os.path.exists(results_dir):
        print(f"Diretório de resultados não encontrado: {results_dir}")
        return

    interpolation_dir = os.path.join(results_dir, "Interpolation")
    if not os.path.exists(interpolation_dir):
        print(f"Diretório de interpolação não encontrado: {interpolation_dir}")
        return

    excluded_animals = {"95", "100"}
    files = [f for f in os.listdir(interpolation_dir)
             if f.endswith("_interpolation_bilstm.csv")
             and os.path.basename(f).split("_")[1] not in excluded_animals]

    if not files:
        print("Nenhum arquivo bilstm encontrado para merge.")
        return
    
    try:
        dfs = [pd.read_csv(os.path.join(interpolation_dir, f), header=None) for f in files]
        if not dfs:
            print("Nenhum dado válido encontrado nos arquivos.")
            return

        df_merged = pd.concat(dfs, ignore_index=True)
        animal_name = os.path.basename(file_rawdata).split('.')[0]
        output_path = os.path.join(interpolation_dir, f"map_{animal_name}_interpolation_bilstm_all.csv")
        df_merged.to_csv(output_path, index=False, header=False)
        
        print(f"Arquivo gerado: {output_path}")
        return output_path
    except Exception as e:
        print(f"Erro ao realizar merge bilstm: {e}")
        return

def merge_maps(file_rawdata, list_animals):
    results_dir = results_folder(file_rawdata)
    animal_name = os.path.basename(file_rawdata).split('.')[0]
    
    # Coletar todos os arquivos map_{animal}.csv de todos os animais
    all_files = []
    for current_animal in list_animals:
        files = [f for f in os.listdir(results_dir) if f == f"map_{current_animal}.csv"]
        if files:
            all_files.extend(files)
            print(f"Encontrado arquivo para animal {current_animal}: {files[0]}")
        else:
            print(f"Nenhum arquivo map_{current_animal}.csv encontrado.")

    if not all_files:
        print("Nenhum arquivo map encontrado para merge.")
        return

    # Ler e mesclar TODOS os arquivos em um único DataFrame
    dfs = [pd.read_csv(os.path.join(results_dir, f), header=None) for f in all_files]
    df_merged = pd.concat(dfs, ignore_index=True)
    df_merged.dropna(subset=[2, 3], inplace=True)
    output_path = os.path.join(results_dir, f"map_{animal_name}_all_animals.csv")
    df_merged.to_csv(output_path, index=False, header=False)

    print(f"Arquivo único gerado com todos os animais: {output_path}")
    return output_path
 
def merge_csv(file_csv1, file_csv2, file_rawdata, animal_name, method):
    """
    Merge two CSV files by simple concatenation (no de-duplication).

    Args:
        file_csv1 (str): Base CSV path. The merged content is saved here.
        file_csv2 (str): Second CSV path to append.
        animal_name (str): Unused here; kept for interface compatibility.
    """
    # Read both CSVs as raw (no header) and concatenate
    if not os.path.exists(file_csv1):
        print(f"Base CSV not found: {file_csv1}")
        return

    if not os.path.exists(file_csv2):
        print(f"Second CSV not found: {file_csv2}")
        return

    try:
        df1 = pd.read_csv(file_csv1, header=None)
    except pd.errors.EmptyDataError:
        df1 = pd.DataFrame()

    try:
        df2 = pd.read_csv(file_csv2, header=None)
    except pd.errors.EmptyDataError:
        df2 = pd.DataFrame()

    results_dir = results_folder(file_rawdata)
    output_path = os.path.join(results_dir, f"map_interpolation_merged_{animal_name}_{method}.csv")
    print(results_dir)
    print(output_path)
    merged = pd.concat([df1, df2], ignore_index=True)
    merged.to_csv(path_or_buf=output_path, index=False, header=False)

    print(f"Arquivo de geral de animais(Interpolação e Maps) salvo em {output_path}")
    
    return output_path

def return_maps(file_rawdata, list_animals):
    results_dir = results_folder(file_rawdata)
    animal_name = os.path.basename(file_rawdata).split('.')[0]
    
    # Coletar todos os arquivos map_{animal}.csv de todos os animais
    all_files = []
    for current_animal in list_animals:
        files = [f for f in os.listdir(results_dir) if f == f"map_{current_animal}.csv"]
        if files:
            all_files.extend(files)
            print(f"Encontrado arquivo para animal {current_animal}: {files[0]}")
        else:
            print(f"Nenhum arquivo map_{current_animal}.csv encontrado.")

    if not all_files:
        print("Nenhum arquivo map encontrado para merge.")
        return None

    return all_files

def return_bilstm_list(file_rawdata, list_animals):
    results_dir = results_folder(file_rawdata)
    interpotalion_dir = os.path.join(results_dir, "Interpolation")
    animal_name = os.path.basename(file_rawdata).split('.')[0]
    
    # Coletar todos os arquivos map_{animal}_interpolation_bilstm.csv de todos os animais
    all_files = []
    for current_animal in list_animals:
        files = [f for f in os.listdir(interpotalion_dir) if f == f"map_{current_animal}_interpolation_bilstm.csv"]
        if files:
            all_files.extend(files)
            print(f"Encontrado arquivo BiLSTM para animal {current_animal}: {files[0]}")
        else:
            print(f"Nenhum arquivo map_{current_animal}_interpolation_bilstm.csv encontrado.")

    if not all_files:
        print("Nenhum arquivo BiLSTM encontrado para merge.")
        return None

    return all_files

def calculate_average_metrics(file_rawdata, method, list_animals):
    """
    Calcula a média geral das métricas de interpolação a partir dos arquivos JSON
    para uma lista de animais e um método específico.
    """
    results_dir = results_folder(file_rawdata)
    interpolation_dir = os.path.join(results_dir, "Interpolation")
    
    metrics_sum = {
        "rmse_deltas": 0.0,
        "mae_deltas": 0.0,
        "ade_meters": 0.0,
        "fde_meters": 0.0,
        "infeasible_steps_ratio": 0.0,
        "turning_angles_kl_divergence": 0.0,
        "sinuosity_ratio": 0.0,
        "area_difference_ratio": 0.0,
        "frechet_distance_meters": 0.0
    }
    
    valid_files_count = 0
    
    for animal in list_animals:
        # Padrão de nome do arquivo: metrics_{method}_{animal}.json
        file_path = os.path.join(interpolation_dir, f"metrics_{method.lower()}_{animal}.json")
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    
                metrics_sum["rmse_deltas"] += data.get("rmse_deltas", 0.0) or 0.0
                metrics_sum["mae_deltas"] += data.get("mae_deltas", 0.0) or 0.0
                metrics_sum["ade_meters"] += data.get("ade_meters", 0.0) or 0.0
                metrics_sum["fde_meters"] += data.get("fde_meters", 0.0) or 0.0
                
                biological = data.get("biological_metrics", {})
                if biological:
                    metrics_sum["infeasible_steps_ratio"] += biological.get("infeasible_steps_ratio", 0.0) or 0.0
                    metrics_sum["turning_angles_kl_divergence"] += biological.get("turning_angles_kl_divergence", 0.0) or 0.0
                    metrics_sum["sinuosity_ratio"] += biological.get("sinuosity_ratio", 0.0) or 0.0
                    metrics_sum["area_difference_ratio"] += biological.get("area_difference_ratio", 0.0) or 0.0
                    metrics_sum["frechet_distance_meters"] += biological.get("frechet_distance_meters", 0.0) or 0.0
                
                valid_files_count += 1
            except Exception as e:
                print(f"Erro ao ler {file_path}: {e}")
        else:
            print(f"Arquivo não encontrado: {file_path}")
            
    if valid_files_count > 0:
        metrics_avg = {k: v / valid_files_count for k, v in metrics_sum.items()}
        output_path = os.path.join(interpolation_dir, f"metrics_{method.lower()}_average.json")
        with open(output_path, 'w') as f:
            json.dump(metrics_avg, f, indent=4)
        print(f"Média calculada para {valid_files_count} arquivos ({method}) e salva em {output_path}")
        return metrics_avg
    else:
        print(f"Nenhum arquivo válido encontrado para calcular a média do método {method}.")
        return None

def plot_interpolation_comparisons(file_rawdata, list_methods):
    """
    Gera o Gráfico 1 (Desempenho Espacial) e o Gráfico 2 (Fidelidade Ecológica)
    comparando os métodos a partir dos arquivos de média gerados.
    """
    results_dir = results_folder(file_rawdata)
    interpolation_dir = os.path.join(results_dir, "Interpolation")
    
    data = {}
    for method in list_methods:
        filepath = os.path.join(interpolation_dir, f"metrics_{method.lower()}_average.json")
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                data[method] = json.load(f)
        else:
            print(f"Arquivo não encontrado: {filepath}")
            
    if not data:
        print("Dados insuficientes para gerar gráficos.")
        return
    
    methods = list(data.keys())
    
    # ----------------------------------------------------
    # GRÁFICO 1: Desempenho Espacial
    # ----------------------------------------------------
    g1_metrics = ['rmse_deltas', 'ade_meters', 'fde_meters', 'frechet_distance_meters']
    g1_labels = ['RMSE Deltas', 'ADE (m)', 'FDE (m)', 'Distância de Fréchet (m)']
    
    x = np.arange(len(g1_metrics))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c'] # Cores para diferenciar os modelos
    hatches = ['/', '.']  # Padrões de hachura para cada modelo
    
    for i, method in enumerate(methods):
        values = [data[method].get(m, 0) for m in g1_metrics]
        
        # Ajuste de posição dependendo da quantidade de métodos (funciona bem p/ 2 métodos)
        offset = x + (i * width) - (width * (len(methods) - 1) / 2)
        
        bars = ax.bar(offset, values, width, label=method, color=colors[i % len(colors)], hatch=hatches[i % len(hatches)])

        # Adicionar o valor acima das barras para o Gráfico 1
        for bar in bars:
            yval = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, yval + (0.01 * max(values)), 
                    f"{yval:.0f}", ha='center', va='bottom', fontsize=9)
        
    # Linha horizontal verde no eixo y em y = 0.1
    ax.axhline(y=0.2, color='green', linestyle='--', linewidth=3)

    #ax.set_ylabel('Distance / Error (m)')
    ax.set_ylabel('Distância / Erro (m)')
    #ax.set_title('Desempenho Espacial dos Modelos de Interpolação (Quanto Menor, Melhor)')
    #ax.set_title('Spatial Performance of Interpolation Models')
    ax.set_title('Desempenho Espacial dos Modelos de Interpolação')
    ax.set_xticks(x)
    ax.set_xticklabels(g1_labels)
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    
    plt.tight_layout()
    g1_path = os.path.join(interpolation_dir, 'grafico_1_desempenho_espacial.png')
    plt.savefig(g1_path, dpi=300)
    plt.close()
    
    # ----------------------------------------------------
    # GRÁFICO 2: Fidelidade Ecológica
    # ----------------------------------------------------
    g2_metrics = ['turning_angles_kl_divergence', 'sinuosity_ratio', 'area_difference_ratio']
    #g2_labels = ['TAKD\nIdeal: ~0', 'Razão de Sinuosidade\nIdeal: ~1.0', 'Diferença de Area de Vida\nIdeal: ~0']
    g2_labels = ['TAKD\n', 'Razão de Sinuosidade\n', 'Diferença de Area de Vida\n']
    #g2_labels = ['TAKD\n', 'Sinuosity Ratio\n', 'Area Difference Ratio\n']

    fig2, axes = plt.subplots(1, 3, figsize=(16, 6))
    fig2.suptitle('Fidelidade ecológica dos modelos de interpolação.', fontsize=16)
    #fig2.suptitle('Ecological fidelity of interpolation models.', fontsize=16)
    
    for idx, (metric, label) in enumerate(zip(g2_metrics, g2_labels)):
        ax = axes[idx]
        values = [data[method].get(metric, 0) for method in methods]
        bars = ax.bar(methods, values, color=colors[:len(methods)], hatch=hatches[:len(methods)])
        ax.set_title(label)
        ax.grid(axis='y', linestyle='--', alpha=0.7)

        ax.set_xticklabels([])
    
        # Add ideal value line and adjust limits
        if metric == 'turning_angles_kl_divergence':
            ax.axhline(y=0.01, color='green', linestyle='--', linewidth=3)
            ax.set_ylim(bottom=0)
            max_val = max(values) if values else 0
            ax.set_ylim(top=max(0.1, max_val * 1.2))
        elif metric == 'sinuosity_ratio':
            ax.axhline(y=1.0, color='green', linestyle='--', linewidth=3)
            max_val = max(values) if values else 0
            ax.set_ylim(top=max(1.1, max_val * 1.2))
        elif metric == 'area_difference_ratio':
            ax.axhline(y=0, color='green', linestyle='--', linewidth=3)

        for i, v in enumerate(values):
            offset = 0.05 * max([abs(val) for val in values] + [1])
            y_pos = v + offset if v >= 0 else v - offset
            format_str = "{:.4f}" if metric == 'turning_angles_kl_divergence' else "{:.2f}"
            ax.text(i, y_pos, format_str.format(v), ha='center', va='center', fontsize=11, fontweight='bold')

    # Create a single legend for all methods
    legend_handles = [plt.Rectangle((0, 0), 1, 1, color=colors[i % len(colors)], hatch=hatches[i % len(hatches)]) for i in range(len(methods))]
    fig2.legend(
        legend_handles,
        methods,
        loc='upper left',
        ncol=min(len(methods), 2),
        bbox_to_anchor=(0.02, 0.98),
        fontsize=10,
        title_fontsize=11,
        frameon=True,
        borderaxespad=0.3
    )
    fig2.subplots_adjust(top=0.92, right=0.96)

    plt.tight_layout()
    g2_path = os.path.join(interpolation_dir, 'grafico_2_fidelidade_ecologica.png')
    plt.savefig(g2_path, dpi=300)
    plt.close()

    print(f"Gráficos gerados com sucesso e salvos em:\n- {g1_path}\n- {g2_path}")
