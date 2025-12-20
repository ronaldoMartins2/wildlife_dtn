import json
import csv
import os
import numpy as np
import pandas as pd
from Data_preparation.data_field import DataField
from Data_preparation.raw_data_integration import get_id_from_json

interpolations_methods = ['N_BEATS', 'N_HITS']

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

def results_folder( file_rawdata_name ):

    file_name = file_rawdata_name.split('/')
    file_name = file_name[-1].split('.')[0]

    script_dir = os.path.dirname(os.path.abspath(__file__))

    script_dir = script_dir.replace('Common', '')

    results_dir = os.path.join(script_dir, f'Results/{file_name}')
    
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
    raw_path = os.path.join(results_dir, f'map_{current_animal}_outliers_less_test_only.csv')

    df_raw = _read_and_clean(raw_path)

    if method == 'N_BEATS':
        interp_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats.csv')
    elif method == 'N_HITS':
        interp_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nhits.csv')
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

    if method == 'N_BEATS':
        out_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats_merged.csv')
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
    interpolation_dir = os.path.join(results_dir, "Interpolation")
    files = [f for f in os.listdir(interpolation_dir) if f.endswith("_interpolation_nbeats.csv")]

    if not files:
        print("Nenhum arquivo nbeats encontrado para merge.")
        return

    dfs = [pd.read_csv(os.path.join(interpolation_dir, f), header=None) for f in files]
    df_merged = pd.concat(dfs, ignore_index=True)
    animal_name = os.path.basename(file_rawdata).split('.')[0]
    output_path = os.path.join(interpolation_dir, f"map_{animal_name}_interpolation_nbeats_all.csv")
    df_merged.to_csv(output_path, index=False, header=False)

    print(f"Arquivo gerado: {output_path}")
    return output_path

def merge_all_interpolations_nhits(file_rawdata):
    """
    Junta todos os arquivos map_{animal}_interpolation_nhits_merged.csv em um único arquivo.
    """
    results_dir = results_folder(file_rawdata)
    interpolation_dir = os.path.join(results_dir, "Interpolation")
    files = [f for f in os.listdir(interpolation_dir) if f.endswith("_interpolation_nhits_merged.csv")]

    if not files:
        print("Nenhum arquivo nhits encontrado para merge.")
        return
    
    dfs = [pd.read_csv(os.path.join(interpolation_dir, f), header=None) for f in files]
    df_merged = pd.concat(dfs, ignore_index=True)
    animal_name = os.path.basename(file_rawdata).split('.')[0]
    output_path = os.path.join(interpolation_dir, f"map_{animal_name}_interpolation_nhits_all.csv")
    df_merged.to_csv(output_path, index=False, header=False)
    
    print(f"Arquivo gerado: {output_path}")
    return output_path

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
    df_merged.dropna(subset=[2, 3])
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
    merged = pd.concat([df1, df2], ignore_index=True)
    merged.to_csv(path_or_buf=output_path, index=False, header=False)

    print(f"Arquivo de geral de animais(Interpolação e Maps) salvo em {output_path}")
    
    return output_path