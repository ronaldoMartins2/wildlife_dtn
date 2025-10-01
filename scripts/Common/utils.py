import json
import csv
import os
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

def merge_csvs( current_animal, method, file_rawdata_name, file_rawdata_columns ):

    # Define the results directory and file path
    results_dir = results_folder( file_rawdata_name )

    #file_path = os.path.join(results_dir, f'map_{current_animal}.csv')  # Path to the CSV file
    file_path = os.path.join(results_dir, f'map_{current_animal}_outliers_less_test_only.csv')  # Path to the CSV file

    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return None

    # Read the CSV file into a DataFrame
    df_raw = pd.read_csv(file_path, header=None)

    df_raw.columns = ['ID', 'DateTime', 'Longitude', 'Latitude']

    if method == 'N_BEATS':
        file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats.csv')  # Path to the CSV file

    if method == 'N_HITS':
        file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nhits.csv')  # Path to the CSV file


    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return None

    # Read the CSV file into a DataFrame
    df_interpolation = pd.read_csv(file_path, header=None)
    df_interpolation.columns = ['ID', 'DateTime', 'Longitude', 'Latitude']

    result = pd.concat([df_raw, df_interpolation], axis=0)

    # Convert the 'DateTime' column to datetime type
    #result['DateTime'] = pd.to_datetime(result['DateTime'], format='%d/%m/%y %H:%M')
    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

    result['DateTime'] = pd.to_datetime(result['DateTime'], format=mask)

    # Sort the DataFrame by the 'DateTime' column
    df_sorted = result.sort_values(by='DateTime')

    columns_to_save = ['ID', 'DateTime', 'Longitude', 'Latitude']

    results_dir = results_folder( file_rawdata_name )

    if method == 'N_BEATS':    
        file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats_merged.csv')

    if method == 'N_HITS':    
        file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nhits_merged.csv')


    hiper_content = []
    hiper_content.append( f"Total merged {len(df_sorted)} método {method} animal {current_animal}" )

    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')

    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')    

    df_sorted[columns_to_save].to_csv( file_path, index=False, header=False)

def get_list_animals(file_name, file_rawdata_columns):
    # Open the CSV file
    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Create a set to store unique IDs
        ids = set()
        
        # Iterate through each row and add the ID to the set
        for row in reader:
            #ids.add(row['individual.local.identifier (ID)'])

            STR_ID = row[ get_id_from_json(file_rawdata_columns, DataField.ID) ]
            STR_ID = STR_ID.replace(' ', '')

            ids.add( STR_ID )
    
    # Convert the set back to a list before returning
    return list(ids)

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
