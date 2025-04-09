import json
import csv
import os
import pandas as pd

interpolations_methods = ['N_BEATS', 'N_HITS']

def merge_csvs( current_animal, method ):

    print('call merge_csvs %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')

    # Define the results directory and file path
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results')  # Navigate to the parent directory and into 'Results'
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')  # Path to the CSV file

    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return None

    # Read the CSV file into a DataFrame
    df_raw = pd.read_csv(file_path)
    df_raw.columns = ['ID', 'DateTime', 'Longitude', 'Latitude']

    if method == 'N_BEATS':
        file_path = os.path.join(results_dir, f'map_{current_animal}_interpolation_nbeats.csv')  # Path to the CSV file

    if method == 'N_HITS':
        file_path = os.path.join(results_dir, f'map_{current_animal}_interpolation_nhits.csv')  # Path to the CSV file


    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return None

    # Read the CSV file into a DataFrame
    df_interpolation = pd.read_csv(file_path)
    df_interpolation.columns = ['ID', 'DateTime', 'Longitude', 'Latitude']

    result = pd.concat([df_raw, df_interpolation], axis=0)

    print('####################################################################################################################')
    print(result.head(10))

    # Convert the 'DateTime' column to datetime type
    #result['DateTime'] = pd.to_datetime(result['DateTime'], format='%d/%m/%y %H:%M')
    result['DateTime'] = pd.to_datetime(result['DateTime'], format='%m/%d/%y %H:%M')

    # Sort the DataFrame by the 'DateTime' column
    df_sorted = result.sort_values(by='DateTime')

    columns_to_save = ['ID', 'DateTime', 'Longitude', 'Latitude']

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results')  # Navigate to the parent directory and into 'Results'

    if method == 'N_BEATS':    
        file_path = os.path.join(results_dir, f'map_{current_animal}_interpolation_nbeats_merged.csv')

    if method == 'N_HITS':    
        file_path = os.path.join(results_dir, f'map_{current_animal}_interpolation_nhits_merged.csv')


    df_sorted[columns_to_save].to_csv( file_path, index=False, header=False)


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


def get_list_animals(file_name):
    # Open the CSV file
    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Create a set to store unique IDs
        ids = set()
        
        # Iterate through each row and add the ID to the set
        for row in reader:
            ids.add(row['individual.local.identifier (ID)'])
    
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

    #script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    #results_dir = os.path.join(script_dir, '..', 'Results/Clusterization')  # Navigate to the parent directory and into 'Results'
    #file_name = os.path.join(results_dir, f'onca_{current_animal}_kmeans.png')