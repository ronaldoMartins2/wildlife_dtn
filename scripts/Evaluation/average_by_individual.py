import pandas as pd
from datetime import datetime
import sys
import os
from Data_preparation.data_field import DataField
from Data_preparation.raw_data_integration import get_id_from_json

from Common.utils import (
    interpolations_methods,
    results_folder
)

def get_average_nhits_by_animal_sorted( file_rawdata_name ):

    results_dir = results_folder( file_rawdata_name )

    file_path = os.path.join(results_dir, f'Interpolation/averages_nhits.csv')

    # Read the CSV file
    df = pd.read_csv( file_path )

    # Extract the 'average_animal' column and convert it to a list
    average_animal_values = df['average_nhits'].tolist()

    # Sort the list in ascending order
    sorted_average_animal = sorted(average_animal_values)

    return sorted_average_animal

def get_average_nbeats_by_animal_sorted( file_rawdata_name ):

    results_dir = results_folder( file_rawdata_name )

    file_path = os.path.join(results_dir, f'Interpolation/averages_nbeats.csv')

    # Read the CSV file
    df = pd.read_csv( file_path )

    # Extract the 'average_animal' column and convert it to a list
    average_animal_values = df['average_nbeats'].tolist()

    # Sort the list in ascending order
    sorted_average_animal = sorted(average_animal_values)

    return sorted_average_animal


def get_average_by_animal_sorted( file_rawdata_name ):

    results_dir = results_folder( file_rawdata_name )

    file_path = os.path.join(results_dir, f'averages.csv')

    # Read the CSV file
    df = pd.read_csv( file_path )

    # Extract the 'average_animal' column and convert it to a list
    average_animal_values = df['average_animal'].tolist()

    # Sort the list in ascending order
    sorted_average_animal = sorted(average_animal_values)

    return sorted_average_animal

def get_id_animal_sorted( file_rawdata_name ):

    results_dir = results_folder( file_rawdata_name )

    file_path = os.path.join(results_dir, f'averages.csv')

    # Read the CSV file
    df = pd.read_csv( file_path )

    # Extract the 'average_animal' column and convert it to a list
    id_animal_values = df['current_animal'].tolist()

    # Remove duplicates using set and then sort
    unique_id_animal = list(set(id_animal_values))

    # Sort the list in ascending order
    sorted_id_animal = sorted(unique_id_animal)

    return sorted_id_animal

def calc_average_by_method(current_animal, methods, file_rawdata_name):

    print(f'file_rawdata_name >>>>>>>>> {file_rawdata_name}')

    results_dir = results_folder( file_rawdata_name )

    print(f'results_dir >>>>>>>> {results_dir}')

    if methods == 'N_BEATS':
        file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats_merged.csv')
        file_to_save = os.path.join(results_dir, 'Interpolation/averages_nbeats.csv')
        method_to_save = 'average_nbeats'
    
    if methods == 'N_HITS':
        file_path = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nhits_merged.csv')
        file_to_save = os.path.join(results_dir, 'Interpolation/averages_nhits.csv')
        method_to_save = 'average_nhits'

    print(f'file_path >>>>>>>>>>>>>>>>>>>>>> {file_path}')

    # Adiciona verificação para ver se o arquivo existe antes de tentar ler
    if not os.path.exists(file_path):
        print(f"Arquivo de merge não encontrado para o animal {current_animal} e método {methods}. Pulando.")
        return

    #data = pd.read_csv(file_path, header=None)

    # Read the CSV file (assuming no header)
    df = pd.read_csv(file_path, header=None, names=['animal_id', 'datetime', 'longitude', 'latitude'])

    # Convert datetime column to pandas datetime
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Sort by datetime to ensure proper order
    df = df.sort_values('datetime').reset_index(drop=True)

    # Calculate time differences between consecutive records
    time_diffs = df['datetime'].diff()

    # Remove the first NaN value (no previous record to compare with)
    time_diffs = time_diffs.dropna()

    # Convert to hours
    time_diffs_hours = time_diffs.dt.total_seconds() / 3600

    # Calculate average interval in hours
    average_interval_hours = time_diffs_hours.mean()

    print(f"Average datetime interval: {average_interval_hours:.2f} hours")
    print(f"Number of intervals: {len(time_diffs_hours)}")
    print(f"Min interval: {time_diffs_hours.min():.2f} hours")
    print(f"Max interval: {time_diffs_hours.max():.2f} hours")

    append_to_csv(current_animal, average_interval_hours, file_to_save, method_to_save, file_rawdata_name )

    '''
    # Convert the second column to datetime objects
    #data[1] = pd.to_datetime(data[1], format='%Y-%m-%d %H:%M:%S')
    data[1] = pd.to_datetime(data[1], format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')

    #print(f' data[1] {data[1]}')

    #data[1] = pd.to_datetime(data[1], format='%d/%m/%y %H:%M')
    #data[1] = pd.to_datetime(data[1], format='%m/%d/%y %H:%M')

    # Calculate the datetime deltas (differences) between consecutive rows
    data['delta'] = data[1].diff()

    # Remove the first NaT (Not a Time) value as it has no previous value to subtract from
    data = data.dropna(subset=['delta'])

    # Calculate the average time delta
    average_delta = data['delta'].mean()

    # Convert the average timedelta to hours
    average_hours = average_delta.total_seconds() / 3600  # Convert seconds to hours

    # Output the average delta in hours
    print(f"Average time delta in hours: {average_hours:.2f} hours")

    # Use the calculated average as average_nbeats for the append function
    append_to_csv(current_animal, average_hours, file_to_save, method_to_save, file_rawdata_name )

    '''

def get_len_animal(current_animal, file_rawdata_name):
    # Define the results directory and file path

    results_dir = results_folder( file_rawdata_name )

    file_path = os.path.join(results_dir, 'averages.csv')  # Path to the CSV file

    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return None

    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Filter the DataFrame to find the row with the given current_animal
    #df['current_animal'] = df['current_animal'].astype(int)
    #row = df[df['current_animal'] == int(current_animal)]

    print(f'current_animal>>> {current_animal} file_path {file_path}')

    #df['current_animal'] = df['current_animal']
    #row = df[df['current_animal'] == current_animal]

    # Convert current_animal to int if possible, otherwise keep as string
    try:
        current_animal = int(current_animal)
    except ValueError:
        current_animal = str(current_animal)

    # Update the DataFrame column (if needed)
    df['current_animal'] = df['current_animal'].apply(lambda x: int(x) if str(x).isdigit() else str(x))

    # Filter the DataFrame
    row = df[df['current_animal'] == current_animal]

    # Check if the row exists
    if row.empty:
        print(f"No data found for current_animal: {current_animal}")
        return None

    # Extract the len_animal value
    len_animal = row['len_animal'].values[0]
    return len_animal

def get_top_botom_date(current_animal, file_rawdata_name, file_rawdata_columns):

    results_dir = results_folder( file_rawdata_name )

    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')

    df = pd.read_csv( file_path, header=None, names=['animal_id', 'timestamp', 'longitude', 'latitude'])

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

    # Convert the 'timestamp' column to datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'], format=mask)

    # Robust timestamp parsing: normalize, remove header-like rows, try mask then fallback to infer/coerce
    df['timestamp'] = df['timestamp'].astype(str).str.strip()
    header_mask = df['timestamp'].str.lower().isin(['timestamp', 'datetime', 'date', 'time'])
    if header_mask.any():
        df = df.loc[~header_mask].reset_index(drop=True)

    if mask:
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format=mask)
        except Exception:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', infer_datetime_format=True)
    else:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', infer_datetime_format=True)

    # Drop invalid timestamps
    df = df.dropna(subset=['timestamp']).reset_index(drop=True)

    if df.empty:
        raise RuntimeError(f"No valid timestamps found in {file_path} after parsing.")

    # Find the earliest and latest dates
    earliest_date = df['timestamp'].min()
    latest_date = df['timestamp'].max()

    return earliest_date, latest_date

def get_existing_values(current_animal):
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results')  # Navigate to the parent directory and into 'Results'
    
    # Define the file path for 'averages.csv'
    file_path = os.path.join(results_dir, 'averages.csv')

    # Check if the 'averages.csv' file exists
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return None, None

    df = pd.read_csv(file_path)

    # Print the content of the CSV
    print(df)

    
    # Read the CSV file into a DataFrame
    #df = pd.read_csv(file_path)
    
    # Find the row corresponding to the current_animal
    df['current_animal'] = df['current_animal'].astype(int)
    row = df[df['current_animal'] == int(current_animal)]    
    
    if row.empty:
        print(f"Data for current_animal {current_animal} not found in {file_path}.")
        return None, None
    else:
        # Extract average_animal and len_animal from the row
        average_animal = row['average_animal'].values[0]
        len_animal = row['len_animal'].values[0]
        return average_animal, len_animal

def append_to_csv(current_animal, average_nbeats, file_to_save, method_variable, file_rawdata_name):

    # Define the results directory and file path
    #script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    #results_dir = os.path.join(script_dir, '..', 'Results')  # Navigate to the parent directory and into 'Results'
    
    results_dir = results_folder( file_rawdata_name )

    # Define the file path for 'averages_nbeats.csv'
    file_path = os.path.join(results_dir, file_to_save)

    # Create a DataFrame for the new row with average_nbeats
    new_row = pd.DataFrame({
        'current_animal': [current_animal],
        method_variable: [average_nbeats]
    })


    # Check if the file exists
    if not os.path.exists(file_path):
        # If the file doesn't exist, create it and write the header

        new_row.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Created new file and saved data for current_animal: {current_animal}")
    
    
    else:
        # If the file exists, read the existing data
        df_existing = pd.read_csv(file_path)

        # If it doesn't exist, append the new row
        df_existing = pd.concat([df_existing, new_row], ignore_index=True)
        print(f"Appended new row for current_animal: {current_animal}")

        # Save the updated DataFrame back to the CSV file
        df_existing.to_csv(file_path, index=False)
    

def run( current_animal, file_rawdata_name, file_rawdata_columns ):

    results_dir = results_folder( file_rawdata_name )
    file_path = os.path.join(results_dir, f'map_{current_animal}.csv')

    # Lê o arquivo CSV
    try:
        df = pd.read_csv( file_path, header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
    except FileNotFoundError:
        print(f"Arquivo {file_path} não encontrado.")
        sys.exit(1)

    # Verifica se o DataFrame está vazio
    if df.empty:
        print("O arquivo CSV está vazio.")
        sys.exit(1)

    # Converte a coluna 'Timestamp' para datetime
    try:
        #df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%m/%d/%y %H:%M')
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format=get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK))

    except ValueError as e:
        print(f"Erro ao converter Timestamp: {e}")
        sys.exit(1)

    # Remove duplicatas de timestamps
    df = df.drop_duplicates(subset='Timestamp')

    # Verifica se há pelo menos 2 registros para calcular a diferença
    #if len(df) < 2:
    #if len(df) < 2:
    #    print( f"O arquivo contém menos de 2 registros. Não é possível calcular a média. {current_animal}" )
    #    sys.exit(1)

    # Ordena os timestamps
    df = df.sort_values(by='Timestamp')

    # Calcula as diferenças de tempo
    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600

    # Agrega por hora se necessário (opcional para alta frequência)
    #aggregate = input("\nDeseja agregar os dados por hora? (s/n): ").strip().lower()
    #if aggregate == 's':
    #    df = df.resample('1H', on='Timestamp').first().dropna().reset_index()
    #    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600

    # Calcula a média das diferenças de tempo
    average_time_difference = df['Time Difference (hours)'].mean()

    #print(f"Average time difference (in hours) between consecutive timestamps: {average_time_difference:.2f} hours")
    #print(f"Total de registros processados: {len(df)}")

    if len(df) < 2:
        average_animal = 0
    else:
        average_animal = average_time_difference

    len_animal = len(df)

    # Create a DataFrame from the variables
    data = {'average_animal': [average_animal],
            'len_animal': [len_animal],
            'current_animal': [current_animal]}

    df_new = pd.DataFrame(data)

    #script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    #results_dir = os.path.join(script_dir, '..', 'Results')  # Navigate to the parent directory and into 'Results'
    
    results_dir = results_folder( file_rawdata_name )
    file_path = os.path.join(results_dir, f'averages.csv')

    if os.path.exists(file_path):
        # Read the existing CSV into a DataFrame
        df_existing = pd.read_csv(file_path)
        
        # Remove any duplicate rows based on 'current_animal'
        df_existing = df_existing.drop_duplicates(subset='current_animal', keep='last')
        
        # Check if 'current_animal' already exists in the DataFrame
        if current_animal in df_existing['current_animal'].values:
            # If current_animal exists, update the corresponding row
            df_existing.loc[df_existing['current_animal'] == current_animal, ['average_animal', 'len_animal']] = average_animal, len_animal
            print(f"Updated row for current_animal: {current_animal}")
        else:
            # If current_animal does not exist, append the new row
            df_existing = pd.concat([df_existing, df_new], ignore_index=True)
            print(f"Appended new row for current_animal: {current_animal}")
        
        # Reapply drop_duplicates to ensure no duplicates remain
        df_existing = df_existing.drop_duplicates(subset='current_animal', keep='last')
        
        # Save the updated DataFrame back to the CSV file
        df_existing.to_csv(file_path, index=False)
    else:
        # If the file doesn't exist, create it and save the data
        df_new.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Created new file and saved data for current_animal: {current_animal}")

def run_mock( ):

    # Verifica os argumentos de entrada
    if len(sys.argv) < 2:
        print("Uso: python script.py <source_file>")
        sys.exit(1)

    current_animal = sys.argv [1]
