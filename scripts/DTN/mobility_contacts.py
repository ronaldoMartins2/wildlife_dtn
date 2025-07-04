import os
import pandas as pd
from geopy.distance import geodesic
import sys
from Common.utils import (
    create_clusterization_results,
    append_variables_to_file,
    CONTACT_DISTANCE
)

def find_min_max_dates(file_path):
    """
    Reads a CSV file and identifies the earliest and latest dates in the 'Datetime' column.
    """
    data = pd.read_csv(file_path, header=None)
    data.columns = ['ID', 'Datetime', 'Longitude', 'Latitude']
    data['Datetime'] = pd.to_datetime(data['Datetime'], format='%m/%d/%y %H:%M', errors='coerce')
    data = data.dropna(subset=['Datetime'])
    min_date = data['Datetime'].min()
    max_date = data['Datetime'].max()
    return min_date, max_date

def process_files(file1, file2, file_number_onca1, file_number_onca2):
    suffix1 = f'__{file_number_onca1}'
    suffix2 = f'__{file_number_onca2}'

    # Load data
    df1 = pd.read_csv(file1, header=None, names=['ID', 'Datetime', 'Longitude', 'Latitude'])
    df2 = pd.read_csv(file2, header=None, names=['ID', 'Datetime', 'Longitude', 'Latitude'])

    # Convert to datetime
    df1['Datetime'] = pd.to_datetime(df1['Datetime'], format='%m/%d/%y %H:%M', errors='coerce')
    df2['Datetime'] = pd.to_datetime(df2['Datetime'], format='%m/%d/%y %H:%M', errors='coerce')

    # Merge datasets on datetime
    merged = pd.merge(df1, df2, on='Datetime', suffixes=(suffix1, suffix2))

    print("Merged columns:", list(merged.columns))

    # Compute geodesic distance using iterrows (evita erro do apply)
    print(f" distance ######################################################################")
    distances = []
    for _, row in merged.iterrows():
        coord1 = (row[f'Latitude{suffix1}'], row[f'Longitude{suffix1}'])
        coord2 = (row[f'Latitude{suffix2}'], row[f'Longitude{suffix2}'])
        distance = geodesic(coord1, coord2).meters

        print(f" distance {distance}")

        distances.append(distance)

    merged['Distance'] = distances

    # Filter by distance
    filtered = merged[merged['Distance'] < CONTACT_DISTANCE]

    create_clusterization_results('Results/DTN')
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results/DTN')  # Navigate to the parent directory and into 'Results'
    output_DTN_filename = os.path.join(results_dir, f'contacts_DTN.txt')


    # Generate output with up and down events
    for i, row in filtered.iterrows():

        up = f"{i*5} CONN {row[f'ID{suffix1}']} {row[f'ID{suffix2}']} up"
        down = f"{(i*5) + 5} CONN {row[f'ID{suffix1}']} {row[f'ID{suffix2}']} down" 
        print(up)
        print(down)

        append_variables_to_file(up, down, output_DTN_filename)

    # Export DTN contacts file


    # Export to CSV

    create_clusterization_results('Results/DTN')
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results/DTN')  # Navigate to the parent directory and into 'Results'
    output_filename = os.path.join(results_dir, f'contacts_{file_number_onca1}_{file_number_onca2}.csv')

    filtered.to_csv(output_filename, index=False)
    print(f"\nFiltered contacts saved to '{output_filename}'")

def run(file_number_onca1, file_number_onca2):

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results/Interpolation')  # Navigate to the parent directory and into 'Results'
    
    file_path_1 = os.path.join(results_dir, f'map_{file_number_onca1}_interpolation_nbeats.csv')
    file_path_2 = os.path.join(results_dir, f'map_{file_number_onca2}_interpolation_nbeats.csv')

    # Rodar o script
    process_files(file_path_1, file_path_2, file_number_onca1, file_number_onca2)

def run_mock():
    file_number_onca1 = sys.argv[1]
    file_number_onca2 = sys.argv[2]
    run(file_number_onca1, file_number_onca2)

