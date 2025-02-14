import json
import csv

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
