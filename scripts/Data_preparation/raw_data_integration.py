import json

from Data_preparation.data_field import DataField

'''
def get_id_from_json(json_file_path, field):
    """
    Extract the ID field from a JSON file.
    
    Args:
        json_file_path (str): Path to the JSON file
        
    Returns:
        str: The value associated with the "ID" key
    """
    try:

        #script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
        #results_dir = os.path.join(script_dir, '..', 'rawdata')  # Navigate to the parent directory and into 'Results'
        #output_file = os.path.join(results_dir, f'map_{current_animal}.csv')

        # Open and read the JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        
        # Extract the ID field

        print( f'field {field} *********************')

        if field in data:
            return data[field]
        else:
            return None
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None
'''

def get_id_from_json(json_file_path, field):
    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)
          
        # Convert enum value to string
        field_str = str(field)
        
        # Case-insensitive search
        for key in data:
            if key.upper() == field_str.upper():
                return data[key]
        
        return None
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None