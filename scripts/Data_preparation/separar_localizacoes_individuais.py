#Código-fonte 1 – Script em python para obter as localizações individuais

# para executar
# python3 1_separar_localizacoes_individuais.py 94

import csv
import sys
import os
from datetime import datetime as dt
from Data_preparation.raw_data_integration import get_id_from_json
from Data_preparation.data_field import DataField
from Common.utils import results_folder


def run( current_animal, file_rawdata_name, file_rawdata_columns ):

    hiper_content = []

    results_dir = results_folder( file_rawdata_name )

    output_file = os.path.join(results_dir, f'map_{current_animal}.csv')

    file = open( output_file, 'w')

    fields = ( 'id', 'time', 'long', 'lat')
    writer = csv.DictWriter( file, fieldnames = fields, lineterminator= '\n')

    mask = get_id_from_json(file_rawdata_columns, DataField.DATETIME_MASK)

    # empty dictionary
    list_animals = { }

    actualTimeEvenStr = '1/1/2022 17:39'
    actualTimeEven = dt.strptime ( actualTimeEvenStr, "%m/%d/%Y %H:%M" )

    with open ( file_rawdata_name ) as csv_file :

        csv_reader = csv.DictReader(csv_file)

        rows = list(csv_reader)
        if rows:
            print(f"There is data in {file_rawdata_name}!")
        else:
            print(f"No data found in {file_rawdata_name}.")
            return
            
        line_count = 0

        for row in rows :

            if line_count == 0:
                line_count += 1
            else :
                currentTime = dt.strptime ( row[ get_id_from_json(file_rawdata_columns, DataField.DATETIME) ] , mask )

                if currentTime < actualTimeEven :
                    actualTimeEven = currentTime

                list_animals [ row[ get_id_from_json(file_rawdata_columns, DataField.ID) ] ] = 'id'

                line_count += 1
        line_count -= 1

        print(f'>>>>>>>>>>>>> line_count total {line_count} for animal {current_animal}')

    count_animal = 0

    with open ( file_rawdata_name ) as csv_file :

        csv_reader = csv.DictReader(csv_file)

        line_count = 0
        
        for row in csv_reader :
            if line_count == 0:
                line_count += 1
            else :

                id_raw_data = row[ get_id_from_json(file_rawdata_columns, DataField.ID) ]
                id_raw_data = id_raw_data.replace(" ", "")

                list_animals[ id_raw_data ] = 'id'

                if id_raw_data == current_animal :

                    writer.writerow({
                        'id': id_raw_data,
                        'time': row[ get_id_from_json(file_rawdata_columns, DataField.DATETIME) ],
                        'long': row[ get_id_from_json(file_rawdata_columns, DataField.LONGITUDE) ],
                        'lat': row[ get_id_from_json(file_rawdata_columns, DataField.LATITUDE) ]
                    })
                    count_animal += 1

                line_count += 1
        
        print ( f' Processed {line_count} lines. ' )
        hiper_content.append( f'{file_rawdata_name} animal {current_animal} Processed {count_animal} from total {line_count} lines. ' )

    results_dir = results_folder(file_rawdata_name)
    hiper_path = os.path.join(results_dir, f'hiperparameters.txt')
    with open(hiper_path, "a") as file:
        for line in hiper_content:
            file.write(line + '\n')

    file.close( )

def run_mock( ):
    
    current_animal = sys.argv [1]
    file_rawdata_name = '../Results/jaguar_mamiraua.csv'

    run( current_animal, file_rawdata_name )