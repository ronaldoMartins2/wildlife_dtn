
import sys
import os

# environment preparation

# pip3 install -r requirements.txt 

# python3 -m venv venv
# source ./venv/bin/activate

# to run

# inside wildlife_dtn folder
# python3 scripts/Wildlife/app_wildlife.py rawdata/jaguar_mamiraua.csv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from Common.utils import (
    read_field_from_json,
    get_list_animals
) 

from Data_preparation.separar_localizacoes_individuais import (
    run as run_preparation
)

from Interpolation.nbeat_interpolation import (
    run as run_interpolation_nbeat
)

from Interpolation.nhits_interpolation import (
    run as run_interpolation_nhits
)

# data preparation

file_rawdata = sys.argv [1]

list_animals = get_list_animals( file_rawdata )

print(f' list_animals {len(list_animals)}')

#print(f'{list_animals}')

for current_animal in list_animals:
    run_preparation(current_animal, file_rawdata)


for current_animal in list_animals:
    #TODO review number_of_predictions 
    number_of_predictions = 5
    run_interpolation_nbeat(current_animal, number_of_predictions)


#for current_animal in list_animals:
#    run_interpolation_nhits(current_animal)
   

# run_interpolation('94')

# python 3 Interpolation/5_nbeat_interpolation.py 94 1032


# python3 Data_preparation/1_separar_localizacoes_individuais.py 94

# interpolation

# python3 5_nbeat_interpolation.py 94 n
