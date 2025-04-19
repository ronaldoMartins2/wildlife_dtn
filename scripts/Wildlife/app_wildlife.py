
import sys
import os
import time

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
    get_list_animals,
    merge_csvs,
    create_pairs
) 

from Data_preparation.separar_localizacoes_individuais import (
    run as run_preparation
)

from Interpolation.nbeat_interpolation import (
    run as run_interpolation_nbeat
)

'''
from Interpolation.nhits_interpolation import (
    run as run_interpolation_nhits
)
'''


from Evaluation.average_by_individual import (
    run as run_average_by_individual,
    calc_average_by_method,
    get_len_animal,
    get_top_botom_date
)

from Evaluation.average_comparison import (
    run as run_average_comparison
)

from Common.utils import (
    interpolations_methods
)

from Clusterization.kmeans_individual_csv import (
    run as run_kmeans
)

from Clusterization.SOM_individual import (
    run as run_som
)

from Clusterization.Mean_Shift import (
    run as run_mean_shift
)

from Clusterization.BIRCH import (
    run as run_birch
)

from DTN.contacts import (
    run as run_contacts
)

# data preparation

file_rawdata = sys.argv [1]

list_animals = get_list_animals( file_rawdata )

len_animals = len(list_animals)

print(f' list_animals { len_animals }')

#print(f'{list_animals}')

for current_animal in list_animals:
    run_preparation( current_animal, file_rawdata )

    run_average_by_individual( current_animal )
    
time.sleep(2)



for current_animal in list_animals:
    #TODO review number_of_predictions 
    number_of_predictions = 5
    len_animal = get_len_animal( current_animal )
    run_interpolation_nbeat(current_animal, number_of_predictions, len_animal)

'''
for current_animal in list_animals:

    len_animal = get_len_animal( current_animal )
    start_date, end_date = get_top_botom_date( current_animal )
    run_interpolation_nhits(current_animal, len_animal, start_date, end_date)
'''   

'''
for current_animal in list_animals:

    merge_csvs( current_animal, 'N_BEATS' )
    merge_csvs( current_animal, 'N_HITS' )

for current_animal in list_animals:

    calc_average_by_method( current_animal, 'N_BEATS' )
    calc_average_by_method( current_animal, 'N_HITS' )

run_average_comparison( len_animals )
'''

# run clusterization kmeans
'''
for current_animal in list_animals:

    #run_kmeans(current_animal)
    run_som(current_animal)
'''
#run_som(95)
#run_mean_shift(94)
#run_kmeans(93)
#run_som(93)

#run_birch(94)

# run clusterization SOM

# run clusterization Mean Shift

#DTN Contacts
#criar os conjunto dois a dois sem repetição

pairs = create_pairs(list_animals)

for pair in pairs:
    run_contacts(pair[0], pair[1])

#print('pairs ')
#print(pairs)

#run_contacts(93, 94)


