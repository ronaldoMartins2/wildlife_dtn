
import sys
import os
import time

# environment preparation

# pip3 install -r scripts/requirements.txt 

# python3 -m venv venv
# source ./venv/bin/activate

# to run

# inside wildlife_dtn folder
# python3 scripts/Wildlife/app_wildlife.py rawdata/jaguar_mamiraua.csv rawdata/jaguar_columns.json
# python3 scripts/Wildlife/app_wildlife.py rawdata/tangara_mata_atlantica.csv rawdata/tangara_columns.json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from Common.utils import (
    read_field_from_json,
    get_list_animals,
    merge_csvs,
    create_pairs,
    results_folder
)

from Data_preparation.separar_localizacoes_individuais import (
    run as run_preparation
)

from Interpolation.nhits_trainer import (
    main_training,
    main_training_list
)

from Interpolation.nbeat_trainer import (
    train_nbeats_model_single,
    train_nbeats_model_list
)

from Interpolation.nbeat_interpolation import (
    run as run_interpolation_nbeat
)

from Interpolation.nhits_interpolation import (
    run as run_interpolation_nhits
)

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

from Clusterization.plot_kmeans_som_birch_mean_shift import (
    run as run_plot_kmeans_som_birch_mean_shift
)   

from DTN.contacts import (
    run as run_contacts
)

# data preparation

file_rawdata = sys.argv [1]
file_rawdata_columns = sys.argv [2]

results_dir = results_folder(file_rawdata)
hiper_path = os.path.join(results_dir, 'hiperparameters.txt')

# Check if the file exists before trying to delete it
if os.path.exists(hiper_path):
    os.remove(hiper_path)
    print(f"Deleted: {hiper_path}")
else:
    print(f"No file found at: {hiper_path}")

list_animals = get_list_animals( file_rawdata, file_rawdata_columns )

len_animals = len(list_animals)

print(f' list_animals { len_animals }')

print(f'{list_animals}')


for current_animal in list_animals:
    run_preparation( current_animal, file_rawdata, file_rawdata_columns )
    run_average_by_individual( current_animal, file_rawdata, file_rawdata_columns )
    
time.sleep(2)


#run_preparation( 93, file_rawdata, file_rawdata_columns )
#run_preparation( 'G54907', file_rawdata, file_rawdata_columns )
#run_average_by_individual( 'G54907', file_rawdata, file_rawdata_columns )


############## INTERPOLATION ##############################

tangara = file_rawdata.split('.')[-2]
tangara = tangara.split('/')[-1]

if tangara == 'tangara_mata_atlantica':
    #list_animals = ['E62718', 'E62726', 'OR34MGA', 'G21547', 'E62705', 'E57527', 'E49920', 'E62722', 'G56076' ]
    list_animals = ['OR34MGA' ]
#else:
#    list_animals = [94]
# E62724 loop
# G56068 empty

# 93 loop infinito  - nhits
# 95 loop infinito  - nhits

# TRAIN step

#for current_animal in list_animals:
#    trainer = main_training(current_animal, file_rawdata, file_rawdata_columns)

main_training_list(list_animals, file_rawdata, file_rawdata_columns)
#sys.exit()

#train_nbeats_model_single(current_animal, file_rawdata, file_rawdata_columns)
#train_nbeats_model_list(list_animals, file_rawdata, file_rawdata_columns)
#sys.exit()

for current_animal in list_animals:
    #TODO review number_of_predictions 
    number_of_predictions = 5
    len_animal = get_len_animal( current_animal, file_rawdata )
    
    print(f'len_animal {len_animal} current_animal {current_animal} file_rawdata {file_rawdata}')

    #sys.exit()

    run_interpolation_nbeat(current_animal, number_of_predictions, len_animal, file_rawdata, file_rawdata_columns)

    start_date, end_date = get_top_botom_date( current_animal, file_rawdata, file_rawdata_columns )

    run_interpolation_nhits(current_animal, len_animal, start_date, end_date, file_rawdata, file_rawdata_columns)


sys.exit()


'''
number_of_predictions = 5
len_animal = get_len_animal( 'G54907', file_rawdata )
run_interpolation_nbeat( 'G54907', number_of_predictions, len_animal, file_rawdata, file_rawdata_columns)
'''


''''

start_date, end_date = get_top_botom_date( 93 )
run_interpolation_nhits(93, len_animal, start_date, end_date)
'''

for current_animal in list_animals:

    merge_csvs( current_animal, 'N_BEATS', file_rawdata, file_rawdata_columns )
    merge_csvs( current_animal, 'N_HITS', file_rawdata, file_rawdata_columns )


for current_animal in list_animals:

    calc_average_by_method( current_animal, 'N_BEATS', file_rawdata )
    calc_average_by_method( current_animal, 'N_HITS', file_rawdata )


run_average_comparison( len_animals, file_rawdata )

sys.exit()

'''

############## CLUSTERIZATION ##############################
# run clusterization kmeans

#for current_animal in list_animals:

    #run_kmeans(current_animal)
    #run_som(current_animal)

'''
run_kmeans(93)
run_som(93)
run_mean_shift(93)
run_birch(93)     
'''

#run_som(95)
#run_mean_shift(94)
#run_kmeans(93)
#run_som(93)

#run_birch(94)

# run clusterization SOM

# run clusterization Mean Shift

'''
for current_animal in list_animals:

        run_plot_kmeans_som_birch_mean_shift(current_animal)
'''

run_plot_kmeans_som_birch_mean_shift(93)

############## #DTN Contacts ##################################
#criar os conjunto dois a dois sem repetição

'''
pairs = create_pairs(list_animals)

for pair in pairs:
    run_contacts(pair[0], pair[1])


run_contacts(93, 94)
run_contacts(95, 96)
run_contacts(96, 97)