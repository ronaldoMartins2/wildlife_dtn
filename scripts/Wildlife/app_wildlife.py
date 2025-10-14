
import sys
import os
import time

# environment preparation

# pip3 install -r scripts/requirements.txt 

# python3 -m venv venv

# Segundo passo
# source ./venv/bin/activate

# pip3 install -r scripts/requirements.txt 

# ========== to run ==========

# Terceiro passo
# pip3 install -r scripts/requirements.txt

# Quarto passo
# inside wildlife_dtn folder
# python3 scripts/Wildlife/app_wildlife.py rawdata/jaguar_mamiraua.csv rawdata/jaguar_columns.json
# python3 scripts/Wildlife/app_wildlife.py rawdata/tangara_mata_atlantica.csv rawdata/tangara_columns.json

# SELECT * FROM jaguar_contacts;

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from Common.utils import (
    read_field_from_json,
    get_list_animals,
    merge_csvs,
    create_combinations,
    results_folder,
    merge_all_interpolations_nbeat,
    merge_all_interpolations_nhits,
    remove_nan_data
    )

from Data_preparation.separar_localizacoes_individuais import (
    run as run_preparation
)

from Interpolation.nhits_trainer import (
    main_training,
    nhits_main_training_list
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

from Evaluation.media_tempos_hist import (
    run as run_media_tempos_hist
)

from Evaluation.average_comparison import (
    run as run_average_comparison
)

from Common.utils import (
    interpolations_methods
)

from Clusterization.kmeans_individual_csv import (
    run as run_kmeans,
    run_all as run_all_kmeans
)

from Clusterization.SOM_individual import (
    run as run_som,
    run_all as run_som_all
)

from Clusterization.Mean_Shift import (
    run as run_mean_shift,
    run_all as run_mean_shift_all
)

from Clusterization.BIRCH import (
    run as run_birch,
    run_all as run_birch_all
)

from Clusterization.plot_kmeans_som_birch_mean_shift import (
    run as run_plot_kmeans_som_birch_mean_shift
)

from Clusterization.plot_dispersion import (
    run as run_dispersion_plot
)

from Clusterization.plot_dispersion_geral import (
    run_all_dispersion as run_all_dispersion
)

from DTN.mobility_contacts import (
    run as run_contacts
)

from DTN.cluster_contacts_fixed_points import (
    run as run_cluster_contacts
)

from DTN.find_contacts_between_nodes import (
    run as run_find_contacts_between_nodes
)

from DTN.add_down_event import (
    run as run_add_down_event
)

# ========== data preparation ==========

file_rawdata = sys.argv [1]
file_rawdata_columns = sys.argv [2]

results_dir = results_folder(file_rawdata)
hiper_path = os.path.join(results_dir, 'hiperparameters.txt')

# Check if the file exists before trying to delete it
# if os.path.exists(hiper_path):
#     os.remove(hiper_path)
#     print(f"Deleted: {hiper_path}")
# else:
#     print(f"No file found at: {hiper_path}")

list_animals = get_list_animals( file_rawdata, file_rawdata_columns )

len_animals = len(list_animals)

print(f' list_animals { len_animals }')

print(f'{list_animals}')


for current_animal in list_animals:
    run_preparation( current_animal, file_rawdata, file_rawdata_columns )
    run_average_by_individual( current_animal, file_rawdata, file_rawdata_columns )
    run_media_tempos_hist( current_animal, file_rawdata)

time.sleep(2)
# sys.exit()

#run_preparation( 93, file_rawdata, file_rawdata_columns )
#run_preparation( 'G54907', file_rawdata, file_rawdata_columns )
#run_average_by_individual( 'G54907', file_rawdata, file_rawdata_columns )


############## INTERPOLATION ##############################

tangara = file_rawdata.split('.')[-2]
tangara = tangara.split('/')[-1]

#if tangara == 'tangara_mata_atlantica':
    #list_animals = ['E62718', 'E62726', 'OR34MGA', 'G21547', 'E62705', 'E57527', 'E49920', 'E62722', 'G56076' ]
#    list_animals = ['OR34MGA' ]
#else:
#    list_animals = [94]

# E62724 loop
# G56068 empty


# TRAIN step

#for current_animal in list_animals:
#    trainer = main_training(current_animal, file_rawdata, file_rawdata_columns)

################ call for training models Nbeat and Nhits #########################################################

#nhits_main_training_list(list_animals, file_rawdata, file_rawdata_columns)
#sys.exit()

#train_nbeats_model_single(current_animal, file_rawdata, file_rawdata_columns)
#train_nbeats_model_list(list_animals, file_rawdata, file_rawdata_columns)
#sys.exit()
####################################################################################################################

for current_animal in list_animals:
     #TODO review number_of_predictions 
     number_of_predictions = 5
     len_animal = get_len_animal( current_animal, file_rawdata )
   
     print(f'len_animal {len_animal} current_animal {current_animal} file_rawdata {file_rawdata}')

     run_interpolation_nbeat(current_animal, number_of_predictions, file_rawdata, file_rawdata_columns)

     start_date, end_date = get_top_botom_date( current_animal, file_rawdata, file_rawdata_columns )

     run_interpolation_nhits(current_animal, start_date, end_date, file_rawdata, file_rawdata_columns)
#exit ()
#sys.exit()

for current_animal in list_animals:
    merge_csvs( current_animal, 'N_BEATS', file_rawdata, file_rawdata_columns )
    merge_csvs( current_animal, 'N_HITS', file_rawdata, file_rawdata_columns )

#exit()


# for current_animal in list_animals:

#     calc_average_by_method( current_animal, 'N_BEATS', file_rawdata )
#     calc_average_by_method( current_animal, 'N_HITS', file_rawdata )

#run_average_comparison( len_animals, file_rawdata )

#sys.exit()

############## CLUSTERIZATION ##############################
# run clusterization kmeans

#Rodando Dispersao Geral dos animais: Tangara e Jaguar
#Criando csv das coordenadas interpoladas
merge_all_interpolations_nbeat(file_rawdata)
merge_all_interpolations_nhits(file_rawdata)

file_interpolated_nbeats = os.path.join( results_folder(file_rawdata), 'Interpolation', f'map_{tangara}_interpolation_nbeats_all.csv' )
file_interpolated_nhits = os.path.join( results_folder(file_rawdata), 'Interpolation', f'map_{tangara}_interpolation_nhits_all.csv' )

#Roda kmeans para todos os animais
run_all_kmeans(file_interpolated_nbeats, 'nbeats')
run_all_kmeans(file_interpolated_nhits, 'nhits')
run_birch_all(file_interpolated_nbeats, 'nbeats')
run_birch_all(file_interpolated_nhits, 'nhits')
run_som_all(file_interpolated_nbeats, 'nbeats')
run_som_all(file_interpolated_nhits, 'nhits')

#Por enquanto desabilitado
#run_mean_shift_all(file_interpolated_nbeats, 'nbeats')
#run_mean_shift_all(file_interpolated_nhits, 'nhits')

#Roda dispersao geral para todos os animais Raw data
run_all_dispersion()
sys.exit()

for current_animal in list_animals:
    run_plot_kmeans_som_birch_mean_shift(current_animal)

sys.exit()

#for current_animal in list_animals:
#   run_cluster_contacts(current_animal)

#for current_animal in list_animals:
#        run_plot_kmeans_som_birch_mean_shift(current_animal)


for current_animal in list_animals:
       run_cluster_contacts(current_animal)

run_cluster_contacts(current_animal, file_rawdata)
sys.exit()

'''
############## #DTN Contacts ##################################
#criar os conjunto dois a dois sem repetição

'''
#Combinação sem repetições
pairs = create_combinations(list_animals)

for pair in pairs:
    run_contacts(pair[0], pair[1], file_rawdata)

for pair in pairs:
    run_find_contacts_between_nodes(pair[0], pair[1], file_rawdata)
    run_add_down_event(f'{pair[0]}_{pair[1]}', file_rawdata)
