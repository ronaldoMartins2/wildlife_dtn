
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
    remove_nan_data, 
    merge_maps,
    merge_csv
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
    #train_nbeats_model_list
)

from Interpolation.nbeat_interpolation import (
    run as run_interpolation_nbeat
)

from Interpolation.run_nbeats_all import (
    run_pipeline_all as run_pipeline_all_nbeats
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

#time.sleep(2)
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



# 2. RUN FULL N-BEATS PIPELINE (Train -> Eval -> Interpolate)
# This replaces the old separated steps.
run_pipeline_all_nbeats(file_rawdata, file_rawdata_columns)


## LEGACY CALLS COMMENTED OUT FOR SAFETY ##
#train_nbeats_model_single(93, file_rawdata, file_rawdata_columns)
#train_nbeats_model_list(list_animals, file_rawdata, file_rawdata_columns)

#run_interpolation_nbeat(93, number_of_predictions, file_rawdata, file_rawdata_columns)

#start_date, end_date = get_top_botom_date( 93, file_rawdata, file_rawdata_columns )

#run_interpolation_nhits(93, start_date, end_date, file_rawdata, file_rawdata_columns)
'''
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
'''
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

file_interpolated_nbeats = merge_all_interpolations_nbeat(file_rawdata)
file_interpolated_nhits = merge_all_interpolations_nhits(file_rawdata)
file_merged = merge_maps(file_rawdata, list_animals)
file_merged_nbeats = merge_csv(file_merged, file_interpolated_nbeats, file_rawdata, tangara, 'nbeats')
file_merged_nhits = merge_csv(file_merged, file_interpolated_nhits, file_rawdata, tangara, 'nhits')

for current_animal in list_animals:
    map_animal_interpolated_nbeats = os.path.join(results_dir, f'Interpolation/map_{current_animal}_interpolation_nbeats.csv')
    
    # Check if file exists and is not empty before clustering
    if os.path.exists(map_animal_interpolated_nbeats) and os.path.getsize(map_animal_interpolated_nbeats) > 0:
        print(f"Clustering interpolated data for {current_animal}...")
        run_all_kmeans(map_animal_interpolated_nbeats, file_rawdata, f'map_{current_animal}_INTERPOLATED_NBEATS')
        run_birch_all(map_animal_interpolated_nbeats, file_rawdata, f'map_{current_animal}_INTERPOLATED_NBEATS')
        run_som_all(map_animal_interpolated_nbeats, file_rawdata, f'map_{current_animal}_INTERPOLATED_NBEATS')
    else:
        print(f"Skipping clustering for {current_animal}: Interpolation file not found or empty.")


#map_animal_93 = os.path.join(results_dir, f'map_93.csv')
#map_animal_93_interpolated_nhits = os.path.join(results_dir, f'Interpolation/map_93_interpolation_nhits.csv')
#####

run_all_kmeans(file_merged_nbeats, file_rawdata, 'merged_nbeats')
run_birch_all(file_merged_nbeats, file_rawdata, 'merged_nbeats')
run_som_all(file_merged_nbeats, file_rawdata, 'merged_nbeats')
sys.exit()

script_dir = os.path.dirname(os.path.abspath(__file__))
data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
#n_c_BIRCH = read_field_from_json(hyperparam_path, "BIRCH_NCLUSTERS")
n_c_KMEANS = read_field_from_json(hyperparam_path, "n_clusters_kmeans")
n_c_SOM_x = read_field_from_json(hyperparam_path, "som_x")
n_c_SOM_y = read_field_from_json(hyperparam_path, "som_y")

run_all_kmeans(file_merged, file_rawdata, f'RawData_{n_c_KMEANS}_{tangara}')
#run_birch_all(file_merged, file_rawdata, f'RawData_{n_c_BIRCH}_{tangara}')
run_som_all(file_merged, file_rawdata, f'RawData_{n_c_SOM_x * n_c_SOM_y}_{tangara}')
sys.exit()

run_all_kmeans(map_animal_93, file_rawdata, 'map_93RAWDATA')
run_birch_all(map_animal_93, file_rawdata, 'map_93RAWDATA')
run_som_all(map_animal_93, file_rawdata, 'map_93RAWDATA')

run_all_kmeans(map_animal_93_interpolated_nbeats, file_rawdata, 'map_93_INTERPOLATED_NBEATS')
run_birch_all(map_animal_93_interpolated_nbeats, file_rawdata, 'map_93_INTERPOLATED_NBEATS')
run_som_all(map_animal_93_interpolated_nbeats, file_rawdata, 'map_93_INTERPOLATED_NBEATS')
sys.exit()
run_all_kmeans(map_animal_93_interpolated_nhits, file_rawdata, 'map_93_INTERPOLATED_NHITS')
run_birch_all(map_animal_93_interpolated_nhits, file_rawdata, 'map_93_INTERPOLATED_NHITS')
run_som_all(map_animal_93_interpolated_nhits, file_rawdata, 'map_93_INTERPOLATED_NHITS')

sys.exit()

#Chamar as funções abaixo con os dados brutos file_merged
############## RAWDATA CLUSTERIZATION ##############################


############## INTERPOLATED DATA CLUSTERIZATION ##############################
run_all_kmeans(file_interpolated_nbeats, file_rawdata, 'nbeats')
run_all_kmeans(file_interpolated_nhits, file_rawdata, 'nhits')
run_birch_all(file_interpolated_nbeats, file_rawdata, 'nbeats')
run_birch_all(file_interpolated_nhits, file_rawdata, 'nhits')
run_som_all(file_interpolated_nbeats, file_rawdata, 'nbeats')
run_som_all(file_interpolated_nhits, file_rawdata, 'nhits')

#Por enquanto desabilitado
#run_mean_shift_all(file_interpolated_nbeats, 'nbeats')
#run_mean_shift_all(file_interpolated_nhits, 'nhits')

#Roda dispersao geral para o dataset atual
run_all_dispersion(file_rawdata)
#sys.exit()

for current_animal in list_animals:
    run_plot_kmeans_som_birch_mean_shift(current_animal)

#sys.exit()

#for current_animal in list_animals:
#   run_cluster_contacts(current_animal)

#for current_animal in list_animals:
#        run_plot_kmeans_som_birch_mean_shift(current_animal)

run_cluster_contacts(current_animal, file_rawdata)

#for current_animal in list_animals:
#    run_cluster_contacts(current_animal, file_rawdata, tangara)

#run_cluster_contacts(current_animal, file_rawdata)
#sys.exit()

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
