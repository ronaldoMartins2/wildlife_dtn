
import sys
import os
import time

# Environment preparation
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# =================================================================================================
# IMPORTS
# =================================================================================================

# Common Utils
from Common.utils import (
    read_field_from_json,
    get_list_animals,
    merge_csvs,
    create_combinations,
    results_folder,
    merge_all_interpolations_nbeat,
    merge_all_interpolations_nhits,
    merge_all_interpolations_pidl,
    remove_nan_data, 
    merge_maps,
    merge_csv,
    interpolations_methods
)

# Data Preparation
from Data_preparation.separar_localizacoes_individuais import run as run_preparation

# Interpolation
from Interpolation.nhits_trainer import main_training, nhits_main_training_list
from Interpolation.nbeat_trainer import train_nbeats_model_single
from Interpolation.nbeat_interpolation import run as run_interpolation_nbeat
from Interpolation.run_nbeats_all import run_pipeline_all as run_pipeline_all_nbeats
from Interpolation.nhits_interpolation import run as run_interpolation_nhits
from Interpolation.pidl_interpolation import run_pipeline_all_pidl
from Interpolation.train_nbeats_global import train_nbeats_global
from Interpolation.clean_interpolations import run_cleaning_pipeline

# Evaluation
from Evaluation.average_by_individual import (
    run as run_average_by_individual,
    calc_average_by_method,
    get_len_animal,
    get_top_botom_date
)
from Interpolation.evaluate_pidl import run_evaluation_all_pidl
from Evaluation.media_tempos_hist import run as run_media_tempos_hist
from Evaluation.average_comparison import run as run_average_comparison

# Clusterization
from Clusterization.kmeans_individual_csv import run as run_kmeans, run_all as run_all_kmeans
from Clusterization.SOM_individual import run as run_som, run_all as run_som_all
from Clusterization.Mean_Shift import run as run_mean_shift, run_all as run_mean_shift_all
from Clusterization.BIRCH import run as run_birch, run_all as run_birch_all
from Clusterization.plot_kmeans_som_birch_mean_shift import run as run_plot_kmeans_som_birch_mean_shift
from Clusterization.plot_dispersion import run as run_dispersion_plot
from Clusterization.plot_dispersion_geral import run_all_dispersion as run_all_dispersion

# DTN (Mobility / Contacts)
from DTN.mobility_contacts import run as run_contacts
from DTN.cluster_contacts_fixed_points import run as run_cluster_contacts
from DTN.find_contacts_between_nodes import run as run_find_contacts_between_nodes
from DTN.add_down_event import run as run_add_down_event


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    print("=== STARTING WILDLIFE PIPELINE ===")
    
    # 1. ARGS & SETUP
    if len(sys.argv) < 3:
        print("Usage: python3 app_wildlife.py <rawdata.csv> <columns.json>")
        sys.exit(1)

    file_rawdata = sys.argv[1]
    file_rawdata_columns = sys.argv[2]
    
    tangara = file_rawdata.split('.')[-2]
    tangara = tangara.split('/')[-1]

    results_dir = results_folder(file_rawdata)
    hiper_path = os.path.join(results_dir, 'hiperparameters.txt')

    # Delete old hyperparameters file if exists
    if os.path.exists(hiper_path):
        os.remove(hiper_path)
        print(f"Deleted old hyperparameters file: {hiper_path}")

    # Get Animal List
    list_animals = get_list_animals(file_rawdata, file_rawdata_columns)
    len_animals = len(list_animals)
    print(f'Found {len_animals} animals: {list_animals}')

    # 2. DATA PREPARATION & DISPERSION
    print("\n--- Data Preparation & Dispersion ---")
    run_all_dispersion(file_rawdata)

    for current_animal in list_animals:
        run_preparation(current_animal, file_rawdata, file_rawdata_columns)
        run_average_by_individual(current_animal, file_rawdata, file_rawdata_columns)
        run_media_tempos_hist(current_animal, file_rawdata)

    # 3. INTERPOLATION (Training & Serving)
    print("\n--- Interpolation Phase ---")
    
    # Run N-BEATS Pipeline (Train -> Eval -> Interpolate)
    train_nbeats_global(file_rawdata, file_rawdata_columns)
    run_pipeline_all_nbeats(file_rawdata, file_rawdata_columns, run_train=True, run_eval=True, run_predict=True)


    # Run PER-DATASET PIDL Pipeline
    #run_pipeline_all_pidl(file_rawdata, file_rawdata_columns)
    #run_evaluation_all_pidl(file_rawdata, file_rawdata_columns)

    # CLEAN Interpolation Results
    run_cleaning_pipeline(file_rawdata)
    sys.exit(0)
    # Merge Interpolation Results per Animal
    for current_animal in list_animals:
        merge_csvs(current_animal, 'N_BEATS', file_rawdata, file_rawdata_columns)
        merge_csvs(current_animal, 'N_HITS', file_rawdata, file_rawdata_columns)
        merge_csvs(current_animal, 'PIDL', file_rawdata, file_rawdata_columns)

    # 4. DATA MERGING FOR CLUSTERING
    print("\n--- Preparing Data for Clustering ---")
    
    # Create merged map of raw data
    file_merged = merge_maps(file_rawdata, list_animals)
    
    file_interpolated_nbeats = merge_all_interpolations_nbeat(file_rawdata)
    file_interpolated_pidl = merge_all_interpolations_pidl(file_rawdata)

    file_merged_nbeats = None
    file_merged_pidl = None

    file_animal_94 = f'{results_dir}/map_94.csv'
    file_animal_95 = f'{results_dir}/map_95.csv'

    file_interpolated_nbeats_94_merged = f'{results_dir}/Interpolation/map_94_interpolation_nbeats_merged.csv'
    file_interpolated_nbeats_95_merged = f'{results_dir}/Interpolation/map_95_interpolation_nbeats_merged.csv'

    # file_interpolated_nbeats_merged = f'{results_dir}/Interpolation/map_interpolation_merged_93_new.csv'

    # if file_interpolated_nbeats_93: 
    #     print("Running Clustering on 93 Interpolated Data...")

    #     run_all_kmeans(file_interpolated_nbeats_merged,file_rawdata, 'interpolated_93_merged')
    #     run_birch_all(file_interpolated_nbeats_merged, file_rawdata, 'interpolated_93_merged')
    #     run_som_all(file_interpolated_nbeats_merged, file_rawdata, 'interpolated_93_merged')

    if file_animal_94:
        print("Running Clustering on Map 94 Raw Data...")

        run_all_kmeans(file_animal_94,file_rawdata, 'map_94')
        run_birch_all(file_animal_94, file_rawdata, 'map_94')
        run_som_all(file_animal_94, file_rawdata, 'map_94')

    if file_animal_95:
        print("Running Clustering on Map 95 Raw Data...")

        run_all_kmeans(file_animal_95,file_rawdata, 'map_95')
        run_birch_all(file_animal_95, file_rawdata, 'map_95')
        run_som_all(file_animal_95, file_rawdata, 'map_95')

    if file_interpolated_nbeats_94_merged:
        print("Running Clustering on Map 94 Interpolated Data...")

        run_all_kmeans(file_interpolated_nbeats_94_merged,file_rawdata, 'interpolated_merged_94')
        run_birch_all(file_interpolated_nbeats_94_merged, file_rawdata, 'interpolated_merged_94')
        run_som_all(file_interpolated_nbeats_94_merged, file_rawdata, 'interpolated_merged_94')

    if file_interpolated_nbeats_95_merged:
        print("Running Clustering on Map 95 Interpolated Data...")

        run_all_kmeans(file_interpolated_nbeats_95_merged,file_rawdata, 'interpolated_merged_95')
        run_birch_all(file_interpolated_nbeats_95_merged, file_rawdata, 'interpolated_merged_95')
        run_som_all(file_interpolated_nbeats_95_merged, file_rawdata, 'interpolated_merged_95')

    sys.exit(0)

    # 5. CLUSTERING: PART A - INTERPOLATED DATA ONLY
    print("\n--- Clustering Part A: Interpolated Data Only ---")
    
    if file_interpolated_nbeats:
        print(f"Running Clustering on N-BEATS Interpolated Data: {file_interpolated_nbeats}")
        file_merged_nbeats = merge_csv(file_merged, file_interpolated_nbeats, file_rawdata, tangara, 'nbeats')
        
        """Cluster ONLY the interpolated points"""
        #run_all_kmeans(file_interpolated_nbeats, file_rawdata, 'nbeats')
        #run_birch_all(file_interpolated_nbeats, file_rawdata, 'nbeats')
        #run_som_all(file_interpolated_nbeats, file_rawdata, 'nbeats')
    
    if file_interpolated_pidl:
        print(f"Running Clustering on PIDL Interpolated Data: {file_interpolated_pidl}")
        file_merged_pidl = merge_csv(file_merged, file_interpolated_pidl, file_rawdata, tangara, 'pidl')
        
        """Cluster ONLY the interpolated points"""
        #run_all_kmeans(file_interpolated_pidl, file_rawdata, 'pidl')
        #run_birch_all(file_interpolated_pidl, file_rawdata, 'pidl')
        #run_som_all(file_interpolated_pidl, file_rawdata, 'pidl')

    # 6. CLUSTERING: PART B - MERGED DATA (RAW + INTERPOLATED)
    print("\n--- Clustering Part B: Merged Data (Raw + Interpolated) ---")
    
    if file_merged_pidl:
        print("Running Clustering on Merged PIDL Data...")

        """Cluster the merged points"""
        #run_all_kmeans(file_merged_pidl, file_rawdata, 'pidl_merged')
        #run_birch_all(file_merged_pidl, file_rawdata, 'pidl_merged')
        #run_som_all(file_merged_pidl, file_rawdata, 'pidl_merged')
    
    if file_merged_nbeats:
        print("Running Clustering on Merged N-BEATS Data...")

        """Cluster the merged points"""
        #run_all_kmeans(file_merged_nbeats, file_rawdata, 'merged_nbeats')
        #run_birch_all(file_merged_nbeats, file_rawdata, 'merged_nbeats')
        #run_som_all(file_merged_nbeats, file_rawdata, 'merged_nbeats')

    print("\n--- Clustering Part C: Raw Data Only ---")

    if file_merged:
        print("Running Clustering on Merged Raw Data...")

        """Cluster the merged points"""
        run_all_kmeans(file_merged, file_rawdata, 'raw_data')
        run_birch_all(file_merged, file_rawdata, 'raw_data')
        run_som_all(file_merged, file_rawdata, 'raw_data')
        
    print("=== PIPELINE FINISHED SUCCESSFULLY ===")
    sys.exit(0)

    for current_animal in list_animals:
        run_plot_kmeans_som_birch_mean_shift(current_animal)

    #sys.exit()

    #for current_animal in list_animals:
    #   run_cluster_contacts(current_animal)

    #for current_animal in list_animals:
    #        run_plot_kmeans_som_birch_mean_shift(current_animal)

    #run_cluster_contacts(current_animal, file_rawdata)

    #for current_animal in list_animals:
    #    run_cluster_contacts(current_animal, file_rawdata, tangara)

    #run_cluster_contacts(current_animal, file_rawdata)
    #sys.exit()

    ############## #DTN Contacts ##################################
    #criar os conjunto dois a dois sem repetição

    #Combinação sem repetições
    pairs = create_combinations(list_animals)

    for pair in pairs:
        run_contacts(pair[0], pair[1], file_rawdata)

    for pair in pairs:
        run_find_contacts_between_nodes(pair[0], pair[1], file_rawdata)
        run_add_down_event(f'{pair[0]}_{pair[1]}', file_rawdata)
   

if __name__ == "__main__":
    main()


# =================================================================================================
# ======================================= LEGACY / OLD CODE =======================================
# =================================================================================================
"""
# pip3 install -r scripts/requirements.txt 
# python3 -m venv venv
# source ./venv/bin/activate
# inside wildlife_dtn folder
# python3 scripts/Wildlife/app_wildlife.py rawdata/jaguar_mamiraua.csv rawdata/jaguar_columns.json
# python3 scripts/Wildlife/app_wildlife.py rawdata/tangara_mata_atlantica.csv rawdata/tangara_columns.json

# SELECT * FROM jaguar_contacts;

#run_preparation( 93, file_rawdata, file_rawdata_columns )
#run_preparation( 'G54907', file_rawdata, file_rawdata_columns )
#run_average_by_individual( 'G54907', file_rawdata, file_rawdata_columns )

############### call for training models Nbeat and Nhits #########################################################
#nhits_main_training_list(list_animals, file_rawdata, file_rawdata_columns)
#sys.exit()

# for current_animal in list_animals:
#     calc_average_by_method( current_animal, 'N_BEATS', file_rawdata )
#     calc_average_by_method( current_animal, 'N_HITS', file_rawdata )

#run_average_comparison( len_animals, file_rawdata )

# script_dir = os.path.dirname(os.path.abspath(__file__))
# data_prep_dir = os.path.join(script_dir, '..', 'Data_preparation')
# hyperparam_path = os.path.join(data_prep_dir, 'hyperparameters.json')
#n_c_BIRCH = read_field_from_json(hyperparam_path, "BIRCH_NCLUSTERS")
#n_c_KMEANS = read_field_from_json(hyperparam_path, "n_clusters_kmeans")
#n_c_SOM_x = read_field_from_json(hyperparam_path, "som_x")
#n_c_SOM_y = read_field_from_json(hyperparam_path, "som_y")

#run_all_kmeans(file_merged, file_rawdata, f'RawData_{n_c_KMEANS}_{tangara}')
#run_birch_all(file_merged, file_rawdata, f'RawData_{n_c_BIRCH}_{tangara}')
#run_som_all(file_merged, file_rawdata, f'RawData_{n_c_SOM_x * n_c_SOM_y}_{tangara}')

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

############## #DTN Contacts ##################################
#criar os conjunto dois a dois sem repetição

#Combinação sem repetições
pairs = create_combinations(list_animals)

for pair in pairs:
    run_contacts(pair[0], pair[1], file_rawdata)

for pair in pairs:
    run_find_contacts_between_nodes(pair[0], pair[1], file_rawdata)
    run_add_down_event(f'{pair[0]}_{pair[1]}', file_rawdata)
"""
