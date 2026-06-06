
import sys
import os
import time

# Environment preparation
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# pip3 install -r scripts/requirements.txt 

# Primeiro passo
# python3 -m venv venv

# Segundo passo
# source ./venv/bin/activate

# to run

# Terceiro passo
# pip3 install -r scripts/requirements.txt

# ========== to run ==========

# Terceiro passo
# pip3 install -r scripts/requirements.txt

# Quarto passo
# inside wildlife_dtn folder
# python scripts/Wildlife/app_wildlife.py rawdata/jaguar_mamiraua.csv rawdata/jaguar_columns.json
# python scripts/Wildlife/app_wildlife.py rawdata/tangara_mata_atlantica.csv rawdata/tangara_columns.json

# python scripts\DTN\distancias_uakari.py scripts\Results\jaguar_mamiraua\map_jaguar_mamiraua_all_animals.csv

# SELECT * FROM jaguar_contacts;

# Salvar a tabela em formato Latex para Journal
# Olhar o item 4.1 do artigo

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
    merge_all_interpolations_bilstm,
    remove_nan_data, 
    merge_maps,
    merge_csv,
    interpolations_methods,
    return_maps,
    return_bilstm_list,
    calculate_average_metrics,
    plot_interpolation_comparisons
)

# Data Preparation
from Data_preparation.separar_localizacoes_individuais import run as run_preparation

# Interpolation
from Interpolation.nhits_trainer import main_training, nhits_main_training_list
from Interpolation.nbeat_trainer import train_nbeats_model_single
from Interpolation.nbeat_interpolation import run as run_interpolation_nbeat
from Interpolation.run_nbeats_all import run_pipeline_all as run_pipeline_all_nbeats
from Interpolation.nhits_interpolation import run as run_interpolation_nhits
from Interpolation.bilstm_interpolation import run_pipeline_all_bilstm
from Interpolation.train_nbeats_global import train_nbeats_global
from Interpolation.clean_interpolations import run_cleaning_pipeline

# Evaluation
from Evaluation.average_by_individual import (
    run as run_average_by_individual,
    calc_average_by_method,
    get_len_animal,
    get_top_botom_date
)
from Interpolation.evaluate_bilstm import run_evaluation_all_bilstm
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

from DTN.export_final_contacts import run as run_export_final_trace
from DTN.find_contacs_animal_to_centroids import run as run_contacts_animal_centroids
from DTN.add_uakari_lodge_contact import run as run_contacts_animal_uakari
from DTN.export_all_final_contacts import run as run_export_all_final_trace
from DTN.setup_database import recreate_table as run_recreate_table
import DTN.generate_all_distances_data_n_plots as run_generate_distances_n_data_n_plots


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

    # for current_animal in list_animals:
    #     run_preparation(current_animal, file_rawdata, file_rawdata_columns)
    #     run_average_by_individual(current_animal, file_rawdata, file_rawdata_columns)
    #     run_media_tempos_hist(current_animal, file_rawdata)

    # 3. INTERPOLATION (Training & Serving)
    print("\n--- Interpolation Phase ---")
    
    # Run N-BEATS Pipeline (Train -> Eval -> Interpolate)
    # train_nbeats_global(file_rawdata, file_rawdata_columns)
    # run_pipeline_all_nbeats(file_rawdata, file_rawdata_columns, run_train=True, run_eval=True, run_predict=True)
   
    # Run PER-DATASET BiLSTM Pipeline
    # run_pipeline_all_bilstm(file_rawdata, file_rawdata_columns)
    # run_evaluation_all_bilstm(file_rawdata, file_rawdata_columns)

    # CLEAN Interpolation Results
    # run_cleaning_pipeline(file_rawdata)

    # Merge Interpolation Results per Animal
    for current_animal in list_animals:
        merge_csvs(current_animal, 'N_BEATS', file_rawdata, file_rawdata_columns)
        #merge_csvs(current_animal, 'N_HITS', file_rawdata, file_rawdata_columns)
        merge_csvs(current_animal, 'BiLSTM', file_rawdata, file_rawdata_columns)

    calculate_average_metrics(file_rawdata, "bilstm", list_animals)
    calculate_average_metrics(file_rawdata, "nbeats", list_animals)
    
    # 3.1. PLOT INTERPOLATION METRICS COMPARISON
    print("\n--- Gerando Gráficos de Comparação de Interpolação ---")
    plot_interpolation_comparisons(file_rawdata, ["bilstm", "nbeats"])

    # sys.exit(0)
    # 4. DATA MERGING FOR CLUSTERING
    print("\n--- Preparing Data for Clustering ---")
    
    # Create merged map of raw data
    file_merged = merge_maps(file_rawdata, list_animals)
    file_interpolated_nbeats = merge_all_interpolations_nbeat(file_rawdata)
    file_interpolated_bilstm = merge_all_interpolations_bilstm(file_rawdata)
    file_merged_nbeats = None
    file_merged_bilstm = None

    # Apenas dados brutos
    # if file_merged:
    #     print(f"Merged raw data file created: {file_merged}")
    #     run_all_kmeans(file_merged, file_rawdata, 'Raw data')
    #     run_birch_all(file_merged, file_rawdata, 'Raw data')
    #     run_som_all(file_merged, file_rawdata, 'Raw data')

    clusters = [8, 16, 32]
    
    all_maps_animals = return_maps(file_rawdata, list_animals)
    if all_maps_animals:
        print("\n--- Clusterizando todos os mapas individuais detectados ---")
        for map_name in all_maps_animals:
            map_path = os.path.join(results_dir, map_name)
            animal_id = map_name.replace('map_','').replace('.csv','')
            for n_clusters in clusters:
                run_all_kmeans(map_path, file_rawdata, animal_id, n_clusters)
                run_birch_all(map_path, file_rawdata, animal_id, n_clusters)
                run_som_all(map_path, file_rawdata, animal_id, n_clusters)

    # sys.exit(0)

    all_maps_bilstm = return_bilstm_list(file_rawdata, list_animals)
    if all_maps_bilstm:
        print("\n--- Clusterizando todos os mapas Bi-LSTM detectados ---")
        for map_name in all_maps_bilstm:
            map_path = os.path.join(results_dir, "Interpolation", map_name)
            animal_id = map_name.replace('map_bilstm_','').replace('.csv','')
            for n_clusters in clusters:
                run_all_kmeans(map_path, file_rawdata, f'{animal_id}_bilstm', n_clusters)
                run_birch_all(map_path, file_rawdata, f'{animal_id}_bilstm', n_clusters)
                run_som_all(map_path, file_rawdata, f'{animal_id}_bilstm', n_clusters)

    # print("\n--- Clustering Part A: Interpolated Data Only ---")
    # Only merge if interpolation files exist
    if file_interpolated_bilstm and file_merged:
        file_merged_bilstm = merge_csv(file_merged, file_interpolated_bilstm, file_rawdata, tangara, 'bilstm')
    
    if file_interpolated_nbeats and file_merged:
        file_merged_nbeats = merge_csv(file_merged, file_interpolated_nbeats, file_rawdata, tangara, 'nbeats')

    if file_merged_bilstm:
        print("Running Clustering on Merged BiLSTM Data...")

        """Cluster the merged points"""
        for n_clusters in clusters:
            run_all_kmeans(file_merged_bilstm, file_rawdata, 'bilstm_merged', n_clusters)
            run_birch_all(file_merged_bilstm, file_rawdata, 'bilstm_merged', n_clusters)
            run_som_all(file_merged_bilstm, file_rawdata, 'bilstm_merged', n_clusters)


    if file_merged_nbeats:
        print("Running Clustering on Merged N-BEATS Data...")

        """Cluster the merged points"""
        for n_clusters in clusters:
            run_all_kmeans(file_merged_nbeats, file_rawdata, 'nbeats_merged', n_clusters)   
            run_birch_all(file_merged_nbeats, file_rawdata, 'nbeats_merged', n_clusters)
            run_som_all(file_merged_nbeats, file_rawdata, 'nbeats_merged', n_clusters)

    if file_interpolated_nbeats:
        print(f"Running Clustering on N-BEATS Interpolated Data: {file_interpolated_nbeats}")
        file_merged_nbeats = merge_csv(file_merged, file_interpolated_nbeats, file_rawdata, tangara, 'nbeats')
        
        """Cluster ONLY the interpolated points"""
        for n_clusters in clusters:
            run_all_kmeans(file_interpolated_nbeats, file_rawdata, 'nbeats', n_clusters)
            run_birch_all(file_interpolated_nbeats, file_rawdata, 'nbeats', n_clusters)
            run_som_all(file_interpolated_nbeats, file_rawdata, 'nbeats', n_clusters)

    # 5. CLUSTERING: PART A - INTERPOLATED DATA ONLY
    
    if file_interpolated_bilstm:
        print(f"Running Clustering on BiLSTM Interpolated Data: {file_interpolated_bilstm}")
        file_merged_bilstm = merge_csv(file_merged, file_interpolated_bilstm, file_rawdata, tangara, 'bilstm')
        
        """Cluster ONLY the interpolated points"""
        for n_clusters in clusters:
            run_all_kmeans(file_interpolated_bilstm, file_rawdata, 'bilstm', n_clusters)
            run_birch_all(file_interpolated_bilstm, file_rawdata, 'bilstm', n_clusters)
            run_som_all(file_interpolated_bilstm, file_rawdata, 'bilstm', n_clusters)

    # 6. CLUSTERING: PART B - MERGED DATA (RAW + INTERPOLATED)
    print("\n--- Clustering Part B: Merged Data (Raw + Interpolated) ---")
    
    if file_merged_bilstm:
        print("Running Clustering on Merged BiLSTM Data...")

        """Cluster the merged points"""
        for n_clusters in clusters:
            run_all_kmeans(file_merged_bilstm, file_rawdata, 'bilstm_merged', n_clusters)
            run_birch_all(file_merged_bilstm, file_rawdata, 'bilstm_merged', n_clusters)
            run_som_all(file_merged_bilstm, file_rawdata, 'bilstm_merged', n_clusters)
    
    if file_merged_nbeats:
        print("Running Clustering on Merged N-BEATS Data...")

        """Cluster the merged points"""
        for n_clusters in clusters:
            run_all_kmeans(file_merged_nbeats, file_rawdata, 'nbeats_merged', n_clusters)   
            run_birch_all(file_merged_nbeats, file_rawdata, 'nbeats_merged', n_clusters)
            run_som_all(file_merged_nbeats, file_rawdata, 'nbeats_merged', n_clusters)

    print("\n--- Clustering Part C: Raw Data Only ---")

    if file_merged:
        print("Running Clustering on Merged Raw Data...")

        """Cluster the merged points"""
        for n_clusters in clusters:
            run_all_kmeans(file_merged, file_rawdata, 'raw_data', n_clusters)
            run_birch_all(file_merged, file_rawdata, 'raw_data', n_clusters)
            run_som_all(file_merged, file_rawdata, 'raw_data', n_clusters)
        
    print("=== PIPELINE FINISHED SUCCESSFULLY ===")
    # sys.exit(0)

    ########################################################################################
    # Para a criação de dados de interpolação, é necessário passar os dados de raw_data
    # Para a criação de dados clusterizados + interpolados, é necessário passar os dados de raw_data + interpolated
    # Para a criação de dados de contatos, é necessário passar os dados de raw_data + interpolated + ou  clusterizados

    ############## #DTN Contacts ##################################
    #criar os conjunto dois a dois sem repetição

    #Combinação sem repetições
    pairs = create_combinations(list_animals)
    print(pairs)

    # sys.exit(0)

    # Chamar todos os scripts de criação de dados de distancias e plots
    # import subprocess
    # subprocess.run([r"venv\Scripts\python.exe", r"scripts\DTN\generate_all_distances_data_n_plots.py"])

    # Limpar o database para gerar novamente os contatos
    run_recreate_table()

    file_rawdata = file_merged_bilstm

    # for pair in pairs:
    #     run_contacts(pair[0], pair[1], file_rawdata)

    if file_merged_bilstm:
        for pair in pairs:
            run_find_contacts_between_nodes(pair[0], pair[1], file_interpolated_bilstm, file_rawdata, 'bilstm', n_clusters)
            run_add_down_event(pair[0], pair[1], file_interpolated_bilstm, file_rawdata, 'bilstm', n_clusters)

    for pair in pairs:
        run_find_contacts_between_nodes(pair[0], pair[1], file_rawdata)
        run_add_down_event(f'{pair[0]}_{pair[1]}', file_rawdata)

    sys.exit(0)

    # Fora do loop dos pares de animais
    print("Gerando arquivo final consolidado...")
    run_export_final_trace(file_rawdata)

    sys.exit(0)

    # No app_wildlife.py, após processar os mapas individuais
    list_animals = ['93', '94', '95', '96', '97', '98', '99', '100']

    # Criar contatos entre onças e centroids
    for animal_id in list_animals:
        # Agora passamos o ID numérico (ex: '93') e não o nome do arquivo bruto
        run_contacts_animal_centroids(animal_id, file_rawdata)

    # Criar contatos entre onças e o Uakari Lodge
    for animal_id in list_animals:
        run_contacts_animal_uakari(animal_id, file_rawdata)

    raw_name = "map_jaguar_mamiraua_all_animals_bilstm"
        
    # Listas para o loop de experimentos
    centroids_list = [16]
    algorithms_list = ["kmeans"] # Seus 3 algoritmos
    interpolations_list = ["bilstm"] # Neste primeiro momento apenas o rawdata

    # Gerar arquivos para cada combinação
    for n in centroids_list:
        for alg in algorithms_list:
            for interp in interpolations_list:
                run_export_all_final_trace(raw_name, n_centroids=n, algorithm=alg, interpolation=interp)
   

if __name__ == "__main__":
    main()