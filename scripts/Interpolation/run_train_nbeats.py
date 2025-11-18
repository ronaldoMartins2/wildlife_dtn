import sys
import os

# garantir que a raiz do projeto está no PYTHONPATH
# __file__ = /home/abinadabe/wild_life/wildlife_dtn/scripts/Interpolation/run_train_nbeats.py
# queremos: /home/abinadabe/wild_life/wildlife_dtn/scripts
script_dir = os.path.dirname(os.path.abspath(__file__))  # scripts/Interpolation
scripts_root = os.path.dirname(script_dir)                # scripts
if scripts_root not in sys.path:
    sys.path.insert(0, scripts_root)

from Interpolation.nbeat_trainer import train_nbeats_model_list
#from Common.utils import load_columns_config

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python run_train_nbeats.py <dataset_raw_filename> <animal_id1> [<animal_id2> ...]")
        sys.exit(1)

    file_rawdata_name = sys.argv[1]           # ex: 'jaguar_mamiraua'
    animal_ids = sys.argv[3:]                 # ex: 93 94

    # Carrega configuração de colunas (usa o mesmo helper que o projeto usa)
    file_rawdata_columns = sys.argv[2]      # ex: 'scripts/Data_preparation/jaguar_mamiraua_columns.json'

    # Chama o treinador (aceita lista de animais)
    train_nbeats_model_list(animal_ids, file_rawdata_name, file_rawdata_columns)