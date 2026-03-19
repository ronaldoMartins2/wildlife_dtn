import subprocess
import shlex
import sys
import os

# Lista de comandos
arquivos = [
    r"scripts\DTN\process_distances\calculate_distances_all_animals_all_centroids.py",
    r"scripts\DTN\process_distances\calculate_distances_animal_to_own_centroid.py",
    r"scripts\DTN\process_distances\calculate_distances_inside_radius.py",
    r"scripts\DTN\process_distances\plot_cdf.py --input-dir scripts\Results\jaguar_mamiraua\distances_all_to_all --output-dir scripts\Results\jaguar_mamiraua\distances_all_to_all",
    r"scripts\DTN\process_distances\plot_cdf.py --input-dir scripts\Results\jaguar_mamiraua\distances_to_own_centroid --output-dir scripts\Results\jaguar_mamiraua\distances_to_own_centroid",
    r"scripts\DTN\process_distances\plot_stats_all_to_all.py --input-file scripts\Results\jaguar_mamiraua\distances_all_to_all\stats_all_to_all.csv --output-dir scripts\Results\jaguar_mamiraua\distances_all_to_all",
    r"scripts\DTN\process_distances\plot_stats_to_own_centroid.py --input-file scripts\Results\jaguar_mamiraua\distances_to_own_centroid\stats_animal_to_own_centroid.csv --output-dir scripts\Results\jaguar_mamiraua\distances_to_own_centroid",
    r"scripts\DTN\time_since_last_visit.py --base_dir scripts\Results\jaguar_mamiraua --methods birch kmeans som --ks 8 16 32 --raio_m 250 --tc_min 120",
    r"scripts\DTN\plot_time_since_last_visit.py"
]

print(f"Iniciando processamento sequencial no venv: {sys.prefix}")

for comando_str in arquivos:
    # shlex.split com posix=False mantém as barras \ do Windows intactas
    args = shlex.split(comando_str, posix=False)
    
    # Monta o comando usando o python do venv atual
    comando_completo = [sys.executable] + args
    
    print(f"--- Executando: {args[0]} ---")
    
    # subprocess.run ESPERA o script acabar antes de ir para o próximo
    resultado = subprocess.run(comando_completo, capture_output=False, text=True)
    
    if resultado.returncode != 0:
        print(f"ERRO ao executar {args[0]}. Interrompendo sequência.")
        break

print("Processamento finalizado.")
