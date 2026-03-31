import subprocess
import shlex
import sys
import os

def execute_distances_scripts(lista_scripts=None, dataset=None):
    """
    Executa uma lista de scripts Python sequencialmente no ambiente virtual atual.
    """
    if lista_scripts is None and dataset is not None:
        lista_scripts = [
            fr"scripts\DTN\process_distances\calculate_distances_all_animals_all_centroids.py",
            fr"scripts\DTN\process_distances\calculate_distances_animal_to_own_centroid.py",
            fr"scripts\DTN\process_distances\calculate_distances_inside_radius.py",
            fr"scripts\DTN\process_distances\plot_cdf.py --input-dir scripts\Results\{dataset}\distances_all_to_all --output-dir scripts\Results\{dataset}\distances_all_to_all",
            fr"scripts\DTN\process_distances\plot_cdf.py --input-dir scripts\Results\{dataset}\distances_to_own_centroid --output-dir scripts\Results\{dataset}\distances_to_own_centroid",
            fr"scripts\DTN\process_distances\plot_stats_all_to_all.py --input-file scripts\Results\{dataset}\distances_all_to_all\stats_all_to_all.csv --output-dir scripts\Results\{dataset}\distances_all_to_all",
            fr"scripts\DTN\process_distances\plot_stats_to_own_centroid.py --input-file scripts\Results\{dataset}\distances_to_own_centroid\stats_animal_to_own_centroid.csv --output-dir scripts\Results\{dataset}\distances_to_own_centroid",
            fr"scripts\DTN\time_since_last_visit.py --base_dir scripts\Results\{dataset} --methods birch kmeans som --ks 8 16 32 --raio_m 250 --tc_min 120",
            fr"scripts\DTN\plot_time_since_last_visit.py"
        ]

    print(f"Iniciando processamento sequencial no venv: {sys.prefix}")
    print("-" * 50)

    for comando_str in lista_scripts:
        # shlex.split com posix=False mantém as barras \ do Windows intactas
        args = shlex.split(comando_str, posix=False)
        
        # Monta o comando usando o executável python do venv atual
        comando_completo = [sys.executable] + args
        
        script_nome = args[0].split(os.sep)[-1]
        print(f">>> Executando: {script_nome}")
        
        try:
            # check=True faz com que o Python levante uma exceção se o comando falhar
            subprocess.run(comando_completo, capture_output=False, text=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"\n[ERRO] O script '{script_nome}' falhou com código {e.returncode}.")
            print("Interrompendo a sequência de processamento.")
            return False # Retorna Falso indicando que não terminou com sucesso
        except Exception as e:
            print(f"\n[ERRO INESPERADO] {e}")
            return False

    print("-" * 50)
    print("Processamento finalizado com sucesso.")
    return True

# # --- Exemplo de uso ---
# if __name__ == "__main__":
#     sucesso = execute_distances_scripts()
#     if sucesso:
#         print("Pipeline concluído!")
#     else:
#         sys.exit(1)