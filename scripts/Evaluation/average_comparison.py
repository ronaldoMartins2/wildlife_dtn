import matplotlib.pyplot as plt
import numpy as np
import os

from Evaluation.average_by_individual import (
    get_average_by_animal_sorted,
    get_average_nbeats_by_animal_sorted,
    get_average_nhits_by_animal_sorted,
    get_id_animal_sorted
)

from Common.utils import (
    results_folder
)

def run( len_animals, file_rawdata ):

    #raw_means = [11.9, 8.84, 100.4, 7.41, 31.26, 10.2, 2.78, 111.22]
    raw_means = get_average_by_animal_sorted( file_rawdata )

    # TODO for some reasons is saved duplicated so I put a len_animals to limit 
    raw_means = raw_means[:len_animals]

    labels = []
    animal_sorted = get_id_animal_sorted( file_rawdata )
    
    print(f'animal_sorted +++++++++++++++++++++++ {animal_sorted} len_animals {len_animals}')

    for current in animal_sorted[:len_animals]:

        # Dados fornecidos
        #labels = ['Onça 93', 'Onça 94', 'Onça 95', 'Onça 96', 'Onça 97', 'Onça 98', 'Onça 99', 'Onça 100']
            
        # Dados fornecidos
        labels.append( f'Animal {current}' )

    print(f'array of measures len: {len(raw_means)}')
    print(get_average_by_animal_sorted( file_rawdata ))

    nbeats_means = get_average_nbeats_by_animal_sorted( file_rawdata )

    nbeats_means = nbeats_means[:len_animals]

    nbeats_merged_means = get_average_nhits_by_animal_sorted( file_rawdata )

    nbeats_merged_means = nbeats_merged_means[:len_animals]

    print(f'nbeats_merged_means -------------- {nbeats_merged_means}')

    x = np.arange(len(labels))  # Posições das barras no eixo Y
    width = 0.25  # Largura das barras

    # Ajustando o gráfico para exibir as barras na vertical
    fig, ax = plt.subplots(figsize=(12, 6))

    bars1 = ax.barh(x - width, raw_means, width, label='Dados Brutos', align='center')
    bars2 = ax.barh(x, nbeats_means, width, label='NBEATS', align='center')
    bars3 = ax.barh(x + width, nbeats_merged_means, width, label='NHITS', align='center')

    # Adicionar títulos e rótulos
    ax.set_ylabel('Animais')
    ax.set_xlabel('Média de Tempo entre Coletas')
    ax.set_title(f'Comparação das Médias de Tempo entre Coletas - Dataset: {file_rawdata}')
    ax.set_yticks(x)
    ax.set_yticklabels(labels)
    ax.legend()

    # Mostrar valores nas barras
    for bars in [bars1, bars2, bars3]:
        for bar in bars:
            width = bar.get_width()
            ax.annotate(f'{width:.2f}',
                        xy=(width, bar.get_y() + bar.get_height() / 2),
                        xytext=(3, 0),  # Deslocamento do texto para o lado
                        textcoords="offset points",
                        ha='left', va='center',
                        fontsize=9)

    plt.tight_layout()

    #script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    #results_dir = os.path.join(script_dir, '..', 'Results/Interpolation')  # Navigate to the parent directory and into 'Results'

    results_dir = results_folder( file_rawdata )

    file_path = os.path.join(results_dir, f'Interpolation/average_comparison.png')

    # Save the plot as an image
    plt.savefig( file_path )

