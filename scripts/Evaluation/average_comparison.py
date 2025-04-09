import matplotlib.pyplot as plt
import numpy as np
import os

from Evaluation.average_by_individual import (
    get_average_by_animal_sorted,
    get_average_nbeats_by_animal_sorted,
    get_average_nhits_by_animal_sorted,
    get_id_animal_sorted
)

def run( len_animals ):

    #raw_means = [11.9, 8.84, 100.4, 7.41, 31.26, 10.2, 2.78, 111.22]
    raw_means = get_average_by_animal_sorted( )

    # TODO for some reasons is saved duplicated so I put a len_animals to limit 
    raw_means = raw_means[:len_animals]

    labels = []
    animal_sorted = get_id_animal_sorted()
    
    for current in animal_sorted[:len_animals]:

        # Dados fornecidos
        #labels = ['Onça 93', 'Onça 94', 'Onça 95', 'Onça 96', 'Onça 97', 'Onça 98', 'Onça 99', 'Onça 100']
            
        # Dados fornecidos
        labels.append( f'Onça {current}' )

    print(f'array of measures len: {len(raw_means)}')
    print(get_average_by_animal_sorted())

    #nbeats_means = [0.13, 5.37, 12.48, 0.73, 18.69, 10.89, 2.73, 7.06]
    nbeats_means = get_average_nbeats_by_animal_sorted()

    nbeats_means = nbeats_means[:len_animals]

    #nbeats_merged_means = [0.13,4.24,11.1,0.67,11.7,5.27,1.38,6.64]

    nbeats_merged_means = get_average_nhits_by_animal_sorted()

    nbeats_merged_means = nbeats_merged_means[:len_animals]

    x = np.arange(len(labels))  # Posições das barras no eixo Y
    width = 0.25  # Largura das barras

    # Ajustando o gráfico para exibir as barras na vertical
    fig, ax = plt.subplots(figsize=(12, 6))

    #bars1 = ax.barh(x - width, raw_means, width, label='Dados Brutos', align='center')
    #bars2 = ax.barh(x, nbeats_means, width, label='Pós NBEATS', align='center')
    #bars3 = ax.barh(x + width, nbeats_merged_means, width, label='Pós merged NBEATS', align='center')

    bars1 = ax.barh(x - width, raw_means, width, label='Dados Brutos', align='center')
    bars2 = ax.barh(x, nbeats_means, width, label='NBEATS', align='center')
    bars3 = ax.barh(x + width, nbeats_merged_means, width, label='NHITS', align='center')

    # Adicionar títulos e rótulos
    ax.set_ylabel('Onças')
    ax.set_xlabel('Média de Tempo entre Coletas')
    ax.set_title('Comparação das Médias de Tempo entre Coletas')
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
    #plt.show()

    script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script directory
    results_dir = os.path.join(script_dir, '..', 'Results')  # Navigate to the parent directory and into 'Results'
    file_path = os.path.join(results_dir, f'average_comparison.png')

    # Save the plot as an image
    plt.savefig( file_path )

