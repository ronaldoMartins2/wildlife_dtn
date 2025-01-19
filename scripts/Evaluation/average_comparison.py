import matplotlib.pyplot as plt
import numpy as np

# Dados fornecidos
labels = ['Onça 93', 'Onça 94', 'Onça 95', 'Onça 96', 'Onça 97', 'Onça 98', 'Onça 99', 'Onça 100']
raw_means = [11.9, 8.84, 100.4, 7.41, 31.26, 10.2, 2.78, 111.22]
nbeats_means = [0.13, 5.37, 12.48, 0.73, 18.69, 10.89, 2.73, 7.06]
nbeats_merged_means = [0.13,4.24,11.1,0.67,11.7,5.27,1.38,6.64]

x = np.arange(len(labels))  # Posições das barras no eixo Y
width = 0.25  # Largura das barras

# Ajustando o gráfico para exibir as barras na vertical
fig, ax = plt.subplots(figsize=(12, 6))

bars1 = ax.barh(x - width, raw_means, width, label='Dados Brutos', align='center')
bars2 = ax.barh(x, nbeats_means, width, label='Pós NBEATS', align='center')
bars3 = ax.barh(x + width, nbeats_merged_means, width, label='Pós merged NBEATS', align='center')

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
plt.show()

# Save the plot as an image
plt.savefig(f'average_comparison.png')

