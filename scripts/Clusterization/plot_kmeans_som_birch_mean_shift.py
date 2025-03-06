import pandas as pd
import matplotlib.pyplot as plt
import sys

# plot_kmeans_som_birch_mean_shift 

# python3 -m venv venv
# source ./venv/bin/activate

# python3 plot_kmeans_som_birch_mean_shift.py 94 

current_animal = sys.argv[1]

# Carregar os dados sem cabeçalho
kmeans_data = pd.read_csv(f"kmeans_coords_{current_animal}.csv", header=None)
som_data = pd.read_csv(f"som_coords_{current_animal}.csv", header=None)
birch = pd.read_csv(f"birch_clusters_map_{current_animal}.csv", header=None)
mean_shift = pd.read_csv(f"birch_clusters_map_{current_animal}.csv", header=None)

# Definir nomes das colunas dinamicamente
if kmeans_data.shape[1] == 2:
    kmeans_data.columns = ['latitude', 'longitude']
elif kmeans_data.shape[1] == 3:
    kmeans_data.columns = ['latitude', 'longitude', 'label']

if som_data.shape[1] == 2:
    som_data.columns = ['latitude', 'longitude']
elif som_data.shape[1] == 3:
    som_data.columns = ['latitude', 'longitude', 'label']

if birch.shape[1] == 2:
    birch.columns = ['latitude', 'longitude']
elif birch.shape[1] == 3:
    birch.columns = ['latitude', 'longitude', 'label']

if mean_shift.shape[1] == 2:
    mean_shift.columns = ['latitude', 'longitude']
elif mean_shift.shape[1] == 3:
    mean_shift.columns = ['latitude', 'longitude', 'label']

# Criar o gráfico
plt.figure(figsize=(10, 6))

# Plotar os pontos originais
plt.scatter(kmeans_data['longitude'], kmeans_data['latitude'], label='K-Means Clusters', alpha=0.6, cmap='viridis')
plt.scatter(som_data['longitude'], som_data['latitude'], label='SOM Clusters', alpha=0.6, cmap='coolwarm')
plt.scatter(birch['longitude'], birch['latitude'], label='BIRCH Clusters', alpha=0.6, cmap='plasma')
plt.scatter(mean_shift['longitude'], mean_shift['latitude'], label='MEAN-SHIFT Clusters', alpha=0.6, cmap='cividis')

plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Clusters e Centróides da Onça")
plt.legend()
plt.grid()
#plt.show()


# Save the plot as an image
plt.savefig(f'onca_{current_animal}_clusterization_comparizon.png')