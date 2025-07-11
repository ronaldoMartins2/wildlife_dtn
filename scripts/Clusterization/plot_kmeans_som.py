import pandas as pd
import matplotlib.pyplot as plt
import sys

# plot_kmeans_som_birch_mean_shift 

# Carregar os dados sem cabeçalho
kmeans_data = pd.read_csv(f"kmeans_coords_{sys.argv[1]}.csv", header=None)
som_data = pd.read_csv(f"som_coords_{sys.argv[1]}.csv", header=None)

# Definir nomes das colunas dinamicamente
if kmeans_data.shape[1] == 2:
    kmeans_data.columns = ['latitude', 'longitude']
elif kmeans_data.shape[1] == 3:
    kmeans_data.columns = ['latitude', 'longitude', 'label']

if som_data.shape[1] == 2:
    som_data.columns = ['latitude', 'longitude']
elif som_data.shape[1] == 3:
    som_data.columns = ['latitude', 'longitude', 'label']

# Criar o gráfico
plt.figure(figsize=(10, 6))

# Plotar os pontos originais
plt.scatter(kmeans_data['longitude'], kmeans_data['latitude'], label='K-Means Clusters', alpha=0.6, cmap='viridis')
plt.scatter(som_data['longitude'], som_data['latitude'], label='SOM Clusters', alpha=0.6, cmap='coolwarm')

plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Clusters e Centróides da Onça")
plt.legend()
plt.grid()