import pandas as pd
import matplotlib.pyplot as plt

# Caminhos dos arquivos
file_paths = [
    ("Método 1", "Contactos_94_birch.csv"),
    ("Método 2", "Contactos_94_kmeans.csv"),
    ("Método 3", "Contactos_94_som.csv"),
    ("Método 4", "Contactos_94_meanshift.csv"),
]

# Contar o número de registros em cada arquivo
methods = []
contact_counts = []

for method, path in file_paths:
    df = pd.read_csv(path, header=None)
    methods.append(method)
    contact_counts.append(len(df))

# Criar o gráfico de barras
plt.figure(figsize=(8, 5))
plt.bar(methods, contact_counts, color=['blue', 'green', 'red','yellow'])
plt.xlabel("Método")
plt.ylabel("Total de Contatos")
plt.title("Comparação do Total de Contatos por Método")
plt.grid(axis="y", linestyle="--", alpha=0.7)

# Exibir os valores em cima das barras
for i, count in enumerate(contact_counts):
    plt.text(i, count + 1, str(count), ha="center", fontsize=12)
