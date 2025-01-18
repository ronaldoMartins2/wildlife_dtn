import pandas as pd
from datetime import datetime
import sys

# Verifica os argumentos de entrada
if len(sys.argv) < 2:
    print("Uso: python script.py <source_file>")
    sys.exit(1)

source = sys.argv[1]

# Lê o arquivo CSV
try:
    df = pd.read_csv(f'{source}.csv', header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
except FileNotFoundError:
    print(f"Arquivo {source}.csv não encontrado.")
    sys.exit(1)

# Verifica se o DataFrame está vazio
if df.empty:
    print("O arquivo CSV está vazio.")
    sys.exit(1)

# Converte a coluna 'Timestamp' para datetime
try:
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%m/%d/%y %H:%M')
except ValueError as e:
    print(f"Erro ao converter Timestamp: {e}")
    sys.exit(1)

# Remove duplicatas de timestamps
df = df.drop_duplicates(subset='Timestamp')

# Verifica se há pelo menos 2 registros para calcular a diferença
if len(df) < 2:
    print("O arquivo contém menos de 2 registros. Não é possível calcular a média.")
    sys.exit(1)

# Ordena os timestamps
df = df.sort_values(by='Timestamp')

# Calcula as diferenças de tempo
df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600

# Agrega por hora se necessário (opcional para alta frequência)
#aggregate = input("\nDeseja agregar os dados por hora? (s/n): ").strip().lower()
#if aggregate == 's':
#    df = df.resample('1H', on='Timestamp').first().dropna().reset_index()
#    df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600

# Calcula a média das diferenças de tempo
average_time_difference = df['Time Difference (hours)'].mean()

print(f"Average time difference (in hours) between consecutive timestamps: {average_time_difference:.2f} hours")
print(f"Total de registros processados: {len(df)}")
