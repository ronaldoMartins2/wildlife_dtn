import pandas as pd

# 1. Carregar o arquivo de entrada
# Substitua 'seus_dados.csv' pelo nome real do seu arquivo
df = pd.read_csv(r'rawdata\jaguar_mamiraua.csv')

# 2. Selecionar e reordenar as colunas
# Estamos pegando: ID, timestamp, longitude, latitude
colunas_desejadas = [
    'individual.local.identifier (ID)', 
    'timestamp', 
    'location.long', 
    'location.lat'
]

df_filtrado = df[colunas_desejadas]

# 3. Salvar no novo formato
# index=False: não salva o número da linha
# header=False: remove o cabeçalho para ficar igual ao seu exemplo
df_filtrado.to_csv(r'scripts\Results\jaguar_mamiraua\map_jaguar_mamiraua_all_animals.csv', index=False, header=False)

print("Arquivo 'dados_processados.csv' criado com sucesso!")