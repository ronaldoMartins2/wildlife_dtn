import pandas as pd

# 1. Carregar o arquivo CSV
df = pd.read_csv(r'rawdata\jaguar_mamiraua.csv')

# 2. Converter a coluna 'timestamp' para o formato datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Nome da coluna que contém o ID
id_col = 'individual.local.identifier (ID)'

# 3. Criar máscaras (filtros)
mask_2_anos = df[id_col].isin([95, 96, 99]) # Onças 95, 96 e 99
mask_1_ano = df[id_col] == 97               # Onça 97
mask_4_anos = df[id_col] == 98              # Onça 98

# 4. Aplicar o aumento de anos nas datas filtradas
df.loc[mask_2_anos, 'timestamp'] = df.loc[mask_2_anos, 'timestamp'].apply(lambda x: x + pd.DateOffset(years=2))
df.loc[mask_1_ano, 'timestamp'] = df.loc[mask_1_ano, 'timestamp'].apply(lambda x: x + pd.DateOffset(years=1))
df.loc[mask_4_anos, 'timestamp'] = df.loc[mask_4_anos, 'timestamp'].apply(lambda x: x + pd.DateOffset(years=4))

# 5. Salvar o resultado com a FORMATAÇÃO DE DATA ESPECÍFICA
output_file = 'jaguar_mamiraua_anos_atualizados.csv'

# AQUI ESTÁ A MUDANÇA: adicionamos o date_format
df.to_csv(output_file, index=False, date_format='%m/%d/%y %H:%M')

print(f"Arquivo atualizado salvo com sucesso como: {output_file}")