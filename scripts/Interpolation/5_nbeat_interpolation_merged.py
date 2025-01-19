import pandas as pd  
import sys  


def merge_and_interpolate():
    
    # Carrega o primeiro arquivo CSV. O nome do arquivo é definido dinamicamente a partir do argumento do sistema.
    data1 = pd.read_csv(f'map_{sys.argv[1]}.csv', skiprows=0, header=None)  
    # Carrega o segundo arquivo CSV. O nome do arquivo é definido dinamicamente a partir do argumento do sistema.
    data2 = pd.read_csv(f'map_{sys.argv[1]}_interpolation.csv', skiprows=0, header=None)
    
    # Define as colunas para o DataFrame do primeiro arquivo.
    data1.columns = ['ID', 'Datetime', 'Longitude', 'Latitude']
    # Define as colunas para o DataFrame do segundo arquivo, idênticas às do primeiro.
    data2.columns = ['ID', 'Datetime', 'Longitude', 'Latitude']
   
    # Converte a coluna 'Datetime' do primeiro DataFrame para o formato datetime, tratando valores inválidos como NaT.
    data1['Datetime'] = pd.to_datetime(data1['Datetime'], format='%m/%d/%y %H:%M', errors='coerce')
    # Converte a coluna 'Datetime' do segundo DataFrame para o formato datetime, tratando valores inválidos como NaT.
    data2['Datetime'] = pd.to_datetime(data2['Datetime'], format='%m/%d/%y %H:%M', errors='coerce')

    # Junta os dois DataFrames em um único DataFrame, ignorando os índices originais.
    combined_data = pd.concat([data1, data2], ignore_index=True)

    # Converte a coluna 'Datetime' de volta para o formato de string.
    combined_data['Datetime'] = combined_data['Datetime'].dt.strftime('%m/%d/%y %H:%M')

    # Ordena o DataFrame combinado pela coluna 'Datetime' em ordem crescente.
    combined_data = combined_data.sort_values(by='Datetime')    
    
    # Salva o DataFrame combinado em um novo arquivo CSV. O nome do arquivo é definido dinamicamente.
    combined_data.to_csv(f'map_{sys.argv[1]}_interpolation_merged.csv', index=False, header=None)

# Código executado se o script estiver sendo rodado diretamente, e não importado como módulo.
if __name__ == "__main__":
    merge_and_interpolate()
