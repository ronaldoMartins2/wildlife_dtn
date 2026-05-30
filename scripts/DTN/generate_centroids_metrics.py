import pandas as pd

def normalizar_metrica(df, nome_metrica):
        # Transforma colunas '8', '16', '32' em linhas
        df_long = df.melt(id_vars=['method'], var_name='K', value_name=nome_metrica)
        df_long['K'] = df_long['K'].astype(int)
        # Padroniza nomes para o merge
        df_long['method'] = df_long['method'].str.lower().replace({'k-means': 'kmeans'})
        return df_long

def gerar_tabela_consolidada():
    # 1. Carregamento dos dados brutos
    df_espacial = pd.read_csv('scripts\\Results\\jaguar_mamiraua\\distances_to_own_centroid\\stats_animal_to_own_centroid.csv')
    df_visitas = pd.read_csv('scripts\\Results\\jaguar_mamiraua\\figuras_tempo_retorno\\table_mean_number_of_visits.csv')
    df_residencia = pd.read_csv('scripts\\Results\\jaguar_mamiraua\\figuras_tempo_retorno\\table_mean_residence_time_hours.csv')
    df_retorno = pd.read_csv('scripts\\Results\\jaguar_mamiraua\\figuras_tempo_retorno\\table_mean_return_time_hours.csv')

    # 2. Processamento das métricas de rede
    visitas = normalizar_metrica(df_visitas, 'Visitas')
    residencia = normalizar_metrica(df_residencia, 'Residência (h)')
    retorno = normalizar_metrica(df_retorno, 'Retorno (h)')

    # 3. Preparação dos dados espaciais
    df_espacial['algorithm'] = df_espacial['algorithm'].str.lower()
    
    # 4. Cruzamento de todos os dados (Merge)
    df_final = df_espacial.merge(visitas, left_on=['algorithm', 'clusters'], right_on=['method', 'K'])
    df_final = df_final.merge(residencia, left_on=['algorithm', 'clusters'], right_on=['method', 'K'])
    df_final = df_final.merge(retorno, left_on=['algorithm', 'clusters'], right_on=['method', 'K'])

    # 5. Seleção e Formatação Final
    colunas_finais = [
        'algorithm', 'clusters', 'mean', 'std', 'max', 
        'Visitas', 'Residência (h)', 'Retorno (h)'
    ]
    
    df_output = df_final[colunas_finais].sort_values(by=['clusters', 'algorithm'])
    
    # Renomear colunas para o padrão de publicação
    df_output.columns = [
        'Algoritmo', 'K', 'Dist. Média (m)', 'Std (m)', 'Dist. Máx (m)', 
        'Visitas (média)', 'Tempo Residência (h)', 'Tempo Retorno (h)'
    ]
    
    # Exportar resultado
    df_output.to_csv('tabela_resultados_distancias.csv', index=False)
    return df_output

# Execução do script
tabela = gerar_tabela_consolidada()
print(tabela.to_string(index=False))