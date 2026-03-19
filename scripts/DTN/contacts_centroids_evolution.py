def grafico_distancia_centroid(arquivo_rawdata, arquivo_map, node_id_alvo):
    # node_id_alvo ex: 0 (para o animal 93)
    original_id = [k for k, v in ID_MAP.items() if v == node_id_alvo][0]
    
    df_map = pd.read_csv(arquivo_map, header=None, names=['id', 'ts', 'lon', 'lat'])
    df_map['ts'] = pd.to_datetime(df_map['ts'])
    df_contacts = pd.read_csv(arquivo_rawdata)
    
    # Converter o 'id' do contato (segundos) em timestamp real
    t0 = df_map['ts'].min()
    df_contacts['real_ts'] = t0 + pd.to_timedelta(df_contacts['id'], unit='s')
    
    plt.figure(figsize=(12, 6))
    
    # Para cada centroid (nó fixo) no arquivo
    for centroid_id in df_contacts['to'].unique():
        c_data = df_contacts[df_contacts['to'] == centroid_id]
        
        # Estimar posição do centroid (média das posições do animal durante o estado 'up')
        up_times = c_data[c_data['state'] == 'up']['real_ts']
        posicoes_contato = df_map[(df_map['id'] == original_id) & (df_map['ts'].isin(up_times))]
        
        if not posicoes_contato.empty:
            c_lon, c_lat = posicoes_contato['lon'].mean(), posicoes_contato['lat'].mean()
            
            # Calcular distância temporal do animal a esse ponto fixo
            animal_pos = df_map[df_map['id'] == original_id].copy()
            animal_pos['d'] = animal_pos.apply(lambda r: haversine(r['lon'], r['lat'], c_lon, c_lat), axis=1)
            
            # Suavizar para o gráfico (média diária)
            resampled = animal_pos.set_index('ts')['d'].resample('1D').mean()
            plt.plot(resampled.index, resampled, label=f'Distância ao Nó Fixo {centroid_id}')

    plt.title(f'Distância do Nó {node_id_alvo} aos Centros de Contato')
    plt.ylabel('Distância (km)')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()

# Exemplo de uso:
# grafico_distancia_centroid('down_contact_93_centroids_16_kmeans_rawdata.csv', 'map_jaguar_mamiraua_all_animals.csv', 0)