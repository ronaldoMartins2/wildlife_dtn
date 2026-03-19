import os
import pandas as pd
from geopy.distance import geodesic
from datetime import datetime
from Common.utils import results_folder

# --- CONFIGURAÇÕES ---
LIMIT_DISTANCE_M = 250 
BASE_DATE = datetime(2014, 3, 14, 4, 0)
ANIMAL_OFFSET = 93
FIXED_GATEWAY_ID = 40 

# Uakari Lodge - Certifique-se que estes valores estão corretos: (Lat, Lon)
GATEWAY_LAT = -3.0631181700354824
GATEWAY_LON = -64.84904676693218

def run(animal_id_str, file_rawdata_name):
    results_dir = results_folder(file_rawdata_name)
    animal_path = os.path.join(results_dir, f'map_{animal_id_str}.csv')
    
    if not os.path.exists(animal_path):
        return

    # 1. Carregamento dos dados
    # Note que forcei os nomes das colunas para garantir a ordem
    df_animal = pd.read_csv(animal_path, header=None, names=['animal_id', 'timestamp', 'lon', 'lat'])
    
    # 2. Limpeza e Conversão Rígida
    df_animal['timestamp'] = pd.to_datetime(df_animal['timestamp'], format="%m/%d/%y %H:%M", errors='coerce')
    df_animal['lat'] = pd.to_numeric(df_animal['lat'], errors='coerce')
    df_animal['lon'] = pd.to_numeric(df_animal['lon'], errors='coerce')
    
    # REMOVE valores que estão fora do range global (Evita o erro do Geopy)
    df_animal = df_animal[
        (df_animal['lat'] >= -90) & (df_animal['lat'] <= 90) &
        (df_animal['lon'] >= -180) & (df_animal['lon'] <= 180)
    ]
    df_animal.dropna(subset=['timestamp', 'lat', 'lon'], inplace=True)

    mapped_id = int(animal_id_str) - ANIMAL_OFFSET
    contacts_list = []

    # 3. Verificação de Proximidade
    for _, row in df_animal.iterrows():
        try:
            # Geopy exige (Latitude, Longitude)
            dist = geodesic((row['lat'], row['lon']), (GATEWAY_LAT, GATEWAY_LON)).meters
            
            if dist <= LIMIT_DISTANCE_M:
                delta = (row['timestamp'] - BASE_DATE).total_seconds()
                contacts_list.append({
                    'id': int(delta),
                    'conn': 'CONN',
                    'for': mapped_id,
                    'to': FIXED_GATEWAY_ID,
                    'state': 'up'
                })
        except Exception as e:
            continue # Pula linhas que ainda assim deem erro

    if not contacts_list:
        return

    # 4. Formatação e Eventos DOWN
    df_res = pd.DataFrame(contacts_list)
    
    df_down = df_res.copy()
    df_down['id'] = df_down['id'] + 3600 # 1 hora de duração
    df_down['state'] = 'down'

    final_df = pd.concat([df_res, df_down], ignore_index=True)
    final_df.sort_values(by=['id', 'state'], ascending=[True, False], inplace=True)

    # 5. Salvamento
    out_dir = os.path.join(results_dir, 'contacts')
    os.makedirs(out_dir, exist_ok=True)
    output_path = os.path.join(out_dir, f"down_contact_{animal_id_str}_uakari_lodge.csv")
    
    final_df[['id', 'conn', 'for', 'to', 'state']].to_csv(output_path, index=False)
    print(f"[OK] Gateway 40: {len(df_res)} contatos para onça {animal_id_str}")