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
# GATEWAY_LAT = -3.0631181700354824
# GATEWAY_LON = -64.84904676693218

# Centro da Área Total (Bounding Box)
# GATEWAY_LAT = -2.939023
# GATEWAY_LON = -64.877404

# Centro Geográfico Calculado (Média)
GATEWAY_LAT = -3.048894
GATEWAY_LON = -64.857451

def run(animal_id_str, file_rawdata_name):
    results_dir = results_folder(file_rawdata_name)
    #animal_path = os.path.join(r"C:\\Users\\jccme\\OneDrive\\Documentos\\MESTRADO\\WILD_LIFE_PROJECT\\wildlife_dtn\\scripts\\Results\\jaguar_mamiraua", f'map_{animal_id_str}.csv')
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
                delta = (row['timestamp'] - BASE_DATE).total_seconds() / 3600
                contacts_list.append({
                    'id': int(delta),
                    'conn': 'CONN',
                    # 'for': mapped_id,
                    # 'to': FIXED_GATEWAY_ID,
                    'for': FIXED_GATEWAY_ID,  # Gateway agora é a origem (No 0)
                    'to': mapped_id,          # Onça agora é o destino (No 1)
                    'state': 'up'
                })
        except Exception as e:
            continue # Pula linhas que ainda assim deem erro

    if not contacts_list:
        return

    # 4. Formatação e Eventos DOWN
    df_res = pd.DataFrame(contacts_list)
    
    df_down = df_res.copy()
    df_down['id'] = df_down['id'] + 1 # 1 hora de duração
    df_down['state'] = 'down'

    # --- ADICIONE ESTA LINHA PARA REALIZAR A INVERSÃO ---
    # O que era 'for' vira 'to' e o que era 'to' vira 'for'
    df_down['for'], df_down['to'] = df_res['to'], df_res['for']
    # ----------------------------------------------------

    final_df = pd.concat([df_res, df_down], ignore_index=True)
    
    # Ordenação: primeiro por tempo, depois garante que 'up' venha antes de 'down' se o tempo for igual
    final_df.sort_values(by=['id', 'state'], ascending=[True, False], inplace=True)
    
    # 5. Salvamento
    #out_dir = os.path.join(r"C:\\Users\\jccme\\OneDrive\\Documentos\\MESTRADO\\WILD_LIFE_PROJECT\\wildlife_dtn\\scripts\\Results\\jaguar_mamiraua", 'contacts')
    out_dir = os.path.join(results_dir, 'contacts')
    
    os.makedirs(out_dir, exist_ok=True)
    output_path = os.path.join(out_dir, f"down_contact_{animal_id_str}_uakari_lodge.csv")
    
    final_df[['id', 'conn', 'for', 'to', 'state']].to_csv(output_path, index=False)
    print(f"[OK] Gateway 40: {len(df_res)} contatos para onça {animal_id_str}")