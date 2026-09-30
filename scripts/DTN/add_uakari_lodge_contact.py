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

# Centro Geográfico Calculado (Média)
GATEWAY_LAT = -3.048894
GATEWAY_LON = -64.857451

def run(animal_id_str, file_rawdata_name, interpolation_method=None):
    results_dir = results_folder(file_rawdata_name)

    # CORREÇÃO 1: Tratar 'rawdata' corretamente para a leitura
    if interpolation_method and interpolation_method != 'rawdata':
        animal_path = os.path.join(results_dir, 'Interpolation', f'map_{animal_id_str}_interpolation_{interpolation_method}_merged.csv')
    else:
        animal_path = os.path.join(results_dir, f'map_{animal_id_str}.csv')
    
    if not os.path.exists(animal_path):
        return

    # 1. Carregamento dos dados
    df_animal = pd.read_csv(animal_path, header=None, names=['animal_id', 'timestamp', 'lon', 'lat'])
    
    # 2. Limpeza e Conversão Rígida
    df_animal['timestamp'] = pd.to_datetime(df_animal['timestamp'], format="%Y-%m-%d %H:%M:%S", errors='coerce')
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
            dist = geodesic((row['lat'], row['lon']), (GATEWAY_LAT, GATEWAY_LON)).meters
            
            if dist <= LIMIT_DISTANCE_M:
                delta = (row['timestamp'] - BASE_DATE).total_seconds() / 3600
                
                contacts_list.append({
                    'id': delta, # CORREÇÃO 2: Removido o int() para manter a precisão do float
                    'conn': 'CONN',
                    'for': FIXED_GATEWAY_ID,  # Gateway agora é a origem (No 0)
                    'to': mapped_id,          # Onça agora é o destino (No 1)
                    'state': 'up'
                })
        except Exception as e:
            continue

    if not contacts_list:
        return

    # 4. Formatação e Eventos DOWN
    df_res = pd.DataFrame(contacts_list)
    
    df_down = df_res.copy()
    df_down['id'] = df_down['id'] + 1.0 # 1 hora de duração (mantido como float)
    df_down['state'] = 'down'

    # Inversão de origem/destino no evento down
    df_down['for'], df_down['to'] = df_res['to'], df_res['for']

    final_df = pd.concat([df_res, df_down], ignore_index=True)
    
    # Ordenação
    final_df.sort_values(by=['id', 'state'], ascending=[True, False], inplace=True)
    
    # 5. Salvamento
    out_dir = os.path.join(results_dir, 'contacts')
    os.makedirs(out_dir, exist_ok=True)
    
    # CORREÇÃO 3: Tratar 'rawdata' corretamente para o nome do arquivo de saída
    if interpolation_method and interpolation_method != 'rawdata':
        output_name = f"down_contact_{animal_id_str}_uakari_lodge_interpolation_{interpolation_method}_merged.csv"
    else:
        output_name = f"down_contact_{animal_id_str}_uakari_lodge.csv"
        
    output_path = os.path.join(out_dir, output_name)
    
    final_df[['id', 'conn', 'for', 'to', 'state']].to_csv(output_path, index=False)
    print(f"[OK] Gateway 40: {len(df_res)} contatos para onça {animal_id_str}")

    # print(f"[OK] Gateway 40: {len(df_res)} contatos para onça {animal_id_str}")

    # # --- NOVA INSERÇÃO NO BANCO DE DADOS ---
    # try:
    #     import psycopg2
    #     from psycopg2.extras import execute_values
        
    #     DB_CONFIG = {
    #         "host": "localhost",
    #         "database": "myappdb",
    #         "port": "5435",
    #         "user": "myuser",
    #         "password": "mypassword"
    #     }
        
    #     conn = psycopg2.connect(**DB_CONFIG)
    #     cur = conn.cursor()
        
    #     # Iterando com iterrows() para acessar colunas com segurança
    #     data_to_insert = [
    #         (float(row['id']), row['conn'], int(row['for']), int(row['to']), row['state'])
    #         for _, row in final_df.iterrows()
    #     ]

    #     query = """
    #         INSERT INTO jaguar_contacts (simulation_time, conn, for_contact, to_contact, state)
    #         VALUES %s
    #     """
        
    #     if data_to_insert:
    #         execute_values(cur, query, data_to_insert)
    #         conn.commit()
    #         print(f"DB: {len(data_to_insert)} registros do Uakari inseridos com sucesso.")
            
    # except Exception as e:
    #     print(f"Erro ao inserir no banco: {e}")
    #     if 'conn' in locals(): conn.rollback()
    # finally:
    #     if 'cur' in locals(): cur.close()
    #     if 'conn' in locals(): conn.close()