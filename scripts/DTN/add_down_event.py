import os
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from Common.utils import results_folder

# --- CONFIGURAÇÕES DO BANCO ---
DB_CONFIG = {
    "host": "localhost",
    "database": "myappdb",
    "port": "5435",
    "user": "myuser",
    "password": "mypassword"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def run(file_target, file_rawdata_name):
    results_dir = results_folder(file_rawdata_name)
    
    # --- Lógica de Nome de Arquivo ---
    filename = f"{file_target}.csv"
    path_original = os.path.join(results_dir, filename)
    path_contact = os.path.join(results_dir, 'contacts', f"contact_{file_target}.csv")
    
    input_path = path_original
    if not os.path.exists(path_original) and os.path.exists(path_contact):
        input_path = path_contact
        filename = f"contact_{file_target}.csv"

    # --- Verificações de Segurança ---
    if not os.path.exists(input_path):
        return
    if os.path.getsize(input_path) == 0:
        return

    try:
        # Lê o CSV original
        df = pd.read_csv(input_path, header=0)
    except Exception:
        return

    if df.empty:
        return

    # Garante que 'id' (tempo) é numérico e remove nulos
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df.dropna(subset=['id'], inplace=True)
    if df.empty: 
        return

    # --- Lógica de Eventos UP/DOWN ---
    # 1. Configura eventos UP
    df['id'] = df['id'] + np.random.randint(0, 5, size=len(df))
    df['state'] = 'up'

    # 2. Cria eventos DOWN (+1h de duração)
    df_down = df.copy()
    df_down['id'] = df_down['id'] + 3600
    df_down['state'] = 'down'

    # 3. Une e ordena para garantir a sequência temporal correta
    final_df = pd.concat([df, df_down], ignore_index=True)
    final_df.sort_values(by=['id', 'state'], ascending=[True, False], inplace=True)

    # 4. Salva o CSV processado (mantendo a estrutura original das colunas)
    out_dir = os.path.join(results_dir, 'contacts')
    os.makedirs(out_dir, exist_ok=True)
    output_path = os.path.join(out_dir, f'down_{filename}')
    final_df.to_csv(output_path, index=False)
    print(f"Arquivo salvo: {output_path}")

    # --- INSERÇÃO NO BANCO DE DADOS ---
    conn = get_db_connection()
    cur = conn.cursor()
    data_to_insert = []
    
    for row in final_df.itertuples(index=False):
        try:
            sim_time = float(row.id)
            conn_val = row.conn
            state_val = row.state
            
            # Posições 2 e 3 do CSV original (ajustadas pelo offset de 93)
            id_origem = int(row[2]) - 93
            id_destino = int(row[3]) - 93
            
            # INVERSÃO DINÂMICA: 
            # Se for 'up', mantém origem -> destino. 
            # Se for 'down', inverte para destino -> origem.
            if state_val == 'up':
                val_for, val_to = id_origem, id_destino
            else:
                val_for, val_to = id_destino, id_origem
            
            data_to_insert.append((sim_time, conn_val, val_for, val_to, state_val))
        except Exception:
            continue

    query = """
        INSERT INTO jaguar_contacts (simulation_time, conn, for_contact, to_contact, state)
        VALUES %s
    """

    try:
        if data_to_insert:
            execute_values(cur, query, data_to_insert)
            conn.commit()
            print(f"DB: {len(data_to_insert)} registros inseridos (com inversão nos eventos DOWN).")
    except psycopg2.DatabaseError as error:
        print(f"Erro SQL: {error}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()