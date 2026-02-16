import os
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from Common.utils import results_folder

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
        # Silencioso se o arquivo não existe (sem contatos)
        return
    if os.path.getsize(input_path) == 0:
        return

    try:
        # Lê o CSV. O Script 1 salva com coluna 'id' (que é o tempo)
        df = pd.read_csv(input_path, header=0)
    except Exception:
        return

    if df.empty:
        return

    # Garante que 'id' (tempo) é numérico
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df.dropna(subset=['id'], inplace=True)
    if df.empty: return

    # --- Lógica de Eventos UP/DOWN ---
    # Adiciona delay aleatório ao tempo (id)
    random_delays = np.random.randint(0, 5, size=len(df)) # Por que somar esse valor aleatório ao tempo?
    df['id'] = df['id'] + random_delays
    
    # Cria evento DOWN (+1h = 3600s) - tempo de contato entre onças
    df_down = df.copy()
    df_down['id'] = df_down['id'] + 3600
    df_down['state'] = 'down'

    final_df = pd.concat([df, df_down], ignore_index=True)
    final_df.sort_values(by='id', inplace=True)

    # Salva o arquivo CSV processado
    output_path = os.path.join(results_dir, 'contacts', f'down_{filename}')
    final_df.to_csv(output_path, index=False)
    print(f"Processado: {output_path}")

    # --- INSERÇÃO NO BANCO (ATUALIZADA) ---
    conn = get_db_connection()
    cur = conn.cursor()

    # Prepara os dados. 
    # Mapeamos: CSV 'id' -> Banco 'simulation_time'
    # O Banco vai gerar o 'record_id' automaticamente.
    
    data_to_insert = []
    
    # Vamos usar itertuples mas com cuidado nos nomes das colunas
    # Ordem esperada na query: simulation_time, conn, for_contact, to_contact, state
    
    for row in final_df.itertuples(index=False):
        try:
            # Pega os valores do DF (row.id é o tempo)
            sim_time = float(row.id)
            conn_val = row.conn
            state_val = row.state
            
            # Ajuste seguro para colunas 'for' e 'to' (subtraindo 93)
            # O Pandas pode ter renomeado 'for' para '_2' ou mantido se for dict
            # Tentativa genérica de pegar os valores das posições 2 e 3
            val_for = int(row[2]) - 93
            val_to = int(row[3]) - 93
            
            data_to_insert.append((sim_time, conn_val, val_for, val_to, state_val))
        except Exception as e:
            # print(f"Erro linha: {e}") 
            continue

    query = """
        INSERT INTO jaguar_contacts (simulation_time, conn, for_contact, to_contact, state)
        VALUES %s
    """

    try:
        if data_to_insert:
            execute_values(cur, query, data_to_insert)
            conn.commit()
            print(f"DB: {len(data_to_insert)} registros inseridos.")
    except psycopg2.DatabaseError as error:
        print(f"Erro SQL: {error}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()