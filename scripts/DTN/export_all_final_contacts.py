import os
import pandas as pd
import psycopg2
import glob
from Common.utils import results_folder

DB_CONFIG = {
    "host": "localhost",
    "database": "myappdb",
    "port": "5435",
    "user": "myuser",
    "password": "mypassword"
}

def run(file_rawdata_name, n_centroids, algorithm, interpolation):
    """
    Gera o trace consolidando contatos onça-onça e onça-centroide.
    """
    print(f"\n>>> Iniciando: {n_centroids} centroids | {algorithm} | {interpolation}")
    
    results_dir = results_folder(file_rawdata_name)
    print("@@@@@@@@@@@@@@@@@ Amazonas @@@@@@@@@@@@@@@@@")

    output_dir = os.path.join(results_dir, 'contacts')
    os.makedirs(output_dir, exist_ok=True)
    
    # CORREÇÃO: Extrai apenas o nome do arquivo (remove "rawdata/") e remove a extensão ".csv"
    pure_filename = os.path.splitext(os.path.basename(file_rawdata_name))[0]
    
    # O nome do arquivo final agora não conterá barras '/' que quebram o caminho
    filename = f"{pure_filename}_contacts_{n_centroids}_centroids_{algorithm}_{interpolation}.txt"
    output_path = os.path.join(output_dir, filename)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 1. Garantir que a tabela existe (com FLOAT para o simulation_time)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jaguar_contacts (
                simulation_time FLOAT,
                conn VARCHAR(10),
                for_contact INTEGER,
                to_contact INTEGER,
                state VARCHAR(10)
            )
        """)
        
        # Limpar a tabela para esta rodada
        cursor.execute("TRUNCATE TABLE jaguar_contacts")
        conn.commit()

        # 2. Identificar quais arquivos CSV devem ser incluídos
        all_files = glob.glob(os.path.join(results_dir, "contacts", "*.csv"))
        files_to_import = []
        
        for f in all_files:
            basename = os.path.basename(f)
            
            # Regra A: Contatos entre onças (não possuem 'centroids' nem 'uakari_lodge')
            if "centroids" not in basename and "uakari_lodge" not in basename and basename.startswith("down_contact_"):
                files_to_import.append(f)
                
            # Regra C: Contatos com Uakari Lodge
            elif "uakari_lodge" in basename:
                files_to_import.append(f)
                
            # Regra B: Contatos com centroides desta configuração exata
            elif f"centroids_{n_centroids}_{algorithm}" in basename:
                if interpolation == "rawdata":
                    if "interpolation" not in basename:
                        files_to_import.append(f)
                else:
                    if f"interpolation_{interpolation}" in basename or f"interpolation_nbeats_{interpolation}" in basename:
                        files_to_import.append(f)

        if not files_to_import:
            print(f"AVISO: Nenhum arquivo encontrado para a config {n_centroids}-{algorithm}-{interpolation}")
            return

        print(f"Importando {len(files_to_import)} arquivos para o banco...")

        # 3. Carregar dados no banco em LOTE
        for f in files_to_import:
            df_temp = pd.read_csv(f)
            
            records_to_insert = [
                (float(row['id']), row['conn'], int(row['for']), int(row['to']), row['state'])
                for _, row in df_temp.iterrows()
            ]
            
            cursor.executemany("""
                INSERT INTO jaguar_contacts (simulation_time, conn, for_contact, to_contact, state)
                VALUES (%s, %s, %s, %s, %s)
            """, records_to_insert)
        
        conn.commit()

        # 4. Exportar o resultado final ordenado
        cursor.execute("""
            SELECT simulation_time, conn, for_contact, to_contact, state
            FROM jaguar_contacts
            ORDER BY simulation_time ASC, state DESC
        """)
        records = cursor.fetchall()
        
        df_final = pd.DataFrame(records, columns=['simulation_time', 'conn', 'for_contact', 'to_contact', 'state'])

        df_final['for_contact'] = df_final['for_contact'].astype(int)
        df_final['to_contact'] = df_final['to_contact'].astype(int)

        # Salva formatado para o ONE Simulator (separado por espaço, sem cabeçalho)
        df_final.to_csv(output_path, sep=' ', header=False, index=False)
        
        print(f"SUCESSO! Arquivo gerado: {filename} ({len(df_final)} linhas)")
        
    except Exception as e:
        print(f"Erro ao processar {filename}: {e}")
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()