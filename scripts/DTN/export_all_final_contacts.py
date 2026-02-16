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
    output_dir = os.path.join(results_dir, 'contacts')
    os.makedirs(output_dir, exist_ok=True)
    
    # Nome do arquivo final conforme o padrão solicitado
    filename = f"{file_rawdata_name}_contacts_{n_centroids}_centroids_{algorithm}_{interpolation}.txt"
    output_path = os.path.join(output_dir, filename)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 1. Limpar a tabela temporária no banco para esta rodada
        cursor.execute("TRUNCATE TABLE jaguar_contacts")
        conn.commit()

        # 2. Identificar quais arquivos CSV devem ser incluídos
        all_files = glob.glob(os.path.join(results_dir, "contacts", "*.csv"))
        
        files_to_import = []
        for f in all_files:
            basename = os.path.basename(f)
            
            # Regra A: Contatos entre onças (ex: down_contact_93_96.csv)
            # Geralmente não possuem a palavra 'centroids' no nome
            if "centroids" not in basename and basename.startswith("down_contact_"):
                files_to_import.append(f)
                
            # Regra B: Contatos com centroides específicos desta configuração
            # (ex: down_contact_93_centroids_8_birch_rawdata.csv)
            elif f"centroids_{n_centroids}_{algorithm}_{interpolation}" in basename:
                files_to_import.append(f)

        if not files_to_import:
            print(f"AVISO: Nenhum arquivo encontrado para a config {n_centroids}-{algorithm}-{interpolation}")
            return

        print(f"Importando {len(files_to_import)} arquivos para o banco...")

        # 3. Carregar dados no banco
        for f in files_to_import:
            df_temp = pd.read_csv(f)
            # Mapeia colunas do CSV (id, conn, for, to, state) para colunas do banco
            for _, row in df_temp.iterrows():
                cursor.execute("""
                    INSERT INTO jaguar_contacts (simulation_time, conn, for_contact, to_contact, state)
                    VALUES (%s, %s, %s, %s, %s)
                """, (int(row['id']), row['conn'], int(row['for']), int(row['to']), row['state']))
        
        conn.commit()

        # 4. Exportar o resultado final ordenado
        query = """
            SELECT simulation_time, conn, for_contact, to_contact, state
            FROM jaguar_contacts
            ORDER BY simulation_time ASC, state DESC
        """
        
        df_final = pd.read_sql(query, conn)

        # Formatação final (Inteiros)
        df_final['simulation_time'] = df_final['simulation_time'].astype(int)
        df_final['for_contact'] = df_final['for_contact'].astype(int)
        df_final['to_contact'] = df_final['to_contact'].astype(int)

        df_final.to_csv(output_path, sep=' ', header=False, index=False)
        
        print(f"SUCESSO! Arquivo gerado: {filename} ({len(df_final)} linhas)")
        
    except Exception as e:
        print(f"Erro ao processar {filename}: {e}")
    finally:
        if 'conn' in locals(): conn.close()

# if __name__ == "__main__":
#     # Parametrização dos experimentos
#     project = "jaguar_mamiraua"
    
#     # Listas conforme sua descrição
#     centroids = [8, 16, 32]
#     algorithms = ["birch", "kmeans", "optics"]
#     interpolations = ["rawdata"] # Primeiro momento apenas rawdata

#     # Loop para gerar todos os arquivos de uma vez
#     for n in centroids:
#         for alg in algorithms:
#             for interp in interpolations:
#                 run(project, n, alg, interp)