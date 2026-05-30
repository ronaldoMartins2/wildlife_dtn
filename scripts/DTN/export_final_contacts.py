import os
import pandas as pd
import psycopg2
from Common.utils import results_folder

DB_CONFIG = {
    "host": "localhost",
    "database": "myappdb",
    "port": "5435",
    "user": "myuser",
    "password": "mypassword"
}

def run(file_rawdata_name):
    print("Gerando arquivo de trace final...")
    
    results_dir = results_folder(file_rawdata_name)
    # Salvar como .txt ou .ops (formato comum para simuladores)
    output_path = os.path.join(r"C:\\Users\\jccme\\OneDrive\\Documentos\\MESTRADO\\WILD_LIFE_PROJECT\\wildlife_dtn\\scripts\\Results\\jaguar_mamiraua", 'contacts', 'jaguar_mamiraua_contacts.txt')

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        # 1. Seleciona as colunas
        # 2. Ordena por tempo (Crescente)
        # 3. Ordena por estado DESC ('up' > 'down', logo 'up' aparece antes)
        query = """
            SELECT 
                simulation_time,
                conn,
                for_contact, 
                to_contact,
                state
            FROM jaguar_contacts
            ORDER BY 
                simulation_time ASC, 
                state DESC
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print("AVISO: Banco de dados vazio. Nenhum arquivo gerado.")
            return

        # --- Formatação Rígida ---
        
        # 1. Converter tempo, for e to para Inteiros
        df['simulation_time'] = df['simulation_time'].astype(int)
        df['for_contact'] = df['for_contact'].astype(int)
        df['to_contact'] = df['to_contact'].astype(int)

        # 2. Exportar
        # sep=' '      -> Separado por espaço
        # header=False -> Sem cabeçalho
        # index=False  -> Sem número da linha
        df.to_csv(output_path, sep=' ', header=False, index=False)
        
        print(f"SUCESSO! Arquivo gerado: {output_path}")
        print(f"Exemplo das primeiras linhas:\n")
        # Mostra prévia formatada para você conferir no terminal
        print(df.head().to_string(index=False, header=False))
        
    except Exception as e:
        print(f"Erro na exportação: {e}")
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    # Teste
    run("jaguar_mamiraua")