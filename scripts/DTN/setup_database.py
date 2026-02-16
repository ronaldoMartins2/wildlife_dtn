import psycopg2
import sys

# Ajuste suas configurações se necessário
DB_CONFIG = {
    "host": "localhost",
    "database": "myappdb",
    "port": "5435",
    "user": "myuser",
    "password": "mypassword"
}

def recreate_table():
    print("Conectando ao banco de dados...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        # 1. Apagar a tabela antiga
        cur.execute("DROP TABLE IF EXISTS jaguar_contacts;")
        
        # 2. Criar a NOVA tabela
        # record_id: É a chave primária real (única e automática)
        # simulation_time: É o seu "id" antigo (tempo em minutos). Aceita valores repetidos.
        sql_create = """
            CREATE TABLE jaguar_contacts (
                record_id SERIAL PRIMARY KEY,
                simulation_time NUMERIC, 
                conn VARCHAR(50),
                for_contact INTEGER,
                to_contact INTEGER,
                state VARCHAR(10)
            );
        """
        # sql_create = """
        #     CREATE TABLE jaguar_contacts (
        #         id SERIAL PRIMARY KEY,
        #         conn VARCHAR(255) NOT NULL,
        #         for_contact VARCHAR(255),
        #         to_contact VARCHAR(255),
        #         state VARCHAR(50),
        #         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        #     );
        # """
        cur.execute(sql_create)
        
        conn.commit()
        print("Tabela 'jaguar_contacts' recriada com sucesso!")
        print("Estrutura: record_id (PK), simulation_time, conn, for, to, state.")
        # print("Estrutura: id, conn, for, to, state, created_at, updated_at")
        
    except psycopg2.Error as e:
        print(f"Erro no banco: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    recreate_table()