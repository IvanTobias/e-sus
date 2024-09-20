
import psycopg2
from psycopg2 import sql

def create_database():
    # Configurações predefinidas para o banco local
    dbname = "esus"
    user = "postgres"
    password = "esus"
    host = "localhost"
    port = "5432"

    try:
        # Conectando ao PostgreSQL
        conn = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
        conn.autocommit = True
        cursor = conn.cursor()

        # Verificar se o banco de dados já existe
        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [dbname])
        exists = cursor.fetchone()

        if not exists:
            # Criando o banco de dados
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            print(f"Banco de dados {dbname} criado com sucesso.".encode('utf-8').decode('utf-8'))
        else:
            print(f"Banco de dados {dbname} já existe.".encode('utf-8').decode('utf-8'))

        # Fechar a conexão
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Erro ao criar o banco de dados: {str(e)}".encode('utf-8').decode('utf-8'))

if __name__ == "__main__":
    create_database()
