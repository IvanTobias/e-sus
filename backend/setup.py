# backend/setup.py
import psycopg2
from psycopg2 import sql

def create_database():
    dbname = "esus"
    user = "postgres"
    password = "esus"
    host = "localhost"
    port = "5432"

    try:
        conn = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [dbname])
        exists = cursor.fetchone()

        if not exists:
            # Criar o banco com ENCODING 'UTF8'
            cursor.execute(sql.SQL(
                "CREATE DATABASE {} WITH ENCODING 'UTF8' TEMPLATE template0"
            ).format(sql.Identifier(dbname)))
            print(f"✅ Banco de dados {dbname} criado com ENCODING UTF8.".encode('utf-8').decode('utf-8'))
        else:
            print(f"⚠️ Banco de dados {dbname} já existe.".encode('utf-8').decode('utf-8'))

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao criar o banco de dados: {str(e)}".encode('utf-8').decode('utf-8'))

def database_exists(cursor, dbname):
    cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [dbname])
    return cursor.fetchone() is not None

if __name__ == "__main__":
    create_database()
