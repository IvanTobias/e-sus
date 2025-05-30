import psycopg2
import sys

# Detalhes da conexão local (ajuste se necessário, mas devem ser os mesmos da sua app)
DB_NAME = "esus"
DB_USER = "postgres"
DB_PASS = "1234"
DB_HOST = "localhost"
DB_PORT = "5432"

print("Python version: " + str(sys.version))
print("Psycopg2 version: " + str(psycopg2.__version__))
print("Default encoding: " + str(sys.getdefaultencoding()))
print("File system encoding: " + str(sys.getfilesystemencoding()))

conn_info = None
try:
    # Conecta para obter informações de codificação
    conn_info = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, dbname=DB_NAME, client_encoding='utf8')
    print("Psycopg2 connection client_encoding: " + str(conn_info.encoding))
    cur_check = conn_info.cursor()
    cur_check.execute("SHOW client_encoding;")
    server_reports_encoding = cur_check.fetchone()[0]
    print("PostgreSQL server reports client_encoding as: " + str(server_reports_encoding))
    cur_check.close()
    if server_reports_encoding.lower() not in ('utf8', 'utf-8'):
        print("ALERTA: A conexão não está efetivamente em UTF-8 segundo o servidor!")
except Exception as e_info:
    print("Erro ao obter informações de codificação da conexão: " + str(e_info))
finally:
    if conn_info:
        conn_info.close()

conn = None
try:
    print("Tentando conectar ao banco de dados local: dbname='" + DB_NAME + "' user='" + DB_USER + "' host='" + DB_HOST + "' port='" + DB_PORT + "' client_encoding='utf8'")
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        client_encoding='utf8'
    )
    print("Conexão bem-sucedida!")

    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS direct_encoding_test;")
    cur.execute("""
        CREATE TABLE direct_encoding_test (
            id SERIAL PRIMARY KEY,
            description TEXT
        );
    """) # Using triple-double-quotes for the SQL string itself
    print("Tabela 'direct_encoding_test' recriada.")

    test_string = "Teste com acentuação e ç, situação normal."
    print("Tentando inserir a string: '" + test_string + "'")
    cur.execute("INSERT INTO direct_encoding_test (description) VALUES (%s)", (test_string,))
    conn.commit()
    print("Inserção bem-sucedida.")

    cur.execute("SELECT description FROM direct_encoding_test WHERE description = %s", (test_string,))
    row = cur.fetchone()
    if row:
        retrieved_string = row[0]
        print("String recuperada do banco: '" + retrieved_string + "'")
        if retrieved_string == test_string:
            print("Sucesso: String recuperada é idêntica à original.")
        else:
            print("ERRO: String recuperada é DIFERENTE da original!")
    else:
        print("ERRO: Não foi possível recuperar a string inserida.")

    cur.close()

except psycopg2.Error as e:
    print("ERRO de Psycopg2: " + str(e))
    if hasattr(e, 'pgcode'):
        print("Psycopg2 pgcode: " + str(e.pgcode))
    if hasattr(e, 'pgerror'):
        print("Psycopg2 pgerror: " + str(e.pgerror))
    if isinstance(e, psycopg2.OperationalError) and "decode byte" in str(e).lower():
        print("Este é um erro de decodificação Unicode diretamente do psycopg2.")
        if hasattr(e, 'diag') and e.diag:
            diag_message = e.diag.message_primary if e.diag.message_primary else ''
            print("Diagnóstico: " + str(diag_message))
except Exception as e_general:
    print("ERRO Geral: " + str(e_general))
    print("Tipo de erro: " + str(type(e_general).__name__))
finally:
    if conn:
        conn.close()
        print("Conexão fechada.")