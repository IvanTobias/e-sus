#backend/banco.py
import pandas as pd
from pandas.api.types import is_object_dtype
from Conexões import get_external_engine, log_message
import unicodedata
# As outras funções do banco.py ficam aqui


# Variáveis globais para rastreamento de progresso
progress = {}
cancel_requests = {}

def log_message(message):
    with open("setup_log.txt", "a") as log_file:
        log_file.write(message + "\n")
    print(message)  # Também imprime a mensagem no console, se necessário

def rename_duplicate_columns(df):
    columns = pd.Series(df.columns)
    for dup in columns[columns.duplicated()].unique():
        duplicates = columns[columns == dup].index
        for i, index in enumerate(duplicates):
            columns[index] = f"{dup}_{i + 1}"
    df.columns = columns
    return df

def clean_dataframe(df):
    df.columns = [col.replace(' ', '_').replace('-', '_').lower() for col in df.columns]
    
    def clean_text(x):
        if isinstance(x, str):
            # Normaliza para formato Unicode NFC (padrão de acentuação correto)
            x = unicodedata.normalize('NFC', x)
            # Remove espaços duplicados
            x = ' '.join(x.split())
        return x

    # Aplica limpeza apenas nos textos (colunas object)
    for col in df.columns:
        if is_object_dtype(df[col]):
            df[col] = df[col].apply(clean_text)

    df = rename_duplicate_columns(df)
    return df

def cancel_import(tipo):
    global cancel_requests
    cancel_requests[tipo] = True
    log_message(f"Cancelamento solicitado para {tipo}.")

def cancel_query(conn, tipo, db_type):
    if db_type == "postgresql":
        query_to_cancel = f"""
            SELECT pg_cancel_backend(pid)
            FROM pg_stat_activity
            WHERE datname = current_database() AND query LIKE '%{tipo}%' AND state = 'active';
        """
        conn.execute(query_to_cancel)
        log_message(f"Consulta para {tipo} cancelada no PostgreSQL.")
    elif db_type == "oracle":
        query_to_cancel = f"""
            SELECT 'ALTER SYSTEM KILL SESSION ''' || sid || ',' || serial# || ''' IMMEDIATE' AS kill_session
            FROM v$session
            WHERE username = user AND status = 'ACTIVE' AND sql_id IN
                (SELECT sql_id FROM v$sql WHERE sql_text LIKE '%{tipo}%');
        """
        kill_command = conn.execute(query_to_cancel).fetchone()[0]
        conn.execute(kill_command)
        log_message(f"Consulta para {tipo} cancelada no Oracle.")

def fetch_bpa_data(query):
    engine = get_external_engine()
    with engine.connect() as connection:
        result = connection.execute(query)
        return result.fetchall()

