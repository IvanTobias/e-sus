#Conexões.py
from sqlalchemy import create_engine
import json

def get_local_engine():
    local_db_uri = "postgresql+psycopg2://postgres:esus@localhost:5432/esus"
    log_message(f"Conectado ao banco de dados local: {local_db_uri}")
    # Adicionando parâmetros de codificação
    return create_engine(local_db_uri, connect_args={'options': '-c client_encoding=utf8'})

def get_external_engine():
    config = load_config()
    external_db_uri = f"postgresql://{config['username']}:{config['password']}@{config['ip']}:{config['port']}/{config['database']}"
    log_message(f"Conectado ao banco de dados externo: {external_db_uri}")
    # Adicionando parâmetros de codificação
    return create_engine(external_db_uri, connect_args={'options': '-c client_encoding=utf8'})

def load_config():
    with open('config.json', 'r') as f:
        config = json.load(f)
    return config

def log_message(message):
    with open("setup_log.txt", "a") as log_file:
        log_file.write(message + "\n")
    print(message)  # Também imprime a mensagem no console, se necessário

