#backend/Conexões.py
from sqlalchemy import create_engine
import json
import os
from datetime import datetime

def get_local_engine():
    local_db_uri = "postgresql+psycopg2://postgres:esus@localhost:5432/esus"
    log_message(f"Conectado ao banco de dados local: {local_db_uri}")
    return create_engine(
        local_db_uri,
        connect_args={'options': '-c client_encoding=utf8'},
        client_encoding='utf8'  # redundante, mas garante fallback
    )

def get_external_engine():
    config = load_config()
    required_keys = ['username', 'password', 'ip', 'port', 'database']
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Chave obrigatória '{key}' não encontrada no config.json")

    external_db_uri = (
        f"postgresql+psycopg2://{config['username']}:{config['password']}"
        f"@{config['ip']}:{config['port']}/{config['database']}"
    )

    # Máscara para não expor a senha no log
    safe_uri = external_db_uri.replace(config['password'], '*****')
    log_message(f"Conectado ao banco de dados externo: {safe_uri}")

    return create_engine(
        external_db_uri,
        connect_args={'client_encoding': 'utf8'}
    )

def load_config():
    config_path = 'config.json'
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Arquivo de configuração '{config_path}' não encontrado.")

    with open(config_path, 'r', encoding='utf-8') as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Erro ao decodificar JSON do arquivo '{config_path}': {e}")
    return config

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"{timestamp} - {message}"
    with open("setup_log.txt", "a", encoding="utf-8") as log_file:
        log_file.write(full_message + "\n")
    print(full_message)
