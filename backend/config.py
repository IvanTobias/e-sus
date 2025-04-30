#backend/config.py
from flask import Flask
from sqlalchemy import create_engine
import json
import os
import logging

app = Flask(__name__)

# Caminho do arquivo de configuração
CONFIG_FILE = 'config.json'

# Variável global para armazenar a configuração do banco externo
external_db_config = {}

# Função para garantir que o arquivo config.json exista e tenha valores padrão se necessário
def ensure_config_exists():
    if not os.path.exists(CONFIG_FILE):
        logging.info("Arquivo config.json não encontrado. Criando um novo com valores padrão.")
        default_config = {
            "ip": "",
            "port": "",
            "database": "",
            "username": "",
            "password": ""
        }
        with open(CONFIG_FILE, 'w') as config_file:
            json.dump(default_config, config_file, indent=4)
        return default_config
    else:
        with open(CONFIG_FILE, 'r') as config_file:
            return json.load(config_file)

# Função para carregar a configuração ao iniciar o backend
def load_config():
    global external_db_config
    external_db_config = ensure_config_exists()

# Função para obter o engine de conexão com base na configuração atual
def get_external_engine():
    db_uri = f"postgresql+psycopg2://{external_db_config['username']}:{external_db_config['password']}@{external_db_config['ip']}:{external_db_config['port']}/{external_db_config['database']}"
    return create_engine(db_uri)


# Inicializa o backend carregando as configurações existentes
if __name__ == '__main__':
    load_config()  # Carrega a configuração ao iniciar o backend
    app.run(debug=True)
