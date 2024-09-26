import json
import os
import datetime
import logging
import traceback
from threading import Event

# Caminho do arquivo de configuração
AUTO_UPDATE_CONFIG_FILE = 'auto_update_config.json'
IMPORT_CONFIG_FILE = 'import_config.json'

# Evento para bloquear a execução enquanto uma tarefa está em andamento
task_event = Event()

task_status = {
    "cadastro": "idle",
    "domiciliofcd": "idle",
    "visitas": "idle",
    "bpa": "idle",
    "iaf": "idle",
    "pse": "idle",
    "pse_prof": "idle"
}

# Configuração de logging para monitoramento
logging.basicConfig(filename='import_tasks.log', level=logging.INFO)

# Cache para configimport.json
_config_cache = None

def load_config():
    global _config_cache
    if _config_cache is None:  # Só carrega o arquivo uma vez
        if os.path.exists('configimport.json'):
            with open('configimport.json', 'r') as f:
                _config_cache = json.load(f)
        else:
            _config_cache = {
                "cadastro": None, 
                "domiciliofcd": None, 
                "bpa": None, 
                "visitas": None, 
                "iaf": None, 
                "pse": None, 
                "pse_prof": None
            }
            print("Arquivo configimport.json criado com valores padrão.")
    return _config_cache

def save_config():
    global _config_cache
    with open('configimport.json', 'w') as f:
        json.dump(_config_cache, f, indent=4)
        f.flush()
        os.fsync(f.fileno())  # Garante que os dados sejam salvos no disco

def update_last_import(import_type):
    try:
        config = load_config()
        config[import_type] = datetime.datetime.now().strftime('%H:%M %d-%m-%Y')
        save_config()
        print(f"Última importação de {import_type} registrada com sucesso.")
    except Exception as e:
        error_message = traceback.format_exc()
        logging.error(f"Erro ao atualizar o arquivo configimport.json: {error_message}")

def update_task_status(task, status):
    if task in task_status:
        task_status[task] = status
        logging.info(f"{datetime.datetime.now()}: Status da tarefa {task}: {status}")
    else:
        logging.warning(f"{datetime.datetime.now()}: Tarefa {task} desconhecida. Status não atualizado.")
