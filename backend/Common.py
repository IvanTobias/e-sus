#Common.py
import json
import os
import datetime
from threading import Event

# Caminho do arquivo de configuração
AUTO_UPDATE_CONFIG_FILE = 'auto_update_config.json'
IMPORT_CONFIG_FILE = 'import_config.json'  # Novo arquivo para armazenar ano e mês


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

def update_last_import(import_type):
    config_file = 'configimport.json'
    
    # Se o arquivo não existir, criar com uma estrutura básica
    if not os.path.exists(config_file):
        with open(config_file, 'w') as f:
            json.dump({
                "cadastro": None, 
                "domiciliofcd": None, 
                "bpa": None, 
                "visitas": None, 
                "iaf": None, 
                "pse": None, 
                "pse_prof": None
            }, f)
        print("Arquivo configimport.json criado com valores padrão.")
    
    try:
        # Abrir o arquivo e carregar os dados
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Atualizar o tipo de importação com a data/hora atual
        config[import_type] = datetime.datetime.now().strftime('%H:%M %d-%m-%Y')

        # Escrever de volta no arquivo
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=4)
            f.flush()
            os.fsync(f.fileno())  # Garante que os dados sejam salvos no disco
        print(f"Última importação de {import_type} registrada com sucesso.")

    except Exception as e:
        print(f"Erro ao atualizar o arquivo configimport.json: {e}")

def update_task_status(task, status):
    if task in task_status:
        task_status[task] = status
        print(f"Status da tarefa {task}: {status}")
    else:
        print(f"Tarefa {task} desconhecida. Status não atualizado.")
