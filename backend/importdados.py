import json
from datetime import datetime, timedelta
import os
from apscheduler.triggers.cron import CronTrigger
from Consultas import execute_long_task 
from socketio_config import socketio
import time
from apscheduler.schedulers.background import BackgroundScheduler

# Inicializar o agendador de tarefas
scheduler = BackgroundScheduler()
scheduler.start()

# Caminho do arquivo de configuração
AUTO_UPDATE_CONFIG_FILE = 'auto_update_config.json'
IMPORT_CONFIG_FILE = 'import_config.json'  # Novo arquivo para armazenar ano e mês

# Dicionário para armazenar o status de execução de cada tarefa
task_status = {
    "cadastro": "idle",
    "domiciliofcd": "idle",
    "visitas": "idle",
    "bpa": "idle"
}

# Função para verificar se uma tarefa está em execução
def is_task_running():
    return any(status == "running" for status in task_status.values())

# Atualizar o status de uma tarefa
def update_task_status(task, status):
    task_status[task] = status
    print(f"Status da tarefa {task}: {status}")

def update_last_import(import_type):
    if not os.path.exists('configimport.json'):
        with open('configimport.json', 'w') as f:
            json.dump({"cadastro": None, "domiciliofcd": None, "bpa": None, "visitas": None}, f)
        print("Arquivo configimport.json criado com valores padrão.")

    try:
        with open('configimport.json', 'r') as f:
            config = json.load(f)

        config[import_type] = datetime.now().strftime('%H:%M %d-%m-%Y')

        with open('configimport.json', 'w') as f:
            json.dump(config, f, indent=4)
            f.flush()
            os.fsync(f.fileno())
        print(f"Última importação de {import_type} registrada com sucesso.")

    except Exception as e:
        print(f"Erro ao atualizar o arquivo configimport.json: {e}")

def ensure_auto_update_config():
    if not os.path.exists(AUTO_UPDATE_CONFIG_FILE):
        default_config = {
            "isAutoUpdateOn": False,
            "autoUpdateTime": "00:00"  # Horário padrão (meia-noite)
        }
        with open(AUTO_UPDATE_CONFIG_FILE, 'w') as config_file:
            json.dump(default_config, config_file, indent=4)
        return default_config
    else:
        with open(AUTO_UPDATE_CONFIG_FILE, 'r') as config_file:
            return json.load(config_file)

def save_auto_update_config(is_auto_update_on, auto_update_time):
    config_data = {
        "isAutoUpdateOn": is_auto_update_on,
        "autoUpdateTime": auto_update_time
    }
    with open(AUTO_UPDATE_CONFIG_FILE, 'w') as config_file:
        json.dump(config_data, config_file, indent=4)

def schedule_auto_import(scheduler, time_str):
    hour, minute = map(int, time_str.split(':'))
    trigger = CronTrigger(hour=hour, minute=minute)
    scheduler.add_job(auto_update_imports, trigger)

# Função para agendar uma nova importação, aguardando que a anterior finalize
def run_import_sequentially(import_type, config_data):
    # Enquanto outra tarefa estiver em execução, aguarda
    while is_task_running():
        print(f"Aguardando conclusão da tarefa anterior... {import_type} em espera.")
        time.sleep(1)  # Evitar loop intenso, espera 1 segundo antes de checar de novo

    # Não será mais necessário verificar ou formatar ano e mes para o BPA
    print(f"Parâmetros recebidos para {import_type}: {config_data}")

    # Quando não há tarefa em execução, inicia a nova tarefa
    execute_long_task(config_data, import_type)

# Removido BPA da função de auto atualização
def auto_update_imports():
    try:
        # Dados de configuração para as tarefas de importação
        config_data = {}

        # Executar cada importação de forma sequencial, sem BPA
        run_import_sequentially('cadastro', config_data)  # Primeiro, importar cadastro
        run_import_sequentially('domiciliofcd', config_data)  # Depois, importar domiciliofcd
        run_import_sequentially('visitas', config_data)  # Depois, importar visitas

        print("Atualização automática de importação executada com sucesso!")

        # Emitir o progresso via WebSocket (opcional)
        for progress in range(0, 101, 10):
            socketio.emit('progress_update', {'type': 'auto_update', 'progress': progress})
            time.sleep(1)

    except Exception as e:
        print(f"Erro ao realizar a autoatualização: {e}")

def is_file_available(import_type):
    file_mapping = {
        'cadastro': 'cadastros_exportados.xlsx',
        'domiciliofcd': 'domiciliofcd_exportadas.xlsx',
        'bpa': 'bpa.xlsx',
        'visitas': 'visitas.xlsx'
    }
    file_path = file_mapping.get(import_type)
    if file_path and os.path.exists(file_path):
        return True
    return False
