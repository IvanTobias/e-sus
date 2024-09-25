#importdados.py
import json
from Conexões import log_message
import os
from apscheduler.triggers.cron import CronTrigger
from Consultas import execute_long_task 
from socketio_config import socketio
import time
from apscheduler.schedulers.background import BackgroundScheduler
from Common import AUTO_UPDATE_CONFIG_FILE, task_event, task_status, update_last_import
# Inicializar o agendador de tarefas
scheduler = BackgroundScheduler()
scheduler.start()

# Função para verificar se uma tarefa está em execução
def is_task_running():
    return any(status == "running" for status in task_status.values())

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
        try:
            with open(AUTO_UPDATE_CONFIG_FILE, 'r') as config_file:
                return json.load(config_file)
        except (json.JSONDecodeError, OSError) as e:  # Lida com erros de leitura ou arquivo corrompido
            log_message(f"Erro ao ler {AUTO_UPDATE_CONFIG_FILE}: {e}. Recriando arquivo com valores padrão.")
            default_config = {
                "isAutoUpdateOn": False,
                "autoUpdateTime": "00:00"
            }
            with open(AUTO_UPDATE_CONFIG_FILE, 'w') as config_file:
                json.dump(default_config, config_file, indent=4)
            return default_config

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

    # Verificar se uma tarefa está rodando no momento antes de agendar
    if is_task_running():
        print("Uma tarefa já está em execução. A nova tarefa não será agendada.")
        return

    scheduler.add_job(auto_update_imports, trigger)

# Função para agendar uma nova importação, aguardando que a anterior finalize
def run_import_sequentially(import_type, config_data):
    while is_task_running():
        print(f"Aguardando conclusão da tarefa anterior... {import_type} em espera.")
        task_event.wait()  # Aguarda até que a tarefa anterior seja finalizada

    task_event.clear()  # Limpa o evento para bloquear outras tarefas enquanto esta está em execução
    try:
        print(f"Parâmetros recebidos para {import_type}: {config_data}")
        
        # Inicia a nova tarefa
        execute_long_task(config_data, import_type)
        
         # Atualizar a última importação no arquivo configimport.json
        update_last_import(import_type)

    except Exception as e:
        print(f"Erro ao executar a tarefa {import_type}: {e}")
    
    finally:
        # Libera o evento após a conclusão ou em caso de erro
        task_event.set()

# Removido BPA da função de auto atualização
def auto_update_imports():
    try:
        # Dados de configuração para as tarefas de importação
        config_data = {}

        # Executar cada importação de forma sequencial, sem BPA
        run_import_sequentially('cadastro', config_data)  # Primeiro, importar cadastro
        run_import_sequentially('domiciliofcd', config_data)  # Depois, importar domiciliofcd
        run_import_sequentially('visitas', config_data)  # Depois, importar visitas
        run_import_sequentially('iaf', config_data)  # Depois, importar iaf
        run_import_sequentially('pse', config_data)  # Depois, importar pse
        run_import_sequentially('pse_prof', config_data)  # Depois, importar pse_prof

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
        'visitas': 'visitas.xlsx',
        'iaf': 'iaf.xlsx',
        'pse': 'pse.xlsx',
        'pse_prof': 'pse_prof.xlsx'
    }
    file_name = file_mapping.get(import_type)
    if not file_name:
        return False

    file_path = os.path.join(os.getcwd(), file_name)  # Caminho absoluto

    try:
        return os.path.exists(file_path)
    except OSError as e:
        print(f"Erro ao acessar o arquivo {file_path}: {e}")
        return False

