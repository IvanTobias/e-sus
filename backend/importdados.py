import json
import os
import asyncio
import redis
import traceback
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler
from Consultas import execute_long_task
from socketio_config import socketio
from Conexões import log_message
from Common import AUTO_UPDATE_CONFIG_FILE, task_event, task_status, update_last_import


# Inicializar o cliente Redis para controlar tarefas distribuídas
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Inicializar o agendador de tarefas
scheduler = BackgroundScheduler()
scheduler.start()

# Classe utilitária para gerenciamento de arquivos
class FileManager:
    def __init__(self, base_dir=os.getcwd()):
        self.base_dir = base_dir

    def is_file_available(self, file_name):
        file_path = os.path.join(self.base_dir, file_name)
        try:
            return os.path.exists(file_path)
        except OSError as e:
            print(f"Erro ao acessar o arquivo {file_path}: {e}")
            return False

file_manager = FileManager()

# Função para verificar se uma tarefa está em execução (usando Redis para múltiplas instâncias)
def acquire_lock(lock_name, timeout=5):
    return redis_client.set(lock_name, "locked", ex=timeout, nx=True)

def release_lock(lock_name):
    redis_client.delete(lock_name)

# Função para verificar se o arquivo de configuração de auto-update existe
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
        except (json.JSONDecodeError, OSError) as e:
            log_message(f"Erro ao ler {AUTO_UPDATE_CONFIG_FILE}: {e}. Recriando arquivo com valores padrão.")
            default_config = {
                "isAutoUpdateOn": False,
                "autoUpdateTime": "00:00"
            }
            with open(AUTO_UPDATE_CONFIG_FILE, 'w') as config_file:
                json.dump(default_config, config_file, indent=4)
            return default_config

# Função para salvar a configuração de auto-update
def save_auto_update_config(is_auto_update_on, auto_update_time):
    config_data = {
        "isAutoUpdateOn": is_auto_update_on,
        "autoUpdateTime": auto_update_time
    }
    with open(AUTO_UPDATE_CONFIG_FILE, 'w') as config_file:
        json.dump(config_data, config_file, indent=4)

# Função para agendar o auto-update
def schedule_auto_import(scheduler, time_str):
    hour, minute = map(int, time_str.split(':'))
    trigger = CronTrigger(hour=hour, minute=minute)

    # Verifica se uma tarefa já está agendada
    if not scheduler.get_job("auto_import"):
        scheduler.add_job(auto_update_imports, trigger, id="auto_import")
    else:
        print("Tarefa de auto importação já agendada.")

# Função para rodar tarefas de importação sequencialmente
def run_import_sequentially(import_type, config_data):
    lock_name = f"import_lock_{import_type}"

    # Tenta adquirir o lock para garantir que apenas uma tarefa seja executada
    if acquire_lock(lock_name):
        task_event.clear()  # Bloqueia novas tarefas até que esta seja finalizada
        try:
            print(f"Iniciando tarefa {import_type}")
            execute_long_task(config_data, import_type)
            update_last_import(import_type)
        except Exception as e:
            error_message = traceback.format_exc()
            log_message(f"Erro na tarefa {import_type}: {error_message}")
        finally:
            release_lock(lock_name)
            task_event.set()  # Libera para permitir novas tarefas
    else:
        print(f"Tarefa {import_type} já está em execução.")

    # Função de autoatualização que emite progresso via WebSocket
async def auto_update_imports():
    try:
        config_data = {}
        import_types = ['cadastro', 'domiciliofcd', 'visitas', 'iaf', 'pse', 'pse_prof']

        for import_type in import_types:
            run_import_sequentially(import_type, config_data)
            for progress in range(0, 101, 10):
                socketio.emit('progress_update', {'type': 'auto_update', 'progress': progress})
                print(f"Emitiu progresso: {progress}% para {import_type}")                
                await asyncio.sleep(1)  # Espera não-bloqueante

        print("Atualização automática de importação executada com sucesso!")

    except Exception as e:
        error_message = traceback.format_exc()
        log_message(f"Erro ao realizar a autoatualização: {error_message}")

# Função para verificar se um arquivo necessário está disponível
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

    return file_manager.is_file_available(file_name)

# Exemplo de uso da função schedule_auto_import
auto_update_config = ensure_auto_update_config()
if auto_update_config['isAutoUpdateOn']:
    schedule_auto_import(scheduler, auto_update_config['autoUpdateTime'])
