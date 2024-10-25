import json
import os
import asyncio
import threading
import traceback
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler
from Consultas import execute_long_task
from socketio_config import socketio
from Conexões import log_message
from Common import AUTO_UPDATE_CONFIG_FILE, task_event, task_status, update_last_import

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

# Dicionário para manter os locks locais
locks = {}
lock_dict_lock = threading.Lock()

# Função para adquirir lock
def acquire_lock(lock_name, timeout=5):
    with lock_dict_lock:  # Protege o acesso ao dicionário de locks
        if lock_name not in locks:
            locks[lock_name] = threading.Lock()

    lock_acquired = locks[lock_name].acquire(timeout=timeout)
    print(f"Lock para {lock_name}: {'Adquirido' if lock_acquired else 'Não adquirido'}")
    return lock_acquired

# Função para liberar lock
def release_lock(lock_name):
    with lock_dict_lock:
        if lock_name in locks:
            locks[lock_name].release()
            print(f"Lock para {lock_name} liberado.")

# Função para verificar se o arquivo de configuração de auto-update existe
def ensure_auto_update_config():
    if not os.path.exists(AUTO_UPDATE_CONFIG_FILE):
        default_config = {
            "isAutoUpdateOn": False,
            "autoUpdateTime": "00:00"  # Horário padrão (meia-noite)
        }
        with open(AUTO_UPDATE_CONFIG_FILE, 'w') as config_file:
            json.dump(default_config, config_file, indent=4)
        print(f"Arquivo de configuração {AUTO_UPDATE_CONFIG_FILE} criado com valores padrão.")
        return default_config
    else:
        try:
            with open(AUTO_UPDATE_CONFIG_FILE, 'r') as config_file:
                config = json.load(config_file)
                print(f"Configuração de auto-update carregada: {config}")
                return config
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
    print(f"Configuração de auto-update salva: {config_data}")

# Função para rodar tarefas de importação de forma assíncrona
def auto_update_imports_wrapper():
    asyncio.run(auto_update_imports())  # Usando asyncio.run para executar a coroutine

# Função para agendar o auto-update
def schedule_auto_import(scheduler, time_str):
    hour, minute = map(int, time_str.split(':'))
    trigger = CronTrigger(hour=hour, minute=minute)

    # Verifica se uma tarefa já está agendada
    if not scheduler.get_job("auto_import"):
        scheduler.add_job(auto_update_imports_wrapper, trigger, id="auto_import")
        print(f"Tarefa de auto-importação agendada para {time_str}.")
    else:
        print("Tarefa de auto importação já agendada.")

# Função para rodar tarefas de importação sequencialmente
def run_import_sequentially(import_type, config_data):
    lock_name = f"import_lock_{import_type}"

    if acquire_lock(lock_name):
        print(f"Lock adquirido para {import_type}")
        task_event.clear()  # Bloqueia novas tarefas até que esta seja finalizada
        try:
            print(f"Iniciando tarefa {import_type}")
            execute_long_task(config_data, import_type)
            update_last_import(import_type)
            print(f"Tarefa {import_type} concluída")
        except Exception as e:
            error_message = traceback.format_exc()
            log_message(f"Erro na tarefa {import_type}: {error_message}")
        finally:
            release_lock(lock_name)
            print(f"Lock liberado para {import_type}")
            task_event.set()  # Libera para permitir novas tarefas
    else:
        print(f"Tarefa {import_type} já está em execução.")

# Função de autoatualização que emite progresso via WebSocket
async def auto_update_imports():
    try:
        config_data = {}
        import_types = ['cadastro', 'domiciliofcd', 'visitas', 'iaf', 'pse', 'pse_prof']

        for import_type in import_types:
            print(f"Iniciando importação para {import_type}")
            
            # Executa a tarefa de importação sequencialmente
            run_import_sequentially(import_type, config_data)

            # Simula progresso e envia atualização via WebSocket
            for progress in range(0, 101, 10):
                socketio.emit('progress_update', {'type': 'auto_update', 'progress': progress})
                print(f"Progresso {progress}% emitido para {import_type}")
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
        print(f"Tipo de importação desconhecido: {import_type}")
        return False

    file_available = file_manager.is_file_available(file_name)
    print(f"Arquivo {file_name} disponível para {import_type}: {file_available}")
    return file_available

# Exemplo de uso da função schedule_auto_import
auto_update_config = ensure_auto_update_config()
if auto_update_config['isAutoUpdateOn']:
    schedule_auto_import(scheduler, auto_update_config['autoUpdateTime'])
else:
    print("Auto-atualização está desativada.")
