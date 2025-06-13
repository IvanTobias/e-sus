import json
import os
import asyncio
import threading
import traceback
import logging
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler
from Consultas import execute_long_task
from socketio_config import socketio
from Conexoes import log_message # Changed from Conexões
from Common import AUTO_UPDATE_CONFIG_FILE, task_event, update_last_import
from init import app

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




# Função para executar as importações automáticas
def auto_update_imports_wrapper():
    with app.app_context():  # Garante que estamos dentro do contexto da aplicação
        logging.info("Executando autoatualização de importações...")
        # Aqui você pode chamar as funções de importação
        # Por exemplo, para cada tipo de importação:
        tipos = ['cadastro', 'domiciliofcd', 'bpa', 'visitas', 'atendimentos', 'iaf', 'pse', 'pse_prof','fiocruz']
        for tipo in tipos:
            try:
                # Lê a configuração atual
                with open('config.json', 'r') as config_file:
                    config_data = json.load(config_file)
                # Executa a importação
                execute_long_task(config_data, tipo)
            except Exception as e:
                logging.error(f"Erro ao executar autoatualização para {tipo}: {str(e)}")

# Função para agendar a importação automática
def schedule_auto_import(scheduler, auto_update_time):
    try:
        # Divide a string "HH:MM" em horas e minutos
        hour, minute = map(int, auto_update_time.split(':'))
        # Agenda a tarefa para rodar diariamente no horário especificado
        scheduler.add_job(
            auto_update_imports_wrapper,
            'cron',
            hour=hour,
            minute=minute,
            id='auto_update_imports_wrapper'
        )
        logging.info(f"Tarefa de auto-importação agendada para {auto_update_time}.")
    except Exception as e:
        logging.error(f"Erro ao agendar auto-importação: {str(e)}")

# Função para garantir a configuração de autoatualização
def ensure_auto_update_config():
    config_file = 'auto_update_config.json'
    default_config = {'isAutoUpdateOn': False, 'autoUpdateTime': '23:00'}
    if not os.path.exists(config_file):
        with open(config_file, 'w') as f:
            json.dump(default_config, f)
        logging.info("Arquivo de configuração de autoatualização criado com valores padrão.")
    with open(config_file, 'r') as f:
        config = json.load(f)
    logging.info(f"Configuração de auto-update carregada: {config}")
    return config

# Função para salvar a configuração de autoatualização
def save_auto_update_config(is_auto_update_on, auto_update_time):
    config = {'isAutoUpdateOn': is_auto_update_on, 'autoUpdateTime': auto_update_time}
    with open('auto_update_config.json', 'w') as f:
        json.dump(config, f)
    logging.info("Configuração de autoatualização salva.")
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
        import_types = ['cadastro', 'bpa', 'domiciliofcd', 'visitas', 'iaf', 'pse', 'pse_prof']

        for import_type in import_types:
            print(f"Iniciando importação para {import_type}")
            run_import_sequentially(import_type, config_data)
            while not task_event.is_set():
                await asyncio.sleep(1)  # Aguarda a tarefa atual terminar

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
