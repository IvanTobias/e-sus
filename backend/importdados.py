import json
from datetime import datetime
import os
from apscheduler.triggers.cron import CronTrigger
import requests
from socketio_config import socketio
import time
from apscheduler.schedulers.background import BackgroundScheduler

# Inicializar o agendador de tarefas
scheduler = BackgroundScheduler()
scheduler.start()

# Caminho do arquivo de configuração
AUTO_UPDATE_CONFIG_FILE = 'auto_update_config.json'

def update_last_import(import_type):
    # Verifica se o arquivo existe, caso contrário, cria um novo
    if not os.path.exists('configimport.json'):
        with open('configimport.json', 'w') as f:
            json.dump({"cadastro": None, "domiciliofcd": None, "bpa": None}, f)
        print("Arquivo configimport.json criado com valores padrão.")

    try:
        # Lê o arquivo JSON atual
        with open('configimport.json', 'r') as f:
            config = json.load(f)

        # Atualiza a timestamp do tipo de importação
        config[import_type] = datetime.now().strftime('%H:%M %d-%m-%Y')

        # Grava o arquivo atualizado
        with open('configimport.json', 'w') as f:
            json.dump(config, f, indent=4)
            f.flush()
            os.fsync(f.fileno())
        print(f"Última importação de {import_type} registrada com sucesso.")

    except Exception as e:
        print(f"Erro ao atualizar o arquivo configimport.json: {e}")

# Função para garantir que o arquivo de configuração exista e tenha valores padrão
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

# Função para salvar a configuração de autoatualização
def save_auto_update_config(is_auto_update_on, auto_update_time):
    config_data = {
        "isAutoUpdateOn": is_auto_update_on,
        "autoUpdateTime": auto_update_time
    }
    with open(AUTO_UPDATE_CONFIG_FILE, 'w') as config_file:
        json.dump(config_data, config_file, indent=4)


# Função para agendar a importação automática no horário definido
def schedule_auto_import(scheduler, time_str):
    hour, minute = map(int, time_str.split(':'))
    trigger = CronTrigger(hour=hour, minute=minute)
    scheduler.add_job(auto_update_imports, trigger)


# Função que realiza as importações de cadastros, domiciliofcd e BPA
def auto_update_imports():
    try:
        # Carregar a configuração existente para saber ano e mês
        with open('config.json', 'r') as config_file:
            config_data = json.load(config_file)
        
        # Dados de configuração (ano e mês) para BPA
        ano = config_data.get('ano')
        mes = config_data.get('mes')

        # Função para disparar cada importação
        def run_import(import_type):
            url = f"http://127.0.0.1:5000/execute-queries/{import_type}"
            data = {}
            if import_type == 'bpa':
                data = {"ano": str(ano), "mes": mes}
            
            # Enviar o request POST para iniciar a importação
            response = requests.post(url, json=data)
            if response.status_code == 200:
                print(f"Importação de {import_type} iniciada com sucesso.")
            else:
                print(f"Erro ao iniciar a importação de {import_type}: {response.text}")
            return response.status_code == 200

        # Importação de cadastros individuais
        if run_import('cadastro'):
            time.sleep(2000)  # Adiciona um pequeno delay entre as requisições
            # Importação de cadastros domiciliares (domiciliofcd)
            if run_import('domiciliofcd'):
                time.sleep(2000)
                # Importação de BPA
                #run_import('bpa')

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
        'bpa': 'bpa.xlsx'
    }
    file_path = file_mapping.get(import_type)
    if file_path and os.path.exists(file_path):
        return True
    return False
