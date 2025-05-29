from flask import Blueprint, request, jsonify, make_response
import threading
import json
import logging

# Assuming app is in the parent directory (backend) and can be imported using ..
# init.py in backend should make 'app' available
# from .. import app # app is not directly used in these routes, but context might be needed. Let's see.
                     # logger is also not directly used by the moved functions, they use the global logger from app.py
                     # For now, let's get the logger instance here.

# Assuming Consultas and Common are in the parent directory or accessible from there
from ..Common import task_status # update_task_status is not used by the moved routes
from ..Consultas import send_progress_update, execute_long_task, get_progress

query_bp = Blueprint('query_api', __name__, url_prefix='/api')
logger = logging.getLogger(__name__) # Get a logger instance for this blueprint

@query_bp.route('/query-progress', methods=['GET'])
def query_progress():
    # This route seems to be a placeholder or for a different progress mechanism
    # as execute_queries uses send_progress_update and get_progress_endpoint uses get_progress
    return jsonify({"progress": 100})

@query_bp.route('/execute-queries/<tipo>', methods=['GET', 'POST'])
def execute_queries(tipo):
    # Note: This logger is local to query_bp. If global app.logger is preferred, it needs to be imported.
    # For now, using local logger.
    if tipo not in ['cadastro', 'domiciliofcd', 'bpa', 'visitas', 'atendimentos', 'iaf', 'pse', 'pse_prof']:
        logger.error(f"Tipo desconhecido: {tipo}")
        return jsonify({"status": "error", "message": "Tipo desconhecido."})

    if task_status.get(tipo) == "running":
        logger.warning(f"Consultas {tipo} já estão em execução.")
        return jsonify({"status": "error", "message": f"Consultas {tipo} já estão em execução."})

    logger.info(f"[API] Iniciando tarefa para tipo={tipo}")
    send_progress_update(tipo, 0) # This function needs to be checked if it uses the global logger or its own

    try:
        # Assuming config.json is at the root of the application, where app.py is.
        # If this code runs from backend/routes, the path needs to be ../../config.json or an absolute path.
        # For now, let's assume 'config.json' is accessible from the current working directory of the app.
        # This might need adjustment if the working directory for blueprint routes is different.
        with open('config.json', 'r') as config_file:
            config_data = json.load(config_file)
    except FileNotFoundError:
        logger.error("Arquivo config.json não encontrado.")
        return jsonify({"status": "error", "message": "Arquivo de configuração não encontrado."}), 500
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar config.json: {e}")
        return jsonify({"status": "error", "message": "Erro ao ler o arquivo de configuração."}), 500

    if request.method == 'POST':
        incoming_data = request.json
        if incoming_data is None:
            logger.error("Nenhum dado JSON recebido na requisição POST.")
            return jsonify({"status": "error", "message": "Nenhum dado JSON fornecido."}), 400
        if 'ano' in incoming_data:
            config_data['ano'] = incoming_data['ano']
        if 'mes' in incoming_data:
            config_data['mes'] = incoming_data['mes']

    # execute_long_task is imported from ..Consultas
    thread = threading.Thread(target=execute_long_task, args=(config_data, tipo))
    thread.start()
    logger.info(f"Tarefa para tipo={tipo} iniciada em uma thread.")

    return jsonify({"status": "success", "message": f"Consultas {tipo} em execução."})

@query_bp.route('/progress/<tipo>', methods=['GET'])
def get_progress_endpoint(tipo):
    # get_progress is imported from ..Consultas
    current_progress = get_progress(tipo) # Renamed variable to avoid conflict with imported progress
    response = make_response(jsonify({"progress": current_progress}))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    return response

@query_bp.route('/task-status/<tipo>', methods=['GET'])
def get_task_status_endpoint(tipo):
    logger.info(f"[TASK-STATUS] Acessando task_status para tipo={tipo}, task_status atual: {task_status}")
    if tipo not in task_status:
        logger.info(f"[TASK-STATUS] Tipo {tipo} não encontrado em task_status")
        return jsonify({"status": "unknown"}), 404
    status = task_status.get(tipo, "unknown")
    if not isinstance(status, str):
        # This check and conversion might be better inside the task_status mechanism itself if possible
        logger.error(f"[TASK-STATUS] Status inválido para {tipo}: {status}. Convertendo para string.")
        status = str(status) 
    response = {"status": status}
    logger.info(f"[TASK-STATUS] Retornando status para {tipo}: {response}")
    return jsonify(response)
