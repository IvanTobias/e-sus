import os
import json
import threading
# import traceback # Moved to services.py
import logging
from flask import Blueprint, request, jsonify, send_file, make_response
# import pandas as pd # Moved to services.py
# SQLAlchemyError might not be directly used here if database interactions are in Common/Consultas
# from sqlalchemy.exc import SQLAlchemyError

# Assuming these are still relevant or will be moved appropriately
from Common import task_status, update_task_status
from Conexoes import get_local_engine
from Consultas import (
    send_progress_update,
    execute_long_task,
    get_progress
    # progress, progress_lock, update_progress_safely, log_message are used by Consultas/services
)
# emit_start_task, emit_progress, emit_end_task are used by services or Consultas
from socketio_config import task_clients # Retain for export_data route
from services import export_task # Import the moved function

logger = logging.getLogger(__name__)
task_bp = Blueprint('task_bp', __name__, url_prefix='/api') # Adding /api prefix here

@task_bp.route('/download-results')
def download_results():
    filename = request.args.get('file')
    # Ensure the filepath is secure and within an expected directory
    # For now, assuming it's relative to the current working directory as in the original app.py
    # Consider adding path validation/sanitization here for security
    return send_file(filename, as_attachment=True)

@task_bp.route('/query-progress') # This route was in app.py, seems related to tasks
def query_progress():
    # This might need to be more sophisticated or integrated with get_progress(tipo)
    return jsonify({"progress": 100})

@task_bp.route('/execute-queries/<tipo>', methods=['GET', 'POST'])
def execute_queries(tipo):
    if tipo not in ['cadastro', 'domiciliofcd', 'bpa', 'visitas', 'atendimentos', 'iaf', 'pse', 'pse_prof']:
        logger.error(f"Tipo desconhecido: {tipo}")
        return jsonify({"status": "error", "message": "Tipo desconhecido."})

    if task_status.get(tipo) == "running":
        logger.warning(f"Consultas {tipo} já estão em execução.")
        return jsonify({"status": "error", "message": f"Consultas {tipo} já estão em execução."})

    logger.info(f"[API] Iniciando tarefa para tipo={tipo}")
    send_progress_update(tipo, 0) # This seems to interact with global 'progress' dict

    try:
        # Assuming config.json is still in the root directory relative to where app.py runs
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

    # execute_long_task is imported from Consultas
    thread = threading.Thread(target=execute_long_task, args=(config_data, tipo))
    thread.start()
    logger.info(f"Tarefa para tipo={tipo} iniciada em uma thread.")

    return jsonify({"status": "success", "message": f"Consultas {tipo} em execução."})

@task_bp.route('/progress/<tipo>', methods=['GET'])
def get_progress_endpoint(tipo):
    # get_progress is imported from Consultas and uses the global 'progress' dict
    current_progress = get_progress(tipo)
    response = make_response(jsonify({"progress": current_progress}))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    return response

@task_bp.route('/task-status/<tipo>', methods=['GET'])
def get_task_status_endpoint(tipo):
    logger.info(f"[TASK-STATUS] Acessando task_status para tipo={tipo}, task_status atual: {task_status}")
    if tipo not in task_status: # task_status imported from Common
        logger.info(f"[TASK-STATUS] Tipo {tipo} não encontrado em task_status")
        return jsonify({"status": "unknown"}), 404
    status = task_status.get(tipo, "unknown")
    if not isinstance(status, str):
        logger.error(f"[TASK-STATUS] Status inválido para {tipo}: {status}. Convertendo para string.")
        status = str(status)
    response = {"status": status}
    logger.info(f"[TASK-STATUS] Retornando status para {tipo}: {response}")
    return jsonify(response)

# export_task function was moved to backend/services.py

@task_bp.route('/export/<tipo>', methods=['GET'])
def export_data(tipo):
    export_configs = {
        "cadastro": {"query": "SELECT * FROM tb_cadastro", "filename": "cadastros_exportados.xlsx"},
        "domiciliofcd": {"query": "SELECT * FROM tb_domicilio", "filename": "domiciliofcd_exportadas.xlsx"},
        "visitas": {"query": "SELECT * FROM tb_visitas", "filename": "visitas_exportadas.xlsx"},
        "atendimentos": {"query": "SELECT * FROM tb_atendimentos", "filename": "atendimentos_exportadas.xlsx"},
        "bpa": {"query": "SELECT * FROM tb_bpa", "filename": "bpa.xlsx"},
        "iaf": {"query": "SELECT * FROM tb_iaf", "filename": "iaf.xlsx"},
        "pse": {"query": "SELECT * FROM tb_pse", "filename": "pse.xlsx"},
        "pse_prof": {"query": "SELECT * FROM tb_pse_prof", "filename": "pse_prof.xlsx"}
    }

    if tipo not in export_configs:
        logger.error(f"[EXPORT-{tipo.upper()}] Tipo não suportado: {tipo}")
        return jsonify({"status": "error", "message": f"Tipo {tipo} não suportado."}), 400

    if task_status.get(tipo) == "running": # task_status from Common
        logger.info(f"[EXPORT-{tipo.upper()}] Tarefa para {tipo} já está em execução.")
        return jsonify({"status": "error", "message": f"Exportação {tipo} já está em execução."}), 400

    sid = task_clients.get(tipo) # task_clients from socketio_config
    if not sid:
        logger.warning(f"[EXPORT-{tipo.upper()}] Nenhum cliente associado à tarefa {tipo}. Usando emissão global.")
        sid = None

    config = export_configs[tipo]
    logger.info(f"[EXPORT-{tipo.upper()}] Iniciando thread de exportação para tipo={tipo}, sid={sid}")
    # Calls the local export_task helper function
    threading.Thread(target=export_task, args=(tipo, config["query"], config["filename"], sid)).start()
    return jsonify({"status": "started", "message": "Exportação iniciada com sucesso."})

@task_bp.route('/download-exported-file/<tipo>', methods=['GET'])
def download_exported_file(tipo):
    export_configs = {
        "cadastro": "cadastros_exportados.xlsx",
        "domiciliofcd": "domiciliofcd_exportadas.xlsx",
        "visitas": "visitas_exportadas.xlsx",
        "atendimentos": "atendimentos_exportadas.xlsx",
        "bpa": "bpa.xlsx",
        "iaf": "iaf.xlsx",
        "pse": "pse.xlsx",
        "pse_prof": "pse_prof.xlsx"
    }

    if tipo not in export_configs:
        return jsonify({"status": "error", "message": f"Tipo {tipo} não suportado."}), 400

    current_status = task_status.get(tipo) # task_status from Common
    if current_status == "running":
        return jsonify({"status": "error", "message": "Exportação ainda em andamento. Aguarde a conclusão."}), 400
    elif current_status == "failed":
        return jsonify({"status": "error", "message": "Exportação falhou. Verifique os logs para mais detalhes."}), 400
    elif current_status != "completed":
        return jsonify({"status": "error", "message": "Nenhuma exportação concluída para este tipo."}), 400

    try:
        filename = export_configs[tipo]
        # Assuming os.getcwd() gives the root directory where app.py runs
        filepath = os.path.join(os.getcwd(), filename)
        if not os.path.exists(filepath):
            return jsonify({"status": "error", "message": "Arquivo não encontrado. Tente exportar novamente."}), 404

        response = make_response(send_file(filepath, as_attachment=True))
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        return response
    except Exception as e:
        logger.error(f"[DOWNLOAD-{tipo.upper()}] Erro ao baixar o arquivo: {str(e)}")
        return jsonify({"status": "error", "message": f"Erro ao baixar o arquivo: {str(e)}"}), 500
