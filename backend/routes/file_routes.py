from flask import Blueprint, request, jsonify, send_file, send_from_directory, make_response
import os
import threading
import pandas as pd
import traceback
import logging

# Imports from the project structure (..)
from ..Common import task_status, update_task_status
# log_message is in Consultas according to current app.py, not Common
from ..Consultas import progress, progress_lock, update_progress_safely, log_message
from ..Conexões import get_local_engine
from ..socketio_config import task_clients, emit_end_task, emit_progress, emit_start_task
# Removed 'socketio' as it's not directly used by these functions, only its methods via socketio_config

file_bp = Blueprint('file_api', __name__, url_prefix='/api')
logger = logging.getLogger(__name__) # Local logger for this blueprint

# Helper function export_task
def export_task(tipo, query, filename, sid):
    try:
        logger.info(f"[EXPORT-{tipo.upper()}] Iniciando exportação para {tipo} para sid={sid}")
        emit_start_task(tipo, sid)
        update_task_status(tipo, "running")
        with progress_lock:
            progress[tipo] = 0
        update_progress_safely(tipo, 0)

        logger.info(f"[EXPORT-{tipo.upper()}] Enviando progresso inicial (0%) para sid={sid}")
        emit_progress(tipo, 0, sid)

        update_progress_safely(tipo, 25)
        logger.info(f"[EXPORT-{tipo.upper()}] Enviando progresso (25%) para sid={sid}")
        emit_progress(tipo, 25, sid)

        engine = get_local_engine()
        logger.info(f"[EXPORT-{tipo.upper()}] Conectado ao banco de dados, executando query: {query}")
        df = pd.read_sql(query, engine)
        logger.info(f"[EXPORT-{tipo.upper()}] Query executada, {len(df)} linhas retornadas")

        update_progress_safely(tipo, 50)
        logger.info(f"[EXPORT-{tipo.upper()}] Enviando progresso (50%) para sid={sid}")
        emit_progress(tipo, 50, sid)

        # Ensure os.getcwd() provides the correct base path for file saving.
        # This assumes files are saved relative to where app.py is run.
        filepath = os.path.join(os.getcwd(), filename)
        logger.info(f"[EXPORT-{tipo.upper()}] Salvando arquivo em {filepath}")
        df.to_excel(filepath, index=False)
        logger.info(f"[EXPORT-{tipo.upper()}] Arquivo salvo com sucesso")

        update_progress_safely(tipo, 100)
        logger.info(f"[EXPORT-{tipo.upper()}] Enviando progresso final (100%) para sid={sid}")
        emit_progress(tipo, 100, sid)

        update_task_status(tipo, "completed")
        log_message(f"Exportação de {tipo} concluída com sucesso.") # log_message from ..Consultas
    except Exception as e:
        error_message = traceback.format_exc()
        log_message(f"Erro na exportação de {tipo}: {error_message}") # log_message from ..Consultas
        update_task_status(tipo, "failed")
        update_progress_safely(tipo, 0, error=str(e))
        logger.error(f"[EXPORT-{tipo.upper()}] Erro durante exportação: {str(e)}")
        emit_progress(tipo, 0, sid, error=str(e))
    finally:
        emit_end_task(tipo, sid)
        logger.info(f"[EXPORT-{tipo.upper()}] Finalizando tarefa para sid={sid}")

# Routes
@file_bp.route('/download-results', methods=['GET'])
def download_results():
    filename = request.args.get('file')
    # This assumes the filename includes any necessary path or is in the current working directory.
    # For security, ensure filename is sanitized or points to a designated downloads folder.
    return send_file(filename, as_attachment=True)

@file_bp.route('/export/<tipo>', methods=['GET'])
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

    if task_status.get(tipo) == "running":
        logger.info(f"[EXPORT-{tipo.upper()}] Tarefa para {tipo} já está em execução.")
        return jsonify({"status": "error", "message": f"Exportação {tipo} já está em execução."}), 400

    sid = task_clients.get(tipo)
    if not sid:
        logger.warning(f"[EXPORT-{tipo.upper()}] Nenhum cliente associado à tarefa {tipo}. Usando emissão global.")
        # sid = None # No need to set to None explicitly if that's the default for get() or if emit functions handle None
    
    config = export_configs[tipo]
    logger.info(f"[EXPORT-{tipo.upper()}] Iniciando thread de exportação para tipo={tipo}, sid={sid}")
    # Note: export_task is now defined in this file.
    threading.Thread(target=export_task, args=(tipo, config["query"], config["filename"], sid)).start()
    return jsonify({"status": "started", "message": "Exportação iniciada com sucesso."})

@file_bp.route('/download-exported-file/<tipo>', methods=['GET'])
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

    current_status = task_status.get(tipo)
    if current_status == "running":
        return jsonify({"status": "error", "message": "Exportação ainda em andamento. Aguarde a conclusão."}), 400
    elif current_status == "failed":
        return jsonify({"status": "error", "message": "Exportação falhou. Verifique os logs para mais detalhes."}), 400
    elif current_status != "completed":
        return jsonify({"status": "error", "message": "Nenhuma exportação concluída para este tipo."}), 400

    try:
        filename = export_configs[tipo]
        # This assumes files are in os.getcwd() - the main app execution directory
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

@file_bp.route('/list-bpa-files', methods=['GET'])
def list_bpa_files():
    files = []
    try:
        # Assumes BPA files are in the root directory (os.getcwd())
        for filename in os.listdir('.'): 
            if filename.startswith('bpa_') and filename.endswith('.txt'):
                files.append(filename)
        return jsonify({"files": files})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@file_bp.route('/delete-bpa-file', methods=['DELETE'])
def delete_bpa_file():
    filename = request.args.get('filename')
    try:
        # Assumes BPA files are in the root directory (os.getcwd())
        file_path = os.path.join(os.getcwd(), filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            return jsonify({"message": f"Arquivo {filename} deletado com sucesso!"})
        else:
            return jsonify({"message": "Arquivo não encontrado."}), 404
    except Exception as e:
        return jsonify({"message": f"Erro ao deletar arquivo: {str(e)}"}), 500

@file_bp.route('/download-bpa-file', methods=['GET'])
def download_bpa_file():
    filename = request.args.get('filename')
    try:
        # Assumes BPA files are in the root directory (os.getcwd())
        return send_from_directory(os.getcwd(), filename, as_attachment=True)
    except Exception as e:
        return jsonify({"message": f"Erro ao baixar o arquivo: {str(e)}"}), 500
