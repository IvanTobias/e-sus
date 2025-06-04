import json
import logging
from flask import Blueprint, request, jsonify, current_app

import importdados

logger = logging.getLogger(__name__)
import_config_bp = Blueprint('import_config_bp', __name__)

@import_config_bp.route('/configimport', methods=['GET'])
def get_last_import():
    try:
        # Assuming 'configimport.json' is in the instance folder or a predefined path
        # For simplicity, using current working directory as per original app structure
        with open('configimport.json', 'r') as f:
            config = json.load(f)
        return jsonify(config)
    except FileNotFoundError:
        logger.error("configimport.json não encontrado.")
        return jsonify({"error": "Arquivo de configuração de importação não encontrado."}), 404
    except json.JSONDecodeError:
        logger.error("Erro ao decodificar configimport.json.")
        return jsonify({"error": "Erro ao ler o arquivo de configuração de importação."}), 500


@import_config_bp.route('/api/get-import-config', methods=['GET'])
def get_import_config():
    config_data = importdados.ensure_auto_update_config()
    return jsonify(config_data)

@import_config_bp.route('/api/save-auto-update-config', methods=['POST'])
def save_auto_update_config_route():
    # Using current_app as 'app' is not directly available in blueprints
    with current_app.app_context():
        data = request.json
        is_auto_update_on = data.get('isAutoUpdateOn') # Using .get for safer access
        auto_update_time = data.get('autoUpdateTime')

        if is_auto_update_on is None or auto_update_time is None:
            return jsonify({"status": "error", "message": "Dados incompletos fornecidos."}), 400

        importdados.save_auto_update_config(is_auto_update_on, auto_update_time)

        if is_auto_update_on:
            # Ensure scheduler is accessed correctly via importdados module
            if len(importdados.scheduler.get_jobs()) > 0:
                importdados.scheduler.remove_all_jobs()
            importdados.schedule_auto_import(importdados.scheduler, auto_update_time)
            logger.info(f"Autoatualização agendada para {auto_update_time}")
        else:
            importdados.scheduler.remove_all_jobs()
            logger.info("Autoatualização desativada")

        return jsonify({"status": "Configuração de autoatualização salva com sucesso!"})

@import_config_bp.route('/check-file/<import_type>', methods=['GET'])
def check_file(import_type):
    if importdados.is_file_available(import_type):
        return jsonify({"available": True})
    return jsonify({"available": False})
