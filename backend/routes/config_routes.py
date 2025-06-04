import json
import os
from flask import Blueprint, request, jsonify
# Remove create_engine and text if no longer directly used
# from sqlalchemy import create_engine, text
import logging
from Conexoes import test_db_connection # Import the new function

logger = logging.getLogger(__name__)
config_bp = Blueprint('config_bp', __name__)

@config_bp.route('/save-config', methods=['POST'])
def save_config():
    config_data = request.json
    if not config_data:
        return jsonify({"status": "Erro: Nenhum dado fornecido."}), 400

    # Validate required fields for connection testing
    required_fields = ['username', 'password', 'ip', 'port', 'database']
    missing_fields = [field for field in required_fields if field not in config_data or not config_data[field]]
    if missing_fields:
        # Also check for 'host' if 'ip' is missing, to align with test_db_connection logic
        if 'ip' in missing_fields and ('host' not in config_data or not config_data['host']):
             return jsonify({"status": f"Erro: Campos obrigatórios ausentes para testar conexão: {', '.join(missing_fields)} e host alternativo não fornecido."}), 400
        elif 'ip' in missing_fields and config_data.get('host'):
            pass # Allow proceeding if 'host' is present as an alternative to 'ip'
        else:
            return jsonify({"status": f"Erro: Campos obrigatórios ausentes para testar conexão: {', '.join(missing_fields)}."}), 400


    is_successful, error_message = test_db_connection(config_data)

    if not is_successful:
        logger.error(f"Erro ao testar a conexão antes de salvar: {error_message}")
        return jsonify({"status": f"Erro ao testar a conexão: {error_message}"}), 400 # Return 400 for client error

    try:
        # Assuming config.json is stored in the instance folder or a configurable path
        # For simplicity, using 'config.json' in the current working directory
        with open('config.json', 'w') as config_file:
            json.dump(config_data, config_file)
        logger.info("Configuração salva com sucesso!")
        return jsonify({"status": "Configuração salva com sucesso!"})
    except Exception as e:
        logger.error(f"Erro ao salvar a configuração: {e}")
        return jsonify({"status": f"Erro ao salvar a configuração: {str(e)}"}), 500

@config_bp.route('/get-config', methods=['GET'])
def get_config():
    try:
        # Using 'config.json' in the current working directory
        with open('config.json', 'r') as config_file:
            config_data = json.load(config_file)
    except FileNotFoundError:
        logger.info("Arquivo config.json não encontrado. Retornando configuração padrão.")
        config_data = {"ip": "", "host": "", "port": "", "database": "", "username": "", "password": ""}
    except json.JSONDecodeError:
        logger.error("Erro ao decodificar config.json. Retornando configuração padrão.")
        config_data = {"ip": "", "host": "", "port": "", "database": "", "username": "", "password": ""}
    return jsonify(config_data)

@config_bp.route('/test-connection', methods=['POST'])
def test_connection_route(): # Renamed to avoid conflict with imported function
    config_data = request.json
    if not config_data:
        return jsonify({"message": "Erro: Nenhum dado fornecido."}), 400

    # Validate required fields for connection testing
    required_fields = ['username', 'password', 'ip', 'port', 'database']
    missing_fields = [field for field in required_fields if field not in config_data or not config_data[field]]
    if missing_fields:
        if 'ip' in missing_fields and ('host' not in config_data or not config_data['host']):
             return jsonify({"message": f"Erro: Campos obrigatórios ausentes: {', '.join(missing_fields)} e host alternativo não fornecido."}), 400
        elif 'ip' in missing_fields and config_data.get('host'):
            pass
        else:
            return jsonify({"message": f"Erro: Campos obrigatórios ausentes: {', '.join(missing_fields)}."}), 400

    is_successful, error_message = test_db_connection(config_data)

    if is_successful:
        return jsonify({"message": "Conexão bem-sucedida!"})
    else:
        logger.error(f"Erro na conexão (rota /test-connection): {error_message}")
        return jsonify({"message": f"Erro na conexão: {error_message}"}), 500 # Return 500 for server/connection error
