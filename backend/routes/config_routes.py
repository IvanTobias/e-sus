from flask import Blueprint, request, jsonify
from sqlalchemy import create_engine, text
import json
import os
import logging

# Assuming app and importdados are in the parent directory (backend)
# and can be imported using ..
# init.py in backend should make 'app' available
from .. import app 
from .. import importdados

config_bp = Blueprint('config_api', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

@config_bp.route('/save-config', methods=['POST'])
def save_config():
    config_data = request.json
    try:
        engine = create_engine(
            f"postgresql://{config_data['username']}:{config_data['password']}@{config_data['ip']}:{config_data['port']}/{config_data['database']}",
            connect_args={'options': '-c client_encoding=utf8'})
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))
        with open('config.json', 'w') as config_file:
            json.dump(config_data, config_file)
        return jsonify({"status": "Configuração salva com sucesso!"})
    except Exception as e:
        logger.error(f"Erro ao testar a conexão: {e}")
        return jsonify({"status": f"Erro ao testar a conexão: {str(e)}"})

@config_bp.route('/get-config', methods=['GET'])
def get_config():
    try:
        with open('config.json', 'r') as config_file:
            config_data = json.load(config_file)
    except FileNotFoundError:
        config_data = {"ip": "", "port": "", "database": "", "username": "", "password": ""}
    return jsonify(config_data)

@config_bp.route('/test-connection', methods=['POST'])
def test_connection():
    config_data = request.json
    try:
        engine = create_engine(
            f"postgresql://{config_data['username']}:{config_data['password']}@{config_data['ip']}:{config_data['port']}/{config_data['database']}",
            connect_args={'options': '-c client_encoding=utf8'})
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))
        return jsonify({"message": "Conexão bem-sucedida!"})
    except Exception as e:
        logger.error(f"Erro na conexão: {e}")
        return jsonify({"message": f"Erro na conexão: {str(e)}"})

@config_bp.route('/save-bpa-config', methods=['POST'])
def save_bpa_config():
    config_data = request.json
    with open('bpa_config.json', 'w') as config_file:
        json.dump(config_data, config_file)
    return jsonify({'status': 'Configuração salva com sucesso'})

@config_bp.route('/load-bpa-config', methods=['GET'])
def load_bpa_config():
    try:
        with open('bpa_config.json', 'r') as config_file:
            config_data = json.load(config_file)
            return jsonify(config_data)
    except FileNotFoundError:
        return jsonify({'error': 'Configuração não encontrada'}), 404

@config_bp.route('/get-import-config', methods=['GET'])
def get_import_config():
    config_data = importdados.ensure_auto_update_config()
    return jsonify(config_data)

@config_bp.route('/save-auto-update-config', methods=['POST'])
def save_auto_update_config_route():
    with app.app_context():  # Garante o contexto da aplicação
        data = request.json
        is_auto_update_on = data['isAutoUpdateOn']
        auto_update_time = data['autoUpdateTime']

        importdados.save_auto_update_config(is_auto_update_on, auto_update_time)

        if is_auto_update_on:
            if len(importdados.scheduler.get_jobs()) > 0:
                importdados.scheduler.remove_all_jobs()
            importdados.schedule_auto_import(importdados.scheduler, auto_update_time)
            logger.info(f"Autoatualização agendada para {auto_update_time}")
        else:
            importdados.scheduler.remove_all_jobs()
            logger.info("Autoatualização desativada")

        return jsonify({"status": "Configuração de autoatualização salva com sucesso!"})
