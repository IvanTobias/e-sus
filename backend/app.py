#app.py
import logging # Ensure logging is imported first for config
import os # For environment variables in logging config

# Centralized Logging Configuration
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
# Defaulting to a 'logs' subdirectory for cleanliness
LOG_DIR = os.environ.get('LOG_DIR', 'logs')
LOG_FILE_NAME = os.environ.get('LOG_FILE_NAME', 'backend_app.log')

# Ensure the log directory exists
if not os.path.exists(LOG_DIR):
    try:
        os.makedirs(LOG_DIR)
    except OSError as e:
        LOG_DIR = '.' 
        print(f"Warning: Could not create log directory {os.path.join(os.getcwd(), LOG_DIR)}. Logging to current directory. Error: {e}")

LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE_NAME)

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode='a'),
        logging.StreamHandler()
    ]
    # force=True # Consider if absolutely necessary and other basicConfigs cannot be removed.
)
# Initialize a logger for this file, after basicConfig.
logger = logging.getLogger(__name__)
logger.info(f"Centralized logging configured in app.py. Log level: {LOG_LEVEL}. Log file: {LOG_FILE_PATH}")

# Chame o monkey_patch do eventlet antes de qualquer importação
import eventlet
eventlet.monkey_patch()

# Agora podemos importar os outros módulos
from backend.init import app, CORS, db # app is initialized in init.py, added db
from backend.models.database import init_db
from backend.database_setup import create_application_tables
from socketio_config import socketio
# import Gerar_BPA # Moved to bpa_routes
# import pandas as pd # Moved to file_routes
import threading
import os
os.environ.setdefault("ENV", "instalador")
import importdados
from Common import task_status, update_task_status
# from Conexões import get_local_engine # Moved to blueprints that need it
# progress, progress_lock, update_progress_safely, log_message removed as they are in file_routes or not used
# from Consultas import execute_and_store_queries # Moved to bpa_routes (if only used there)
import json
from flask import make_response, request, jsonify, send_from_directory # send_file removed
from sqlalchemy import create_engine # text removed as it's used in blueprints
# from sqlalchemy.exc import SQLAlchemyError # Moved to data_import_routes
# import logging # Already imported and configured at the top
# import traceback # Moved to file_routes
from flask_caching import Cache
# from datetime import datetime # Moved to data_import_routes
import sys

# Import Blueprints from current project
# logger = logging.getLogger(__name__) # Already defined after basicConfig
from routes.senhas_routes import senhas_bp
from routes.powerbi_reports_routes import reports_bp
from routes.config_routes import config_bp
from routes.query_routes import query_bp
from routes.file_routes import file_bp
from routes.bpa_routes import bpa_bp
from routes.stats_routes import stats_bp
from routes.data_import_routes import data_import_bp

# Import Blueprints from Fiocruz project (now under src/)
try:
    from src.main.routes.diabetes_routes import diabetes_bp
    from src.main.routes.children_routes import children_bp
    from src.main.routes.city_informations_route import city_informations_bp
    from src.main.routes.demographics_info_route import demographics_info_bp
    from src.main.routes.elderly_routes import elderly_bp
    from src.main.routes.hypertension_routes import hypertension_bp
    from src.main.routes.oral_health import oral_health_bp
    from src.main.routes.records_routes import records_bp
    from src.main.routes.smoking import smoking_bp
    from src.main.routes.units_route import units_bp
    fiocruz_blueprints_available = True
except ImportError as e:
    print(f"Error importing Fiocruz blueprints: {e}. Fiocruz dashboards might not work.")
    fiocruz_blueprints_available = False

# Configuração básica de logging - REMOVED (centralized at the top)
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__) # Already defined after basicConfig

# Configuração do CORS e inicialização do Flask
CORS(app) # app is imported from init.py

# Register Blueprints from current project
app.register_blueprint(senhas_bp, url_prefix="/api/senhas")
app.register_blueprint(reports_bp, url_prefix="/api/reports")
app.register_blueprint(config_bp) # Registers with /api prefix from blueprint
app.register_blueprint(query_bp) # Registers with /api prefix from blueprint
app.register_blueprint(file_bp) # Registers with /api prefix from blueprint
app.register_blueprint(bpa_bp) # Registers with /api prefix from blueprint
app.register_blueprint(stats_bp) # Registers with /api prefix from blueprint
app.register_blueprint(data_import_bp) # Registers with /api prefix from blueprint

# Register Blueprints from Fiocruz project
if fiocruz_blueprints_available:
    app.register_blueprint(diabetes_bp, url_prefix="/api/v1/diabetes")
    app.register_blueprint(children_bp, url_prefix="/api/v1/children")
    app.register_blueprint(city_informations_bp, url_prefix="/api/v1/city-info")
    app.register_blueprint(demographics_info_bp, url_prefix="/api/v1/demographics")
    app.register_blueprint(elderly_bp, url_prefix="/api/v1/elderly")
    app.register_blueprint(hypertension_bp, url_prefix="/api/v1/hypertension")
    app.register_blueprint(oral_health_bp, url_prefix="/api/v1/oral-health")
    app.register_blueprint(records_bp, url_prefix="/api/v1/records")
    app.register_blueprint(smoking_bp, url_prefix="/api/v1/smoking")
    app.register_blueprint(units_bp, url_prefix="/api/v1/units")
    logger.info("Fiocruz dashboard blueprints registered.")

# Ensure the backend directory and src directory are in the Python path
backend_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(backend_dir, "src")
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# --- Catch-all for React Frontend ---
frontend_build_path = os.path.abspath(os.path.join(backend_dir, "..", "frontend", "build"))

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_react_app(path):
    if path.startswith("api/"):
        return jsonify({"error": "API endpoint not found"}), 404
    if path != "" and os.path.exists(os.path.join(frontend_build_path, path)):
        return send_from_directory(frontend_build_path, path)
    elif os.path.exists(os.path.join(frontend_build_path, "index.html")):
        return send_from_directory(frontend_build_path, "index.html")
    else:
        return "Frontend build not found.", 404

# Definições de variáveis globais
cancel_requests = {}

# Configuração do cache
cache = Cache(config={'CACHE_TYPE': 'SimpleCache'})
cache.init_app(app)

# =================== Endpoints da API ===================

@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store'
    return response

# Função genérica para exportação - MOVED to file_routes.py
# def export_task(tipo, query, filename, sid):
# ...

# Rota genérica para exportação - MOVED to file_routes.py
# @app.route('/export/<tipo>', methods=['GET'])
# def export_data(tipo):
# ...

# Rota para baixar o arquivo exportado - MOVED to file_routes.py
# @app.route('/download-exported-file/<tipo>', methods=['GET'])
# def download_exported_file(tipo):
# ...
    
# @app.route('/api/gerar-bpa', methods=['POST']) # MOVED to bpa_routes.py
# def gerar_bpa_route():
# ...

# @app.route('/execute-queries/bpa', methods=['GET']) # MOVED to bpa_routes.py and path changed
# def execute_bpa_queries():
# ...

# @app.route('/api/list-bpa-files', methods=['GET']) # MOVED to file_routes.py
# def list_bpa_files():
# ...

# @app.route('/api/delete-bpa-file', methods=['DELETE']) # MOVED to file_routes.py
# def delete_bpa_file():
# ...

# @app.route('/api/download-bpa-file', methods=['GET']) # MOVED to file_routes.py
# def download_bpa_file():
# ...

# @app.route('/api/contagens', methods=['GET']) # MOVED to stats_routes.py
# def fetch_contagens():
# ...

# @app.route('/api/unidades-saude', methods=['GET']) # MOVED to stats_routes.py
# @cache.cached(timeout=300)
# def fetchUnidadesSaude():
# ...

# @app.route('/api/detalhes', methods=['GET']) # MOVED to stats_routes.py
# def fetch_detalhes():
# ...

# @app.route('/api/cadastros-domiciliares', methods=['GET']) # MOVED to stats_routes.py
# def fetch_cadastros_domiciliares():
# ...

# @app.route('/api/detalhes-hover', methods=['GET']) # MOVED to stats_routes.py
# def fetch_detalhes_hover():
# ...

# @app.route('/api/contagem-detalhes', methods=['GET']) # MOVED to stats_routes.py
# def fetch_detalhes_count():
# ...

# @app.route('/configimport', methods=['GET']) # MOVED to data_import_routes.py
# def get_last_import():
# ...

# @app.route('/check-file/<import_type>', methods=['GET']) # MOVED to data_import_routes.py
# def check_file(import_type):
# ...

# @app.route('/api/visitas-domiciliares', methods=['GET']) # MOVED to data_import_routes.py
# def fetch_visitas_domiciliares():
# ...

# @app.route('/api/data', methods=['POST']) # MOVED to data_import_routes.py
# def get_data():
# ...

# @app.route('/api/corrigir-ceps') # MOVED to bpa_routes.py
# def corrigir_ceps():
# ...

# @app.route('/api/atualizar-cep-escolhido', methods=['POST']) # MOVED to bpa_routes.py
# def atualizar_cep_escolhido():
# ...

# Inicialização do servidor
if __name__ == '__main__':
    with app.app_context():  # Garante o contexto da aplicação
        logger.info("Initializing database and tables...")
        init_db(app)
        create_application_tables(db.engine)
        logger.info("Database and tables initialization complete.")

        logger.info(f"[STARTUP] Estado inicial de task_status: {task_status}")
        config = importdados.ensure_auto_update_config()
        if config['isAutoUpdateOn']:
            if len(importdados.scheduler.get_jobs()) > 0:
                importdados.scheduler.remove_all_jobs()
            importdados.schedule_auto_import(importdados.scheduler, config['autoUpdateTime'])
            logger.info(f"Autoatualização agendada para {config['autoUpdateTime']}")
        else:
            logger.info("Autoatualização desativada")

    socketio.run(app, host='0.0.0.0', port=5000, debug=True, use_reloader=False)
    logger.info("Executando importação automática de dados...")