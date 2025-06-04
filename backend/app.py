import eventlet
eventlet.monkey_patch()

# Standard Library Imports
import json
import logging
import os
import sys

# Third-Party Imports
from flask import request, jsonify, send_file, send_from_directory
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# Local Application Imports
from init import app, CORS, cache
from routes.config_routes import config_bp
from routes.task_routes import task_bp
from routes.bpa_routes import bpa_bp
from routes.dashboard_data_routes import dashboard_data_bp
from routes.import_config_routes import import_config_bp
from socketio_config import socketio
from Common import task_status
from Conexoes import get_local_engine # Changed from Conexões. This will need to be changed if Conexões.py is successfully renamed
from Consultas import execute_and_store_queries
from setup import create_database
from criar_banco import setup_local_database


# Define o nível global de logging
logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Silenciar logs do Socket.IO e Engine.IO
logging.getLogger("engineio").setLevel(logging.ERROR)
logging.getLogger("socketio").setLevel(logging.ERROR)
logging.getLogger("werkzeug").setLevel(logging.ERROR)

# Reduz verbosidade de outros loggers conhecidos
for logger_name in ['werkzeug', 'sqlalchemy', 'engineio', 'socketio']:
    logging.getLogger(logger_name).setLevel(logging.ERROR)

# Logger da aplicação principal
logger = logging.getLogger(__name__)

os.environ.setdefault("ENV", "instalador")

create_database()
setup_local_database()

# Ensure the backend directory and src directory are in the Python path
# This needs to be done before attempting to import from src
backend_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(backend_dir, "src")
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir) # Insert at the beginning to prioritize local src
if src_dir not in sys.path:
    sys.path.insert(0, src_dir) # Insert at the beginning

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


# Configuração do CORS e inicialização do Flask
CORS(app)

# Register Blueprints from current project
app.register_blueprint(config_bp, url_prefix='/api')
app.register_blueprint(task_bp) # url_prefix is in the blueprint itself
app.register_blueprint(bpa_bp) # url_prefix is in the blueprint itself
app.register_blueprint(dashboard_data_bp) # url_prefix is in the blueprint itself
app.register_blueprint(import_config_bp)

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
# cache = Cache(config={'CACHE_TYPE': 'SimpleCache'}) # Cache is now initialized in init.py
# cache.init_app(app) # init.py handles this

# =================== Endpoints da API ===================

@app.after_request # This was defined after the routes to be moved, keep it here.
def add_header(response):
    response.headers['Cache-Control'] = 'no-store'
    return response
    
# BPA routes moved to bpa_routes.py:
# /api/gerar-bpa
# /api/save-bpa-config
# /api/load-bpa-config
# /api/list-bpa-files
# /api/delete-bpa-file
# /api/download-bpa-file
# /api/corrigir-ceps
# /api/atualizar-cep-escolhido

@app.route('/execute-queries/bpa', methods=['GET']) # Keeping this route for now.
def execute_bpa_queries():
    config_data = request.args.to_dict()
    tipo = 'bpa'
    execute_and_store_queries(config_data, tipo)
    return jsonify({"status": "success"})

# Dashboard data routes moved to dashboard_data_routes.py:
# /api/contagens
# /api/unidades-saude
# /api/detalhes
# /api/cadastros-domiciliares
# /api/detalhes-hover
# /api/contagem-detalhes
# /api/visitas-domiciliares
# /api/data (POST)

# Data import configuration routes moved to import_config_routes.py:
# /configimport
# /api/get-import-config
# /api/save-auto-update-config
# /check-file/<import_type>

# Corrigir CEPs and Atualizar CEP routes were moved to bpa_routes.py

# Inicialização do servidor
if __name__ == '__main__':
    # Need to import importdados here if it's used in __main__
    import importdados # Ensuring importdados is available for the __main__ block
    with app.app_context():  # Garante o contexto da aplicação
        logger.info(f"[STARTUP] Estado inicial de task_status: {task_status}")
        config = importdados.ensure_auto_update_config() # This line requires importdados
        if config['isAutoUpdateOn']:
            if len(importdados.scheduler.get_jobs()) > 0:
                importdados.scheduler.remove_all_jobs()
            importdados.schedule_auto_import(importdados.scheduler, config['autoUpdateTime'])
            logger.info(f"Autoatualização agendada para {config['autoUpdateTime']}")
        else:
            logger.info("Autoatualização desativada")

    socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False)
    logger.info("Executando importação automática de dados...")