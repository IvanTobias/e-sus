#app.py
# Chame o monkey_patch do eventlet antes de qualquer importação
import eventlet
eventlet.monkey_patch()

# Agora podemos importar os outros módulos
from init import app, CORS
from socketio_config import socketio,task_clients, emit_end_task,emit_progress,emit_start_task
import Gerar_BPA
import pandas as pd
import threading
import os
os.environ.setdefault("ENV", "instalador")
import importdados
from Common import task_status, update_task_status
from Conexões import get_local_engine
from Consultas import send_progress_update, execute_long_task, get_progress, execute_and_store_queries, progress, progress_lock, update_progress_safely, log_message
import json
from flask import make_response, request, jsonify, send_file, send_from_directory
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
import traceback
from flask_caching import Cache
from datetime import datetime
import sys

# Import Blueprints from current project
from routes.senhas_routes import senhas_bp
from routes.powerbi_reports_routes import reports_bp

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

# Database setup imports
from init import db # db should be accessible from init
from database_setup import create_custom_tables
from models.senha import Senha
# It's assumed that src.infra.db.settings.base.Base.metadata is populated correctly
# by importing the entities for db.create_all() to work on them.
# If not, direct imports like these would be needed:
try:
    from src.infra.db.entities.pessoas import Pessoas
    from src.infra.db.entities.equipes import Equipes
    from src.infra.db.entities.diabetes_nominal import DiabetesNominal
    from src.infra.db.entities.hypertension_nominal import HipertensaoNominal
    from src.infra.db.entities.criancas import Crianca # Assuming 'criancas.py' for Crianca model
    from src.infra.db.entities.idosos import Idoso # Assuming 'idosos.py' for Idoso model
except ImportError as e:
    print(f"Could not import all Fiocruz SQLAlchemy models: {e}. Some tables might not be created by db.create_all().")

from importar_ceps_local import populate_correios_ceps_if_empty
import os # Ensure os is imported


# Configuração básica de logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configuração do CORS e inicialização do Flask
CORS(app)

# Register Blueprints from current project
app.register_blueprint(senhas_bp, url_prefix="/api/senhas")
app.register_blueprint(reports_bp, url_prefix="/api/reports")

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
DNE_FILES_DIR = os.path.join(backend_dir, "dne_data") # Define DNE_FILES_DIR

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

@app.route('/api/save-config', methods=['POST'])
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

@app.route('/api/get-config', methods=['GET'])
def get_config():
    try:
        with open('config.json', 'r') as config_file:
            config_data = json.load(config_file)
    except FileNotFoundError:
        config_data = {"ip": "", "port": "", "database": "", "username": "", "password": ""}
    return jsonify(config_data)

@app.route('/api/test-connection', methods=['POST'])
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

@app.route('/api/download-results')
def download_results():
    filename = request.args.get('file')
    return send_file(filename, as_attachment=True)

@app.route('/api/query-progress')
def query_progress():
    return jsonify({"progress": 100})

@app.route('/execute-queries/<tipo>', methods=['GET', 'POST'])
def execute_queries(tipo):
    if tipo not in ['cadastro', 'domiciliofcd', 'bpa', 'visitas', 'atendimentos', 'iaf', 'pse', 'pse_prof']:
        logger.error(f"Tipo desconhecido: {tipo}")
        return jsonify({"status": "error", "message": "Tipo desconhecido."})

    if task_status.get(tipo) == "running":
        logger.warning(f"Consultas {tipo} já estão em execução.")
        return jsonify({"status": "error", "message": f"Consultas {tipo} já estão em execução."})

    logger.info(f"[API] Iniciando tarefa para tipo={tipo}")
    send_progress_update(tipo, 0)

    try:
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

    thread = threading.Thread(target=execute_long_task, args=(config_data, tipo))
    thread.start()
    logger.info(f"Tarefa para tipo={tipo} iniciada em uma thread.")

    return jsonify({"status": "success", "message": f"Consultas {tipo} em execução."})

@app.route('/progress/<tipo>', methods=['GET'])
def get_progress_endpoint(tipo):
    progress = get_progress(tipo)
    response = make_response(jsonify({"progress": progress}))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    return response

@app.route('/task-status/<tipo>', methods=['GET'])
def get_task_status_endpoint(tipo):
    logger.info(f"[TASK-STATUS] Acessando task_status para tipo={tipo}, task_status atual: {task_status}")
    if tipo not in task_status:
        logger.info(f"[TASK-STATUS] Tipo {tipo} não encontrado em task_status")
        return jsonify({"status": "unknown"}), 404
    status = task_status.get(tipo, "unknown")
    if not isinstance(status, str):
        logger.error(f"[TASK-STATUS] Status inválido para {tipo}: {status}. Convertendo para string.")
        status = str(status)
    response = {"status": status}
    logger.info(f"[TASK-STATUS] Retornando status para {tipo}: {response}")
    return jsonify(response)

@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store'
    return response

# Função genérica para exportação
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

        filepath = os.path.join(os.getcwd(), filename)
        logger.info(f"[EXPORT-{tipo.upper()}] Salvando arquivo em {filepath}")
        df.to_excel(filepath, index=False)
        logger.info(f"[EXPORT-{tipo.upper()}] Arquivo salvo com sucesso")

        update_progress_safely(tipo, 100)
        logger.info(f"[EXPORT-{tipo.upper()}] Enviando progresso final (100%) para sid={sid}")
        emit_progress(tipo, 100, sid)

        update_task_status(tipo, "completed")
        log_message(f"Exportação de {tipo} concluída com sucesso.")
    except Exception as e:
        error_message = traceback.format_exc()
        log_message(f"Erro na exportação de {tipo}: {error_message}")
        update_task_status(tipo, "failed")
        update_progress_safely(tipo, 0, error=str(e))
        logger.error(f"[EXPORT-{tipo.upper()}] Erro durante exportação: {str(e)}")
        emit_progress(tipo, 0, sid, error=str(e))
    finally:
        emit_end_task(tipo, sid)
        logger.info(f"[EXPORT-{tipo.upper()}] Finalizando tarefa para sid={sid}")

# Rota genérica para exportação
@app.route('/export/<tipo>', methods=['GET'])
def export_data(tipo):
    export_configs = {
        "cadastro": {
            "query": "SELECT * FROM tb_cadastro",
            "filename": "cadastros_exportados.xlsx"
        },
        "domiciliofcd": {
            "query": "SELECT * FROM tb_domicilio",
            "filename": "domiciliofcd_exportadas.xlsx"
        },
        "visitas": {
            "query": "SELECT * FROM tb_visitas",
            "filename": "visitas_exportadas.xlsx"
        },
        "atendimentos": {
            "query": "SELECT * FROM tb_atendimentos",
            "filename": "atendimentos_exportadas.xlsx"
        },
        "bpa": {
            "query": "SELECT * FROM tb_bpa",
            "filename": "bpa.xlsx"
        },
        "iaf": {
            "query": "SELECT * FROM tb_iaf",
            "filename": "iaf.xlsx"
        },
        "pse": {
            "query": "SELECT * FROM tb_pse",
            "filename": "pse.xlsx"
        },
        "pse_prof": {
            "query": "SELECT * FROM tb_pse_prof",
            "filename": "pse_prof.xlsx"
        }
    }

    if tipo not in export_configs:
        logger.error(f"[EXPORT-{tipo.upper()}] Tipo não suportado: {tipo}")
        return jsonify({"status": "error", "message": f"Tipo {tipo} não suportado."}), 400

    if task_status.get(tipo) == "running":
        logger.info(f"[EXPORT-{tipo.upper()}] Tarefa para {tipo} já está em execução.")
        return jsonify({"status": "error", "message": f"Exportação {tipo} já está em execução."}), 400

    # Verificar se há um cliente associado à tarefa
    sid = task_clients.get(tipo)
    if not sid:
        logger.warning(f"[EXPORT-{tipo.upper()}] Nenhum cliente associado à tarefa {tipo}. Usando emissão global.")
        sid = None  # Fallback para emissão global

    config = export_configs[tipo]
    logger.info(f"[EXPORT-{tipo.upper()}] Iniciando thread de exportação para tipo={tipo}, sid={sid}")
    threading.Thread(target=export_task, args=(tipo, config["query"], config["filename"], sid)).start()
    return jsonify({"status": "started", "message": "Exportação iniciada com sucesso."})

# Rota para baixar o arquivo exportado
@app.route('/download-exported-file/<tipo>', methods=['GET'])
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

    # Verificar o status da tarefa
    current_status = task_status.get(tipo)
    if current_status == "running":
        return jsonify({"status": "error", "message": "Exportação ainda em andamento. Aguarde a conclusão."}), 400
    elif current_status == "failed":
        return jsonify({"status": "error", "message": "Exportação falhou. Verifique os logs para mais detalhes."}), 400
    elif current_status != "completed":
        return jsonify({"status": "error", "message": "Nenhuma exportação concluída para este tipo."}), 400

    try:
        filename = export_configs[tipo]
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
    
@app.route('/api/gerar-bpa', methods=['POST'])
def gerar_bpa_route():
    try:
        filepath = Gerar_BPA.criar_arquivo_bpa()
        if filepath:
            return send_file(filepath, as_attachment=True)
        else:
            return jsonify({"message": "Erro ao gerar BPA: Arquivo não foi criado."}), 500
    except Exception as e:
        logger.error(f"Erro ao gerar BPA: {str(e)}")
        return jsonify({"message": f"Erro ao gerar BPA: {str(e)}"}), 500

@app.route('/execute-queries/bpa', methods=['GET'])
def execute_bpa_queries():
    config_data = request.args.to_dict()
    tipo = 'bpa'
    execute_and_store_queries(config_data, tipo)
    return jsonify({"status": "success"})

@app.route('/api/save-bpa-config', methods=['POST'])
def save_bpa_config():
    config_data = request.json
    with open('bpa_config.json', 'w') as config_file:
        json.dump(config_data, config_file)
    return jsonify({'status': 'Configuração salva com sucesso'})

@app.route('/api/load-bpa-config', methods=['GET'])
def load_bpa_config():
    try:
        with open('bpa_config.json', 'r') as config_file:
            config_data = json.load(config_file)
            return jsonify(config_data)
    except FileNotFoundError:
        return jsonify({'error': 'Configuração não encontrada'}), 404

@app.route('/api/list-bpa-files', methods=['GET'])
def list_bpa_files():
    files = []
    try:
        for filename in os.listdir('.'):
            if filename.startswith('bpa_') and filename.endswith('.txt'):
                files.append(filename)
        return jsonify({"files": files})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/delete-bpa-file', methods=['DELETE'])
def delete_bpa_file():
    filename = request.args.get('filename')
    try:
        file_path = os.path.join(os.getcwd(), filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            return jsonify({"message": f"Arquivo {filename} deletado com sucesso!"})
        else:
            return jsonify({"message": "Arquivo não encontrado."}), 404
    except Exception as e:
        return jsonify({"message": f"Erro ao deletar arquivo: {str(e)}"}), 500

@app.route('/api/download-bpa-file', methods=['GET'])
def download_bpa_file():
    filename = request.args.get('filename')
    try:
        return send_from_directory(os.getcwd(), filename, as_attachment=True)
    except Exception as e:
        return jsonify({"message": f"Erro ao baixar o arquivo: {str(e)}"}), 500

@app.route('/api/contagens', methods=['GET'])
def fetch_contagens():
    unidade_saude = request.args.get('unidade_saude', default='', type=str)
    equipe = request.args.get('equipe', default='', type=str)
    profissional = request.args.get('profissional', default='', type=str)

    engine = get_local_engine()
    query_filters = []
    params = {}

    if unidade_saude:
        unidade_saude_list = unidade_saude.split(',')
        query_filters.append("no_unidade_saude IN :unidade_saude")
        params["unidade_saude"] = tuple(unidade_saude_list)

    if equipe:
        equipe_list = equipe.split(',')
        query_filters.append("no_equipe IN :equipe")
        params["equipe"] = tuple(equipe_list)

    if profissional:
        profissional_list = profissional.split(',')
        query_filters.append("no_profissional IN :profissional")
        params["profissional"] = tuple(profissional_list)

    base_query_geral = """
        SELECT 
        COUNT(t1.co_seq_fat_cad_individual) as "Cadastros Individuais",
        COUNT(CASE WHEN t1.co_dim_tipo_saida_cadastro = '3' AND t1.st_morador_rua = '1' THEN 1 END) as "Moradores de Rua",
        COUNT(CASE WHEN t1.co_dim_tipo_saida_cadastro = '1' THEN 1 END) as "Óbitos",
        COUNT(CASE WHEN t1.co_dim_tipo_saida_cadastro = '2' THEN 1 END) as "Mudou-se",
        COUNT(CASE WHEN t1.co_dim_tipo_saida_cadastro = '3' THEN 1 END) as "Cadastros Ativos",
        COUNT(CASE WHEN t1.co_dim_tipo_saida_cadastro = '3' AND t1.nu_micro_area = 'FA' THEN 1 END) as "Fora de Área",
        COUNT(CASE WHEN dt_atualizado < (CURRENT_DATE - INTERVAL '1 year') THEN 1 END) as "Cadastros Desatualizados",
        COUNT(CASE WHEN t1.co_dim_tipo_saida_cadastro = '3' AND LENGTH(TRIM(nu_cns)) = 15 AND nu_cns != '0' THEN 1 END) as "Cadastros com Cns",
        COUNT(CASE WHEN t1.co_dim_tipo_saida_cadastro = '3' AND (LENGTH(TRIM(nu_cns)) != 15 OR nu_cns = '0') THEN 1 END) as "Cadastros com Cpf"
        FROM tb_cadastro t1
        WHERE t1.st_ativo = 1 
        AND t1.co_dim_tipo_saida_cadastro IS NOT NULL 
        AND t1.st_ficha_inativa = 0
        """

    domicilio_query = """
        SELECT COUNT(q2.co_seq_cds_cad_domiciliar) as "Cadastros Domiciliares"
        FROM tb_domicilio q2
        WHERE q2.st_ativo = 1
        """

    if query_filters:
        base_query_geral += " AND " + " AND ".join(query_filters)
        domicilio_query += " AND " + " AND ".join(query_filters)

    with engine.connect() as connection:
        result_geral = connection.execute(text(base_query_geral), params)
        counts_geral = result_geral.fetchone()

        result_domicilio = connection.execute(text(domicilio_query), params)
        counts_domicilio = result_domicilio.fetchone()

    counts_dict = {
        "Cadastros Individuais": counts_geral[0],
        "Moradores de Rua": counts_geral[1],
        "Óbitos": counts_geral[2],
        "Mudou-se": counts_geral[3],
        "Cadastros Ativos": counts_geral[4],
        "Fora de Área": counts_geral[5],
        "Cadastros Desatualizados": counts_geral[6],
        "Cadastros com Cns": counts_geral[7],
        "Cadastros com Cpf": counts_geral[8],
        "Cadastros Domiciliares": counts_domicilio[0]
    }

    return jsonify(counts_dict)

@app.route('/api/unidades-saude', methods=['GET'])
@cache.cached(timeout=300)
def fetchUnidadesSaude():
    unidade_saude = request.args.get('unidade_saude', default='', type=str)
    equipe = request.args.get('equipe', default='', type=str)
    profissional = request.args.get('profissional', default='', type=str)
    engine = get_local_engine()

    query = """
    SELECT 
    no_unidade_saude, 
    no_equipe, 
    no_profissional
    FROM tb_cadastro
    """

    conditions = []
    params = {}

    if unidade_saude:
        conditions.append("no_unidade_saude = :unidade_saude")
        params["unidade_saude"] = unidade_saude

    if equipe:
        conditions.append("no_equipe = :equipe")
        params["equipe"] = equipe

    if profissional:
        conditions.append("no_profissional = :profissional")
        params["profissional"] = profissional

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " GROUP BY no_unidade_saude, no_equipe, no_profissional"

    try:
        with engine.connect() as connection:
            result = connection.execute(text(query), params)
        
        data = [
            {
                'unidadeSaude': row._mapping['no_unidade_saude'],
                'equipe': row._mapping['no_equipe'],
                'profissional': row._mapping['no_profissional']
            }
            for row in result.fetchall()
        ]

        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/detalhes', methods=['GET'])
def fetch_detalhes():
    tipo = request.args.get('tipo', default='', type=str).strip().lower()
    unidades = request.args.get('unidade_saude', default='', type=str).strip()
    equipes = request.args.get('equipe', default='', type=str).strip()
    profissionais = request.args.get('profissional', default='', type=str).strip()

    engine = get_local_engine()
    query_filters = []
    params = {}

    tipo_map = {
        'responsável familiar': 'st_responsavel_familiar',
        'frequenta creche': 'st_frequenta_creche',
        'frequenta cuidador': 'st_frequenta_cuidador',
        'participa de grupo comunitário': 'st_participa_grupo_comunitario',
        'plano de saúde privado': 'st_plano_saude_privado',
        'deficiência': 'st_deficiencia',
        'deficiência auditiva': 'st_defi_auditiva',
        'deficiência intelectual/cognitiva': 'st_defi_intelectual_cognitiva',
        'deficiência visual': 'st_defi_visual',
        'deficiência física': 'st_defi_fisica',
        'outra deficiência': 'st_defi_outra',
        'gestante': 'st_gestante',
        'doença respiratória': 'st_doenca_respiratoria',
        'doença respiratória (asma)': 'st_doenca_respira_asma',
        'doença respiratória (dpoc/enfisema)': 'st_doenca_respira_dpoc_enfisem',
        'outra doença respiratória': 'st_doenca_respira_outra',
        'doença respiratória (não sabe)': 'st_doenca_respira_n_sabe',
        'fumante': 'st_fumante',
        'consome álcool': 'st_alcool',
        'consome outra droga': 'st_outra_droga',
        'hipertensão arterial': 'st_hipertensao_arterial',
        'diabetes': 'st_diabete',
        'avc': 'st_avc',
        'hanseníase': 'st_hanseniase',
        'tuberculose': 'st_tuberculose',
        'câncer': 'st_cancer',
        'internação nos últimos 12 meses': 'st_internacao_12',
        'tratamento psiquiátrico': 'st_tratamento_psiquiatra',
        'acamado': 'st_acamado',
        'domiciliado': 'st_domiciliado',
        'usa planta medicinal': 'st_usa_planta_medicinal',
        'doença cardíaca': 'st_doenca_cardiaca',
        'insuficiência cardíaca': 'st_doenca_card_insuficiencia',
        'outra doença cardíaca': 'st_doenca_card_outro',
        'doença cardíaca (não sabe)': 'st_doenca_card_n_sabe',
        'problema renal': 'st_problema_rins',
        'insuficiência renal': 'st_problema_rins_insuficiencia',
        'outro problema renal': 'st_problema_rins_outro',
        'problema renal (não sabe)': 'st_problema_rins_nao_sabe'
    }

    if tipo not in ['óbitos', 'mudou-se']:
        query_filters.append("t1.co_dim_tipo_saida_cadastro = '3'")

    if tipo in tipo_map:
        query_filters.append(f"t1.{tipo_map[tipo]} = '1'")

    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',')]
        query_filters.append("no_unidade_saude = ANY(:unidades)")
        params["unidades"] = unidade_saude_list

    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',')]
        query_filters.append("no_equipe = ANY(:equipes)")
        params["equipes"] = equipe_list

    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',')]
        query_filters.append("no_profissional = ANY(:profissionais)")
        params["profissionais"] = profissional_list
    
    if tipo:
        if tipo == "moradores de rua":
            query_filters.append("t1.st_morador_rua = '1'")
        elif tipo == "óbitos":
            query_filters.append("t1.co_dim_tipo_saida_cadastro = '1'")
        elif tipo == "mudou-se":
            query_filters.append("t1.co_dim_tipo_saida_cadastro = '2'")
        elif tipo == "cadastros ativos":
            query_filters.append("t1.co_dim_tipo_saida_cadastro = '3'")
        elif tipo == "fora de área":
            query_filters.append("t1.nu_micro_area = 'FA'")
        elif tipo == "cadastros desatualizados":
            query_filters.append("t1.dt_atualizado < (CURRENT_DATE - INTERVAL '1 year')")
        elif tipo == "cadastros com cns":
            query_filters.append("(LENGTH(TRIM(t1.nu_cns)) = 15 AND t1.nu_cns != '0') and co_dim_tipo_saida_cadastro = '3'")
        elif tipo == "cadastros com cpf":
            query_filters.append("(LENGTH(TRIM(t1.nu_cns)) != 15 OR t1.nu_cns = '0') and co_dim_tipo_saida_cadastro = '3'")

    base_query = """
    SELECT 
        no_cidadao,
        nu_cpf_cidadao,
        nu_cns,
        to_char(dt_nascimento, 'dd/mm/yyyy') AS dt_nascimento,
        no_unidade_saude,
        no_profissional,
        no_equipe,
        co_cidadao,
        to_char(dt_atualizado, 'dd/mm/yyyy') AS dt_atualizado
    FROM tb_cadastro t1
    WHERE st_ativo = 1 
    """

    if query_filters:
        base_query += " AND " + " AND ".join(query_filters)

    with engine.connect() as connection:
        result = connection.execute(text(base_query), params)
        data = []
        for row in result.fetchall():
            item = {
                'nome': row._mapping['no_cidadao'],
                'cpf': row._mapping['nu_cpf_cidadao'],
                'cns': row._mapping['nu_cns'],
                'data_nascimento': row._mapping['dt_nascimento'],
                'unidade_saude': row._mapping['no_unidade_saude'],
                'profissional': row._mapping['no_profissional'],
                'equipe': row._mapping['no_equipe'],
                'co_cidadao': row._mapping['co_cidadao'],
                'dt_atualizado': row._mapping['dt_atualizado']
            }
            data.append(item)

    return jsonify(data)

@app.route('/api/cadastros-domiciliares', methods=['GET'])
def fetch_cadastros_domiciliares():
    unidades = request.args.get('unidade_saude', default='', type=str).strip()
    equipes = request.args.get('equipe', default='', type=str).strip()
    profissionais = request.args.get('profissional', default='', type=str).strip()

    engine = get_local_engine()
    query_filters = []
    params = {}

    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',')]
        if unidade_saude_list:
            query_filters.append("no_unidade_saude = ANY(:unidades)")
            params["unidades"] = unidade_saude_list
    
    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',')]
        if equipe_list:
            query_filters.append("no_equipe = ANY(:equipes)")
            params["equipes"] = equipe_list

    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',')]
        if profissional_list:
            query_filters.append("no_profissional = ANY(:profissionais)")
            params["profissionais"] = profissional_list

    domicilio_query = """
    SELECT 
        no_logradouro AS rua,
        nu_domicilio AS numero,
        ds_complemento AS complemento,
        no_bairro AS bairro,
        nu_cep AS cep,
        no_unidade_saude,
        no_profissional,
        no_equipe
    FROM tb_domicilio q2
    WHERE q2.st_ativo = 1
    """

    if query_filters:
        domicilio_query += " AND " + " AND ".join(query_filters)

    with engine.connect() as connection:
        result = connection.execute(text(domicilio_query), params)
        data = []
        for row in result.fetchall():
            item = {
                'rua': row._mapping['rua'],
                'numero': row._mapping['numero'],
                'complemento': row._mapping['complemento'],
                'bairro': row._mapping['bairro'],
                'cep': row._mapping['cep'],
                'profissional': row._mapping['no_profissional'],
                'unidade_saude': row._mapping['no_unidade_saude'],
                'equipe': row._mapping['no_equipe']
            }
            data.append(item)

    return jsonify(data)

@app.route('/api/detalhes-hover', methods=['GET'])
def fetch_detalhes_hover():
    co_cidadao = request.args.get('co_cidadao', default='', type=str).strip()
    unidades = request.args.get('unidade_saude', default='', type=str).strip()
    equipes = request.args.get('equipe', default='', type=str).strip()
    profissionais = request.args.get('profissional', default='', type=str).strip()

    engine = get_local_engine()

    query = """
    SELECT jsonb_strip_nulls(
        jsonb_build_object(
            'Responsável Familiar', CASE WHEN st_responsavel_familiar = '1' THEN 'Sim' ELSE NULL END,
            'Frequenta Creche', CASE WHEN st_frequenta_creche = '1' THEN 'Sim' ELSE NULL END,
            'Frequenta Cuidador', CASE WHEN st_frequenta_cuidador = '1' THEN 'Sim' ELSE NULL END,
            'Participa de Grupo Comunitário', CASE WHEN st_participa_grupo_comunitario = '1' THEN 'Sim' ELSE NULL END,
            'Plano de Saúde Privado', CASE WHEN st_plano_saude_privado = '1' THEN 'Sim' ELSE NULL END,
            'Deficiência', CASE WHEN st_deficiencia = '1' THEN 'Sim' ELSE NULL END,
            'Deficiência Auditiva', CASE WHEN st_defi_auditiva = '1' THEN 'Sim' ELSE NULL END,
            'Deficiência Intelectual/Cognitiva', CASE WHEN st_defi_intelectual_cognitiva = '1' THEN 'Sim' ELSE NULL END,
            'Deficiência Visual', CASE WHEN st_defi_visual = '1' THEN 'Sim' ELSE NULL END,
            'Deficiência Física', CASE WHEN st_defi_fisica = '1' THEN 'Sim' ELSE NULL END,
            'Outra Deficiência', CASE WHEN st_defi_outra = '1' THEN 'Sim' ELSE NULL END,
            'Gestante', CASE WHEN st_gestante = '1' THEN 'Sim' ELSE NULL END,
            'Doença Respiratória', CASE WHEN st_doenca_respiratoria = '1' THEN 'Sim' ELSE NULL END,
            'Doença Respiratória (Asma)', CASE WHEN st_doenca_respira_asma = '1' THEN 'Sim' ELSE NULL END,
            'Doença Respiratória (DPOC/Enfisema)', CASE WHEN st_doenca_respira_dpoc_enfisem = '1' THEN 'Sim' ELSE NULL END,
            'Outra Doença Respiratória', CASE WHEN st_doenca_respira_outra = '1' THEN 'Sim' ELSE NULL END,
            'Doença Respiratória (Não Sabe)', CASE WHEN st_doenca_respira_n_sabe = '1' THEN 'Sim' ELSE NULL END,
            'Fumante', CASE WHEN st_fumante = '1' THEN 'Sim' ELSE NULL END,
            'Consome Álcool', CASE WHEN st_alcool = '1' THEN 'Sim' ELSE NULL END,
            'Consome Outra Droga', CASE WHEN st_outra_droga = '1' THEN 'Sim' ELSE NULL END,
            'Hipertensão Arterial', CASE WHEN st_hipertensao_arterial = '1' THEN 'Sim' ELSE NULL END,
            'Diabetes', CASE WHEN st_diabete = '1' THEN 'Sim' ELSE NULL END,
            'AVC', CASE WHEN st_avc = '1' THEN 'Sim' ELSE NULL END,
            'Hanseníase', CASE WHEN st_hanseniase = '1' THEN 'Sim' ELSE NULL END,
            'Tuberculose', CASE WHEN st_tuberculose = '1' THEN 'Sim' ELSE NULL END,
            'Câncer', CASE WHEN st_cancer = '1' THEN 'Sim' ELSE NULL END,
            'Internação nos Últimos 12 Meses', CASE WHEN st_internacao_12 = '1' THEN 'Sim' ELSE NULL END,
            'Tratamento Psiquiátrico', CASE WHEN st_tratamento_psiquiatra = '1' THEN 'Sim' ELSE NULL END,
            'Acamado', CASE WHEN st_acamado = '1' THEN 'Sim' ELSE NULL END,
            'Domiciliado', CASE WHEN st_domiciliado = '1' THEN 'Sim' ELSE NULL END,
            'Usa Planta Medicinal', CASE WHEN st_usa_planta_medicinal = '1' THEN 'Sim' ELSE NULL END,
            'Doença Cardíaca', CASE WHEN st_doenca_cardiaca = '1' THEN 'Sim' ELSE NULL END,
            'Insuficiência Cardíaca', CASE WHEN st_doenca_card_insuficiencia = '1' THEN 'Sim' ELSE NULL END,
            'Outra Doença Cardíaca', CASE WHEN st_doenca_card_outro = '1' THEN 'Sim' ELSE NULL END,
            'Doença Cardíaca (Não Sabe)', CASE WHEN st_doenca_card_n_sabe = '1' THEN 'Sim' ELSE NULL END,
            'Problema Renal', CASE WHEN st_problema_rins = '1' THEN 'Sim' ELSE NULL END,
            'Insuficiência Renal', CASE WHEN st_problema_rins_insuficiencia = '1' THEN 'Sim' ELSE NULL END,
            'Outro Problema Renal', CASE WHEN st_problema_rins_outro = '1' THEN 'Sim' ELSE NULL END,
            'Problema Renal (Não Sabe)', CASE WHEN st_problema_rins_nao_sabe = '1' THEN 'Sim' ELSE NULL END
        )
    ) AS details
    FROM tb_cadastro
    WHERE co_cidadao = :co_cidadao
    """

    query_filters = []
    params = {"co_cidadao": co_cidadao}

    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',')]
        query_filters.append("no_unidade_saude = ANY(:unidades)")
        params["unidades"] = unidade_saude_list

    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',')]
        query_filters.append("no_equipe = ANY(:equipes)")
        params["equipes"] = equipe_list

    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',')]
        query_filters.append("no_profissional = ANY(:profissionais)")
        params["profissionais"] = profissional_list

    if query_filters:
        query += " AND " + " AND ".join(query_filters)

    with engine.connect() as connection:
        result = connection.execute(text(query), params)
        data = result.fetchone()
        
        if data:
            details = data._mapping['details']
            return jsonify(details=details)
        else:
            return jsonify(details={})

@app.route('/api/contagem-detalhes', methods=['GET'])
def fetch_detalhes_count():
    unidades = request.args.get('unidade_saude', default='', type=str).strip()
    equipes = request.args.get('equipe', default='', type=str).strip()
    profissionais = request.args.get('profissional', default='', type=str).strip()

    engine = get_local_engine()

    base_query = """
    SELECT
        COUNT(CASE WHEN st_responsavel_familiar = '1' THEN 1 END) AS responsavel_familiar,
        COUNT(CASE WHEN st_frequenta_creche = '1' THEN 1 END) AS frequenta_creche,
        COUNT(CASE WHEN st_frequenta_cuidador = '1' THEN 1 END) AS frequenta_cuidador,
        COUNT(CASE WHEN st_participa_grupo_comunitario = '1' THEN 1 END) AS participa_grupo_comunitario,
        COUNT(CASE WHEN st_plano_saude_privado = '1' THEN 1 END) AS plano_saude_privado,
        COUNT(CASE WHEN st_deficiencia = '1' THEN 1 END) AS deficiencia,
        COUNT(CASE WHEN st_defi_auditiva = '1' THEN 1 END) AS deficiencia_auditiva,
        COUNT(CASE WHEN st_defi_intelectual_cognitiva = '1' THEN 1 END) AS deficiencia_intelectual_cognitiva,
        COUNT(CASE WHEN st_defi_visual = '1' THEN 1 END) AS deficiencia_visual,
        COUNT(CASE WHEN st_defi_fisica = '1' THEN 1 END) AS deficiencia_fisica,
        COUNT(CASE WHEN st_defi_outra = '1' THEN 1 END) AS outra_deficiencia,
        COUNT(CASE WHEN st_gestante = '1' THEN 1 END) AS gestante,
        COUNT(CASE WHEN st_doenca_respiratoria = '1' THEN 1 END) AS doenca_respiratoria,
        COUNT(CASE WHEN st_doenca_respira_asma = '1' THEN 1 END) AS asma,
        COUNT(CASE WHEN st_doenca_respira_dpoc_enfisem = '1' THEN 1 END) AS dpoc,
        COUNT(CASE WHEN st_doenca_respira_outra = '1' THEN 1 END) AS outra_respiratoria,
        COUNT(CASE WHEN st_doenca_respira_n_sabe = '1' THEN 1 END) AS nao_sabe_respiratoria,
        COUNT(CASE WHEN st_fumante = '1' THEN 1 END) AS fumante,
        COUNT(CASE WHEN st_alcool = '1' THEN 1 END) AS alcool,
        COUNT(CASE WHEN st_outra_droga = '1' THEN 1 END) AS outra_droga,
        COUNT(CASE WHEN st_hipertensao_arterial = '1' THEN 1 END) AS hipertensao_arterial,
        COUNT(CASE WHEN st_diabete = '1' THEN 1 END) AS diabetes,
        COUNT(CASE WHEN st_avc = '1' THEN 1 END) AS AVC,
        COUNT(CASE WHEN st_hanseniase = '1' THEN 1 END) AS hanseniase,
        COUNT(CASE WHEN st_tuberculose = '1' THEN 1 END) AS tuberculose,
        COUNT(CASE WHEN st_cancer = '1' THEN 1 END) AS cancer,
        COUNT(CASE WHEN st_internacao_12 = '1' THEN 1 END) AS internacao_12,
        COUNT(CASE WHEN st_tratamento_psiquiatra = '1' THEN 1 END) AS tratamento_psiquiatrico,
        COUNT(CASE WHEN st_acamado = '1' THEN 1 END) AS acamado,
        COUNT(CASE WHEN st_domiciliado = '1' THEN 1 END) AS domiciliado,
        COUNT(CASE WHEN st_usa_planta_medicinal = '1' THEN 1 END) AS planta_medicinal,
        COUNT(CASE WHEN st_doenca_cardiaca = '1' THEN 1 END) AS doenca_cardiaca,
        COUNT(CASE WHEN st_doenca_card_insuficiencia = '1' THEN 1 END) AS insuficiencia_cardiaca,
        COUNT(CASE WHEN st_doenca_card_outro = '1' THEN 1 END) AS outra_cardiaca,
        COUNT(CASE WHEN st_doenca_card_n_sabe = '1' THEN 1 END) AS nao_sabe_cardiaca,
        COUNT(CASE WHEN st_problema_rins = '1' THEN 1 END) AS problema_renal,
        COUNT(CASE WHEN st_problema_rins_insuficiencia = '1' THEN 1 END) AS insuficiencia_renal,
        COUNT(CASE WHEN st_problema_rins_outro = '1' THEN 1 END) AS outro_renal,
        COUNT(CASE WHEN st_problema_rins_nao_sabe = '1' THEN 1 END) AS nao_sabe_renal
    FROM tb_cadastro
    WHERE st_ativo = 1 and co_dim_tipo_saida_cadastro = '3'
    """

    query_filters = []
    params = {}

    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',')]
        query_filters.append("no_unidade_saude = ANY(:unidades)")
        params["unidades"] = unidade_saude_list

    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',')]
        query_filters.append("no_equipe = ANY(:equipes)")
        params["equipes"] = equipe_list

    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',')]
        query_filters.append("no_profissional = ANY(:profissionais)")
        params["profissionais"] = profissional_list

    if query_filters:
        base_query += " AND " + " AND ".join(query_filters)

    with engine.connect() as connection:
        result = connection.execute(text(base_query), params)
        data = result.fetchone()

        count_data = {
            'Responsável Familiar': data[0],
            'Frequenta Creche': data[1],
            'Frequenta Cuidador': data[2],
            'Participa de Grupo Comunitário': data[3],
            'Plano de Saúde Privado': data[4],
            'Deficiência': data[5],
            'Deficiência Auditiva': data[6],
            'Deficiência Intelectual/Cognitiva': data[7],
            'Deficiência Visual': data[8],
            'Deficiência Física': data[9],
            'Outra Deficiência': data[10],
            'Gestante': data[11],
            'Doença Respiratória': data[12],
            'Doença Respiratória (Asma)': data[13],
            'Doença Respiratória (DPOC/Enfisema)': data[14],
            'Outra Doença Respiratória': data[15],
            'Doença Respiratória (Não Sabe)': data[16],
            'Fumante': data[17],
            'Consome Álcool': data[18],
            'Consome Outra Droga': data[19],
            'Hipertensão Arterial': data[20],
            'Diabetes': data[21],
            'AVC': data[22],
            'Hanseníase': data[23],
            'Tuberculose': data[24],
            'Câncer': data[25],
            'Internação nos Últimos 12 Meses': data[26],
            'Tratamento Psiquiátrico': data[27],
            'Acamado': data[28],
            'Domiciliado': data[29],
            'Usa Planta Medicinal': data[30],
            'Doença Cardíaca': data[31],
            'Insuficiência Cardíaca': data[32],
            'Outra Doença Cardíaca': data[33],
            'Doença Cardíaca (Não Sabe)': data[34],
            'Problema Renal': data[35],
            'Insuficiência Renal': data[36],
            'Outro Problema Renal': data[37],
            'Problema Renal (Não Sabe)': data[38],
        }

    return jsonify(count_data)

@app.route('/configimport', methods=['GET'])
def get_last_import():
    with open('configimport.json', 'r') as f:
        config = json.load(f)
    return jsonify(config)

@app.route('/api/get-import-config', methods=['GET'])
def get_import_config():
    config_data = importdados.ensure_auto_update_config()
    return jsonify(config_data)

@app.route('/api/save-auto-update-config', methods=['POST'])
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

@app.route('/check-file/<import_type>', methods=['GET'])
def check_file(import_type):
    if importdados.is_file_available(import_type):
        return jsonify({"available": True})
    return jsonify({"available": False})

@app.route('/api/visitas-domiciliares', methods=['GET'])
def fetch_visitas_domiciliares():
    unidades = request.args.get('unidade_saude', default='', type=str)
    equipes = request.args.get('equipe', default='', type=str)
    profissionais = request.args.get('profissional', default='', type=str)
    start_date = request.args.get('start_date', default='', type=str)
    end_date = request.args.get('end_date', default='', type=str)
    tipo_consulta = request.args.get('tipo_consulta', default='filtros', type=str)

    engine = get_local_engine()
    query_filters = []
    params = {}

    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',')]
        if unidade_saude_list:
            query_filters.append("no_unidade_saude = ANY(:unidades)")
            params["unidades"] = unidade_saude_list
    
    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',')]
        if equipe_list:
            query_filters.append("no_equipe = ANY(:equipes)")
            params["equipes"] = equipe_list

    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',')]
        if profissional_list:
            query_filters.append("no_profissional = ANY(:profissionais)")
            params["profissionais"] = profissional_list

    if start_date and end_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            query_filters.append("dt_visita_mcaf BETWEEN :start_date AND :end_date")
            params["start_date"] = start
            params["end_date"] = end
        except ValueError:
            return jsonify({"error": "Data inválida"}), 400

    if tipo_consulta == 'filtros':
        filtros_query = """
        SELECT
            initcap(no_unidade_saude) AS no_unidade_saude,    
            initcap(no_profissional) AS no_profissional,      
            initcap(no_equipe) AS no_equipe
        FROM tb_visitas
        """

        if query_filters:
            filtros_query += " WHERE " + " AND ".join(query_filters)

        filtros_query += """
        GROUP BY no_unidade_saude, no_profissional, no_equipe
        """

        with engine.connect() as connection:
            result = connection.execute(text(filtros_query), params)
            data = []
            for row in result.fetchall():
                item = {
                    'no_unidade_saude': row._mapping['no_unidade_saude'],
                    'no_profissional': row._mapping['no_profissional'],
                    'no_equipe': row._mapping['no_equipe']
                }
                data.append(item)

        return jsonify(data)

    elif tipo_consulta == 'mapa':
        visitas_query = """
        SELECT
            nu_latitude,
            nu_longitude,
            initcap(no_unidade_saude) AS no_unidade_saude,    
            initcap(no_profissional) AS no_profissional,      
            initcap(no_equipe) AS no_equipe,
            co_dim_desfecho_visita,
            to_char(dt_visita_mcaf, 'DD/MM/YYYY') as dt_visita,
            sg_sexo,
            case when com_localizacao = '0' then 'Não' else 'Sim' end as com_localizacao
        FROM tb_visitas
        """

        if query_filters:
            visitas_query += " WHERE " + " AND ".join(query_filters)

        with engine.connect() as connection:
            result = connection.execute(text(visitas_query), params)
            data = []
            for row in result.fetchall():
                item = {
                    'nu_latitude': row._mapping['nu_latitude'],
                    'nu_longitude': row._mapping['nu_longitude'],
                    'no_unidade_saude': row._mapping['no_unidade_saude'],
                    'no_profissional': row._mapping['no_profissional'],
                    'no_equipe': row._mapping['no_equipe'],
                    'co_dim_desfecho_visita': row._mapping['co_dim_desfecho_visita'],
                    'dt_visita': row._mapping['dt_visita'],
                    'sg_sexo': row._mapping['sg_sexo'],
                    'com_localizacao': row._mapping['com_localizacao']
                }
                data.append(item)

        return jsonify(data)

    else:
        return jsonify({'error': 'tipo_consulta inválido. Use "filtros" ou "mapa".'}), 400

@app.route('/api/data', methods=['POST'])
def get_data():
    try:
        data = request.get_json()
        tipo = data.get('tipo')
        ano = data.get('ano')
        mes = data.get('mes')

        if not tipo or not ano or not mes:
            return jsonify({'error': 'Tipo, ano e mês são obrigatórios!'}), 400

        queries = {
            'iaf': '''
                SELECT 
                profissional,
                cbo, 
                nome_da_unidade, 
                total_de_participantes, 
                total_participantes_registrados, 
                total_de_atividades   
                FROM tb_iaf
                WHERE nu_ano = :ano AND nu_mes = :mes
            ''',
            'pse': '''
                SELECT 
                    inep,
                    nome_da_escola,
                    total_de_atividades,
                    indicador_1,
                    indicador_2
                FROM tb_pse
                WHERE ano = :ano AND mes = :mes
            ''',
            'pse_prof': '''
                SELECT 
                    profissional,
                    nome_cbo,
                    nome_da_unidade,
                    inep,
                    nome_da_escola,
                    total_de_participantes,
                    total_de_participantes_registrados,
                    total_de_atividades
                FROM tb_pse_prof
                WHERE ano = :ano AND mes = :mes
            '''
        }

        query = queries.get(tipo)
        if not query:
            return jsonify({'error': 'Tipo inválido!'}), 400

        with get_local_engine().connect() as conn:
            result = conn.execute(text(query), {'ano': ano, 'mes': mes})
            rows = result.fetchall()

            data_list = []
            columns = result.keys()

            for row in rows:
                row_dict = {}
                for col, value in zip(columns, row):
                    row_dict[col] = str(value) if not isinstance(value, (str, int, float, bool, type(None))) else value
                data_list.append(row_dict)

        return jsonify({
            'columns': list(columns),
            'data': data_list
        })

    except SQLAlchemyError as e:
        logger.error(f"Erro ao buscar os dados do banco de dados: {str(e)}")
        return jsonify({'error': 'Erro ao buscar os dados!'}), 500
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        return jsonify({'error': 'Erro inesperado!'}), 500

@app.route('/api/corrigir-ceps')
def corrigir_ceps():
    try:
        engine = get_local_engine()
        with engine.connect() as connection:
            Gerar_BPA.atualizar_enderecos(connection)
        return jsonify({"status": "ok"})
    except Exception as e:
        logger.error(f"Erro ao corrigir CEPs: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/atualizar-cep-escolhido', methods=['POST'])
def atualizar_cep_escolhido():
    data = request.json
    cep = data.get('cep')
    logradouro = data.get('logradouro')
    bairro = data.get('bairro')

    with get_local_engine().connect() as conn:
        update_query = text("""
            UPDATE tb_bpa
            SET prd_end_pcnte = :logradouro,
                prd_bairro_pcnte = :bairro
            WHERE prd_cep_pcnte = :cep
        """)
        conn.execute(update_query, {
            'logradouro': logradouro,
            'bairro': bairro,
            'cep': cep
        })
        conn.commit() # Added commit
    return jsonify({"status": "Atualizado com sucesso"})

# Inicialização do servidor
if __name__ == '__main__':
    with app.app_context():
        logger.info("Ensuring database schema exists...")
        # Create tables defined by raw SQL (tb_*, correios_ceps)
        # from sqlalchemy import create_engine # Already imported at the top
        
        # Use the URI from Flask config for the custom table creation engine
        # This ensures it uses the same database as SQLAlchemy's db.create_all()
        sql_alchemy_uri = app.config['SQLALCHEMY_DATABASE_URI']
        if sql_alchemy_uri.startswith('postgresql://'): # psycopg2 compatibility
            sql_alchemy_uri = sql_alchemy_uri.replace('postgresql://', 'postgresql+psycopg2://')

        custom_engine = create_engine(sql_alchemy_uri)
        create_custom_tables(custom_engine)
        logger.info("Custom tables checked/created.")

        # Create tables for SQLAlchemy models (Senha, Pessoas, Equipes, etc.)
        # Ensure all models are imported so db.metadata knows about them
        db.create_all()
        logger.info("SQLAlchemy model tables checked/created.")

        logger.info("Checking and populating 'correios_ceps' table if empty...")
        # Ensure the DNE_FILES_DIR exists before attempting to populate
        if not os.path.exists(DNE_FILES_DIR):
            logger.warning(f"DNE data directory not found: {DNE_FILES_DIR}")
            logger.warning("Skipping population of 'correios_ceps'. Please ensure DNE files are in the correct location.")
        else:
            db_uri_for_population = app.config['SQLALCHEMY_DATABASE_URI']
            if db_uri_for_population.startswith('postgresql://'): # Ensure psycopg2 compatibility
                db_uri_for_population = db_uri_for_population.replace('postgresql://', 'postgresql+psycopg2://')
            populate_correios_ceps_if_empty(db_uri_for_population, DNE_FILES_DIR)
            logger.info("'correios_ceps' table check/population complete.")

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