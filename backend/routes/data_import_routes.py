from flask import Blueprint, request, jsonify
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import json
import logging

# Assuming get_local_engine is in ..Conexões
from ..Conexões import get_local_engine
# Assuming importdados.py is in the parent directory (backend)
# and is discoverable in sys.path or via relative import
from .. import importdados 

data_import_bp = Blueprint('data_import_api', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)

@data_import_bp.route('/configimport', methods=['GET']) # Path changed
def get_last_import():
    try:
        # Assuming configimport.json is at the root of the application, where app.py is.
        # This path might need adjustment if the working directory for blueprint routes is different.
        with open('configimport.json', 'r') as f:
            config = json.load(f)
        return jsonify(config)
    except FileNotFoundError:
        logger.error("File 'configimport.json' not found.")
        return jsonify({"error": "Configuration file not found"}), 404
    except json.JSONDecodeError:
        logger.error("Error decoding 'configimport.json'.")
        return jsonify({"error": "Error reading configuration file"}), 500

@data_import_bp.route('/check-file/<import_type>', methods=['GET']) # Path changed
def check_file(import_type):
    # importdados is imported from the parent directory
    if importdados.is_file_available(import_type):
        return jsonify({"available": True})
    return jsonify({"available": False})

@data_import_bp.route('/visitas-domiciliares', methods=['GET'])
def fetch_visitas_domiciliares():
    unidades = request.args.get('unidade_saude', default='', type=str)
    equipes = request.args.get('equipe', default='', type=str)
    profissionais = request.args.get('profissional', default='', type=str)
    start_date_str = request.args.get('start_date', default='', type=str) # Renamed to avoid conflict
    end_date_str = request.args.get('end_date', default='', type=str)     # Renamed to avoid conflict
    tipo_consulta = request.args.get('tipo_consulta', default='filtros', type=str)

    engine = get_local_engine()
    query_filters = []
    params = {}

    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',') if u.strip()]
        if unidade_saude_list: query_filters.append("no_unidade_saude = ANY(:unidades)"); params["unidades"] = unidade_saude_list
    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',') if e.strip()]
        if equipe_list: query_filters.append("no_equipe = ANY(:equipes)"); params["equipes"] = equipe_list
    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',') if p.strip()]
        if profissional_list: query_filters.append("no_profissional = ANY(:profissionais)"); params["profissionais"] = profissional_list

    if start_date_str and end_date_str:
        try:
            start_datetime = datetime.strptime(start_date_str, "%Y-%m-%d") # Renamed variable
            end_datetime = datetime.strptime(end_date_str, "%Y-%m-%d")     # Renamed variable
            query_filters.append("dt_visita_mcaf BETWEEN :start_date AND :end_date")
            params["start_date"] = start_datetime
            params["end_date"] = end_datetime
        except ValueError:
            logger.error(f"Data inválida fornecida: start_date='{start_date_str}', end_date='{end_date_str}'")
            return jsonify({"error": "Data inválida"}), 400

    base_query_select = ""
    if tipo_consulta == 'filtros':
        base_query_select = "SELECT initcap(no_unidade_saude) AS no_unidade_saude, initcap(no_profissional) AS no_profissional, initcap(no_equipe) AS no_equipe FROM tb_visitas"
        if query_filters: base_query_select += " WHERE " + " AND ".join(query_filters)
        base_query_select += " GROUP BY no_unidade_saude, no_profissional, no_equipe"
    elif tipo_consulta == 'mapa':
        base_query_select = "SELECT nu_latitude, nu_longitude, initcap(no_unidade_saude) AS no_unidade_saude, initcap(no_profissional) AS no_profissional, initcap(no_equipe) AS no_equipe, co_dim_desfecho_visita, to_char(dt_visita_mcaf, 'DD/MM/YYYY') as dt_visita, sg_sexo, case when com_localizacao = '0' then 'Não' else 'Sim' end as com_localizacao FROM tb_visitas"
        if query_filters: base_query_select += " WHERE " + " AND ".join(query_filters)
    else:
        return jsonify({'error': 'tipo_consulta inválido. Use "filtros" ou "mapa".'}), 400

    try:
        with engine.connect() as connection:
            result = connection.execute(text(base_query_select), params)
            data = [dict(row._mapping) for row in result.fetchall()] # Convert rows to dicts
        return jsonify(data)
    except Exception as e:
        logger.error(f"Erro ao buscar visitas domiciliares: {e}")
        return jsonify({"error": f"Erro ao buscar dados: {str(e)}"}), 500

@data_import_bp.route('/data', methods=['POST'])
def get_data():
    try:
        data_req = request.get_json() # Renamed to avoid conflict with data variable for response
        tipo = data_req.get('tipo')
        ano = data_req.get('ano')
        mes = data_req.get('mes')

        if not tipo or not ano or not mes:
            return jsonify({'error': 'Tipo, ano e mês são obrigatórios!'}), 400

        queries = {
            'iaf': "SELECT profissional, cbo, nome_da_unidade, total_de_participantes, total_participantes_registrados, total_de_atividades FROM tb_iaf WHERE nu_ano = :ano AND nu_mes = :mes",
            'pse': "SELECT inep, nome_da_escola, total_de_atividades, indicador_1, indicador_2 FROM tb_pse WHERE ano = :ano AND mes = :mes",
            'pse_prof': "SELECT profissional, nome_cbo, nome_da_unidade, inep, nome_da_escola, total_de_participantes, total_de_participantes_registrados, total_de_atividades FROM tb_pse_prof WHERE ano = :ano AND mes = :mes"
        }
        query = queries.get(tipo)
        if not query: return jsonify({'error': 'Tipo inválido!'}), 400

        with get_local_engine().connect() as conn:
            result = conn.execute(text(query), {'ano': ano, 'mes': mes})
            rows = result.fetchall()
            columns = result.keys()
            data_list = []
            for row in rows:
                row_dict = {}
                for col_idx, col_name in enumerate(columns): # Use enumerate for index if needed, or direct mapping
                    row_dict[col_name] = str(row[col_idx]) if not isinstance(row[col_idx], (str, int, float, bool, type(None))) else row[col_idx]
                data_list.append(row_dict)
        return jsonify({'columns': list(columns), 'data': data_list})
    except SQLAlchemyError as e:
        logger.error(f"Erro ao buscar os dados do banco de dados para o tipo '{tipo}': {str(e)}")
        return jsonify({'error': 'Erro ao buscar os dados!'}), 500
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar dados para o tipo '{tipo}': {str(e)}")
        return jsonify({'error': 'Erro inesperado!'}), 500
