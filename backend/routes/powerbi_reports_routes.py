# esus_project/backend/routes/powerbi_reports_routes.py
from Conexões import get_external_engine
from flask import Blueprint, jsonify, request
from init import db
from sqlalchemy import text, func, case
import traceback # Import traceback for detailed error logging
from datetime import datetime, timedelta # Import datetime for date filtering

# Import models if needed, or use raw SQL with text()
# from ..models import CidadaoPec, CadIndividual, UnidadeSaude, Equipe # Example model names

reports_bp = Blueprint("reports", __name__, url_prefix="/api/reports")

def execute_query(query_str, params={}):
    try:
        engine = get_external_engine()
        with engine.connect() as connection:
            result = connection.execute(text(query_str), params)
            if result.returns_rows and result.rowcount == 1:
                row = result.fetchone()
                if row:
                    return dict(row._mapping) if hasattr(row, "_mapping") else row
                else:
                    return None
            elif result.returns_rows:
                return [dict(row._mapping) for row in result.fetchall()]
            else:
                return None
    except Exception as e:
        print(f"Error executing query: {e}")
        print(traceback.format_exc())
        return None

# --- Cadastros ACS Report Endpoints --- 

@reports_bp.route("/cadastros_acs/summary", methods=["GET"])
def get_cadastros_summary():
    """ Endpoint to get summary data for Cadastros ACS dashboard (Page 1). """

    # Query 1: Total Cadastros Ativos (using tb_fat_cad_individual and checking status)
    # Assuming co_dim_tipo_saida_cadastro = 3 means ATIVO based on pasted_content.txt
    total_query = """
        SELECT COUNT(DISTINCT co_fat_cidadao_pec) as total_cadastros
        FROM tb_fat_cad_individual
        WHERE co_dim_tipo_saida_cadastro = 3;
    """
    total_result = execute_query(total_query)
    total_cadastros = total_result["total_cadastros"] if total_result else 0

    # Query 2: Cadastros com CPF e CNS (using tb_fat_cad_individual)
    # Assuming numeric 0 means no CPF/CNS based on pasted_content.txt
    docs_query = """
        SELECT
            COUNT(DISTINCT co_fat_cidadao_pec) FILTER (WHERE nu_cpf_cidadao IS NOT NULL AND nu_cpf_cidadao <> '0') as com_cpf,
            COUNT(DISTINCT co_fat_cidadao_pec) FILTER (WHERE nu_cns IS NOT NULL AND nu_cns <> '0') as com_cns
        FROM tb_fat_cad_individual
        WHERE co_dim_tipo_saida_cadastro = 3;
    """
    docs_result = execute_query(docs_query)
    com_cpf = docs_result["com_cpf"] if docs_result else 0
    com_cns = docs_result["com_cns"] if docs_result else 0

    percent_cpf = (com_cpf / total_cadastros * 100) if total_cadastros > 0 else 0
    percent_cns = (com_cns / total_cadastros * 100) if total_cadastros > 0 else 0

    # Query 3: Distribuição por Unidade (Join tb_fat_cad_individual with tb_dim_unidade_saude)
    unidade_query = """
        SELECT us.no_unidade_saude, COUNT(DISTINCT ci.co_fat_cidadao_pec) as count
        FROM tb_fat_cad_individual ci
        JOIN tb_dim_unidade_saude us ON ci.co_dim_unidade_saude = us.co_seq_dim_unidade_saude
        WHERE ci.co_dim_tipo_saida_cadastro = 3
        GROUP BY us.no_unidade_saude
        ORDER BY count DESC;
    """
    dist_unidade = execute_query(unidade_query) or []

    # Query 4: Distribuição por Equipe (Join tb_fat_cad_individual with tb_dim_equipe)
    equipe_query = """
        SELECT eq.no_equipe, COUNT(DISTINCT ci.co_fat_cidadao_pec) as count
        FROM tb_fat_cad_individual ci
        JOIN tb_dim_equipe eq ON ci.co_dim_equipe = eq.co_seq_dim_equipe
        WHERE ci.co_dim_tipo_saida_cadastro = 3
        GROUP BY eq.no_equipe
        ORDER BY count DESC;
    """
    dist_equipe = execute_query(equipe_query) or []

    # Query 5: Distribuição por Microárea (Group by nu_micro_area in tb_fat_cad_individual)
    microarea_query = """
        SELECT ci.nu_micro_area, COUNT(DISTINCT ci.co_fat_cidadao_pec) as count
        FROM tb_fat_cad_individual ci
        WHERE ci.co_dim_tipo_saida_cadastro = 3 AND ci.nu_micro_area IS NOT NULL AND ci.nu_micro_area <> ''
        GROUP BY ci.nu_micro_area
        ORDER BY count DESC;
    """
    dist_microarea = execute_query(microarea_query) or []

    summary_data = {
        "total_cadastros": total_cadastros,
        "percent_cpf": round(percent_cpf, 2),
        "percent_cns": round(percent_cns, 2),
        "distribuicao_unidade": dist_unidade,
        "distribuicao_equipe": dist_equipe,
        "distribuicao_microarea": dist_microarea
    }

    return jsonify(summary_data)

@reports_bp.route("/cadastros_acs/imoveis", methods=["GET"])
def get_cadastros_imoveis():
    """ Endpoint to get data for Cadastros ACS - Imóveis page (Page 2). """
    # Note: This requires tb_fat_cad_domiciliar table.

    # Query 1: Distribuição por Unidade
    unidade_query = """
        SELECT us.no_unidade_saude, COUNT(DISTINCT cd.co_seq_fat_cad_domiciliar) as count
        FROM tb_fat_cad_domiciliar cd
        JOIN tb_dim_unidade_saude us ON cd.co_dim_unidade_saude = us.co_seq_dim_unidade_saude
        -- Add filter for active domiciles if applicable
        GROUP BY us.no_unidade_saude
        ORDER BY count DESC;
    """
    dist_unidade = execute_query(unidade_query) or []

    # Query 2: Distribuição por Equipe
    equipe_query = """
        SELECT eq.no_equipe, COUNT(DISTINCT cd.co_seq_fat_cad_domiciliar) as count
        FROM tb_fat_cad_domiciliar cd
        JOIN tb_dim_equipe eq ON cd.co_dim_equipe = eq.co_seq_dim_equipe
        GROUP BY eq.no_equipe
        ORDER BY count DESC;
    """
    dist_equipe = execute_query(equipe_query) or []

    # Query 3: Distribuição por Microárea
    microarea_query = """
        SELECT cd.nu_micro_area, COUNT(DISTINCT cd.co_seq_fat_cad_domiciliar) as count
        FROM tb_fat_cad_domiciliar cd
        WHERE cd.nu_micro_area IS NOT NULL AND cd.nu_micro_area <> ''
        GROUP BY cd.nu_micro_area
        ORDER BY count DESC;
    """
    dist_microarea = execute_query(microarea_query) or []

    # Query 4: Tipo de Imóvel (using tb_dim_tipo_imovel)
    tipo_imovel_query = """
        SELECT ti.ds_tipo_imovel, COUNT(DISTINCT cd.co_seq_fat_cad_domiciliar) as count
        FROM tb_fat_cad_domiciliar cd
        JOIN tb_dim_tipo_imovel ti ON cd.co_dim_tipo_imovel = ti.co_seq_dim_tipo_imovel
        GROUP BY ti.ds_tipo_imovel
        ORDER BY count DESC;
    """
    dist_tipo_imovel = execute_query(tipo_imovel_query) or []

    # Query 5: Situação de Moradia (using tb_dim_situacao_moradia)
    situacao_moradia_query = """
        SELECT sm.ds_situacao_moradia, COUNT(DISTINCT cd.co_seq_fat_cad_domiciliar) as count
        FROM tb_fat_cad_domiciliar cd
        JOIN tb_dim_situacao_moradia sm ON cd.co_dim_situacao_moradia = sm.co_seq_dim_situacao_moradia
        GROUP BY sm.ds_situacao_moradia
        ORDER BY count DESC;
    """
    dist_situacao_moradia = execute_query(situacao_moradia_query) or []

    # Add more queries for other charts/data points on this page (localização, tipo acesso, abastecimento, etc.)

    imoveis_data = {
        "distribuicao_unidade": dist_unidade,
        "distribuicao_equipe": dist_equipe,
        "distribuicao_microarea": dist_microarea,
        "distribuicao_tipo_imovel": dist_tipo_imovel,
        "distribuicao_situacao_moradia": dist_situacao_moradia,
        # Add results from other queries here
    }

    return jsonify(imoveis_data)

# --- Visitas ACS Report Endpoints --- 

@reports_bp.route("/visitas_acs/summary", methods=["GET"])
def get_visitas_summary():
    """ Endpoint to get summary data for Visitas ACS dashboard (Page 1). """
    # Based on pasted_content_2.txt and Power BI report view
    
    # Get date range filters from query parameters (optional)
    start_date_str = request.args.get("start_date") # Expected format YYYY-MM-DD
    end_date_str = request.args.get("end_date")     # Expected format YYYY-MM-DD

    # Default to last 12 months if no dates provided
    if not start_date_str or not end_date_str:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=365)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
    
    params = {"start_date": start_date_str, "end_date": end_date_str}
    date_filter_clause = "AND t8.dt_registro BETWEEN :start_date AND :end_date"

    # Query 1: Total Visitas (Domiciliares + Territoriais? Check PowerBI logic)
    # Assuming tb_fat_visita_domiciliar covers both or needs join with territorial visits table
    total_query = f"""
        SELECT COUNT(t0.co_seq_fat_visita_domiciliar) as total_visitas
        FROM tb_fat_visita_domiciliar t0
        JOIN tb_dim_tempo t8 ON t8.co_seq_dim_tempo = t0.co_dim_tempo
        JOIN tb_dim_cbo t1 ON t1.co_seq_dim_cbo = t0.co_dim_cbo
        WHERE t1.nu_cbo LIKE ANY (ARRAY['515105', '515140', '5151F1']) -- ACS/ACE CBOs
        {date_filter_clause};
    """
    total_result = execute_query(total_query, params)
    total_visitas = total_result["total_visitas"] if total_result else 0

    # Query 2: Visitas Identificadas vs Não Identificadas (Based on CNS/CPF presence)
    identificadas_query = f"""
        SELECT 
            COUNT(t0.co_seq_fat_visita_domiciliar) FILTER (WHERE t0.nu_cns IS NOT NULL OR t0.nu_cpf_cidadao IS NOT NULL) as visitas_identificadas,
            COUNT(t0.co_seq_fat_visita_domiciliar) FILTER (WHERE t0.nu_cns IS NULL AND t0.nu_cpf_cidadao IS NULL) as visitas_nao_identificadas
        FROM tb_fat_visita_domiciliar t0
        JOIN tb_dim_tempo t8 ON t8.co_seq_dim_tempo = t0.co_dim_tempo
        JOIN tb_dim_cbo t1 ON t1.co_seq_dim_cbo = t0.co_dim_cbo
        WHERE t1.nu_cbo LIKE ANY (ARRAY['515105', '515140', '5151F1'])
        {date_filter_clause};
    """
    identificadas_result = execute_query(identificadas_query, params)
    visitas_identificadas = identificadas_result["visitas_identificadas"] if identificadas_result else 0
    visitas_nao_identificadas = identificadas_result["visitas_nao_identificadas"] if identificadas_result else 0

    # Query 3: Distribuição por Unidade
    unidade_query = f"""
        SELECT us.no_unidade_saude, COUNT(t0.co_seq_fat_visita_domiciliar) as count
        FROM tb_fat_visita_domiciliar t0
        JOIN tb_dim_unidade_saude us ON t0.co_dim_unidade_saude = us.co_seq_dim_unidade_saude
        JOIN tb_dim_tempo t8 ON t8.co_seq_dim_tempo = t0.co_dim_tempo
        JOIN tb_dim_cbo t1 ON t1.co_seq_dim_cbo = t0.co_dim_cbo
        WHERE t1.nu_cbo LIKE ANY (ARRAY['515105', '515140', '5151F1'])
        {date_filter_clause}
        GROUP BY us.no_unidade_saude
        ORDER BY count DESC;
    """
    dist_unidade = execute_query(unidade_query, params) or []

    # Query 4: Distribuição por Equipe
    equipe_query = f"""
        SELECT eq.no_equipe, COUNT(t0.co_seq_fat_visita_domiciliar) as count
        FROM tb_fat_visita_domiciliar t0
        JOIN tb_dim_equipe eq ON t0.co_dim_equipe = eq.co_seq_dim_equipe
        JOIN tb_dim_tempo t8 ON t8.co_seq_dim_tempo = t0.co_dim_tempo
        JOIN tb_dim_cbo t1 ON t1.co_seq_dim_cbo = t0.co_dim_cbo
        WHERE t1.nu_cbo LIKE ANY (ARRAY['515105', '515140', '5151F1'])
        {date_filter_clause}
        GROUP BY eq.no_equipe
        ORDER BY count DESC;
    """
    dist_equipe = execute_query(equipe_query, params) or []

    # Query 5: Distribuição por Microárea
    microarea_query = f"""
        SELECT t0.nu_micro_area, COUNT(t0.co_seq_fat_visita_domiciliar) as count
        FROM tb_fat_visita_domiciliar t0
        JOIN tb_dim_tempo t8 ON t8.co_seq_dim_tempo = t0.co_dim_tempo
        JOIN tb_dim_cbo t1 ON t1.co_seq_dim_cbo = t0.co_dim_cbo
        WHERE t1.nu_cbo LIKE ANY (ARRAY['515105', '515140', '5151F1'])
        AND t0.nu_micro_area IS NOT NULL AND t0.nu_micro_area <> ''
        {date_filter_clause}
        GROUP BY t0.nu_micro_area
        ORDER BY count DESC;
    """
    dist_microarea = execute_query(microarea_query, params) or []

    summary_data = {
        "total_visitas": total_visitas,
        "visitas_identificadas": visitas_identificadas,
        "visitas_nao_identificadas": visitas_nao_identificadas,
        "distribuicao_unidade": dist_unidade,
        "distribuicao_equipe": dist_equipe,
        "distribuicao_microarea": dist_microarea,
        "start_date": start_date_str,
        "end_date": end_date_str
    }

    return jsonify(summary_data)

# --- Vacinação Report Endpoints --- 

@reports_bp.route("/vacinas/summary", methods=["GET"])
def get_vacinas_summary():
    """ Endpoint to get summary data for Vacinação dashboard (Page 1). """
    # Based on pasted_content.txt (Select Vacina Delta) and Power BI report view

    # Get date range filters from query parameters (optional)
    start_date_str = request.args.get("start_date") # Expected format YYYY-MM-DD
    end_date_str = request.args.get("end_date")     # Expected format YYYY-MM-DD

    # Default to a wide range if no dates provided (adjust as needed)
    if not start_date_str or not end_date_str:
        end_date = datetime.now().date()
        start_date = datetime(2018, 1, 1).date() # Example start date
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

    params = {"start_date": start_date_str, "end_date": end_date_str}
    # Assuming dt_aplicacao is the relevant date field from tb_fat_vacinacao_vacina
    date_filter_clause = "AND to_date(to_char(t2.co_dim_tempo_vacina_aplicada, '99999999'), 'YYYYMMDD') BETWEEN :start_date AND :end_date"

    # Query 1: Cidadãos Atendidos (Distinct citizens with vaccination records in period)
    cidadaos_query = f"""
        SELECT COUNT(DISTINCT t1.co_fat_cidadao_pec) as total_cidadaos
        FROM tb_fat_vacinacao t1
        JOIN tb_fat_vacinacao_vacina t2 ON t2.co_fat_vacinacao = t1.co_seq_fat_vacinacao
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause};
    """
    cidadaos_result = execute_query(cidadaos_query, params)
    total_cidadaos = cidadaos_result["total_cidadaos"] if cidadaos_result else 0

    # Query 2: Doses Aplicadas (Total vaccine records in period)
    doses_query = f"""
        SELECT COUNT(t2.co_seq_fat_vacinacao_vacina) as total_doses
        FROM tb_fat_vacinacao_vacina t2
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause};
    """
    doses_result = execute_query(doses_query, params)
    total_doses = doses_result["total_doses"] if doses_result else 0

    # Query 3: Transcrições Realizadas (st_registro_anterior = 1)
    transcricoes_query = f"""
        SELECT COUNT(t2.co_seq_fat_vacinacao_vacina) as total_transcricoes
        FROM tb_fat_vacinacao_vacina t2
        WHERE t2.st_registro_anterior = 1
        AND t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause};
    """
    transcricoes_result = execute_query(transcricoes_query, params)
    total_transcricoes = transcricoes_result["total_transcricoes"] if transcricoes_result else 0

    # Query 4: Distribuição por Turno (tb_dim_turno)
    turno_query = f"""
        SELECT t15.ds_turno, COUNT(t2.co_seq_fat_vacinacao_vacina) as count
        FROM tb_fat_vacinacao t1
        JOIN tb_fat_vacinacao_vacina t2 ON t2.co_fat_vacinacao = t1.co_seq_fat_vacinacao
        LEFT JOIN tb_dim_turno t15 ON t15.co_seq_dim_turno = t1.co_dim_turno
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause}
        GROUP BY t15.ds_turno
        ORDER BY count DESC;
    """
    dist_turno = execute_query(turno_query, params) or []

    # Query 5: Distribuição por Sexo (tb_dim_sexo)
    sexo_query = f"""
        SELECT 
            CASE t1.co_dim_sexo WHEN 1 THEN 'M' WHEN 2 THEN 'F' ELSE 'N/A' END as sexo,
            COUNT(t2.co_seq_fat_vacinacao_vacina) as count
        FROM tb_fat_vacinacao t1
        JOIN tb_fat_vacinacao_vacina t2 ON t2.co_fat_vacinacao = t1.co_seq_fat_vacinacao
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause}
        GROUP BY sexo
        ORDER BY count DESC;
    """
    dist_sexo = execute_query(sexo_query, params) or []

    # Query 6: Distribuição por Local de Atendimento (tb_dim_local_atendimento)
    local_query = f"""
        SELECT t18.ds_local_atendimento, COUNT(t2.co_seq_fat_vacinacao_vacina) as count
        FROM tb_fat_vacinacao t1
        JOIN tb_fat_vacinacao_vacina t2 ON t2.co_fat_vacinacao = t1.co_seq_fat_vacinacao
        LEFT JOIN tb_dim_local_atendimento t18 ON t18.co_seq_dim_local_atendimento = t1.co_dim_local_atendimento
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause}
        GROUP BY t18.ds_local_atendimento
        ORDER BY count DESC;
    """
    dist_local = execute_query(local_query, params) or []

    # Query 7: Distribuição por Origem (tb_dim_tipo_origem)
    origem_query = f"""
        SELECT 
            CASE t10.nu_identificador 
                 WHEN '-' THEN 'NÃO INFORMADO' WHEN '0' THEN 'CDS OFFLINE' WHEN '1' THEN 'CDS ONLINE' 
                 WHEN '2' THEN 'PEC' WHEN '3' THEN 'EXTERNO/TERCEIRO' WHEN '4' THEN 'e-SUS TERRITÓRIO' 
                 WHEN '5' THEN 'APP ATIVIDADE COLETIVA' WHEN '6' THEN 'APP VACINAÇÃO' 
                 ELSE 'OUTRO' 
            END AS origem_dados,
            COUNT(t2.co_seq_fat_vacinacao_vacina) as count
        FROM tb_fat_vacinacao t1
        JOIN tb_fat_vacinacao_vacina t2 ON t2.co_fat_vacinacao = t1.co_seq_fat_vacinacao
        LEFT JOIN tb_dim_tipo_origem t10 ON t10.co_seq_dim_tipo_origem = t1.co_dim_cds_tipo_origem
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause}
        GROUP BY origem_dados
        ORDER BY count DESC;
    """
    dist_origem = execute_query(origem_query, params) or []

    # Query 8: Distribuição por Estratégia (tb_dim_estrategia_vacinacao)
    estrategia_query = f"""
        SELECT t19.no_estrategia_vacinacao, COUNT(t2.co_seq_fat_vacinacao_vacina) as count
        FROM tb_fat_vacinacao_vacina t2
        LEFT JOIN tb_dim_estrategia_vacinacao t19 ON t19.co_seq_dim_estrategia_vacnacao = t2.co_dim_estrategia_vacinacao
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause}
        GROUP BY t19.no_estrategia_vacinacao
        ORDER BY count DESC;
    """
    dist_estrategia = execute_query(estrategia_query, params) or []

    # Query 9: Vacinas por Unidade
    vac_unidade_query = f"""
        SELECT t8_us.no_unidade_saude, COUNT(t2.co_seq_fat_vacinacao_vacina) as count
        FROM tb_fat_vacinacao t1
        JOIN tb_fat_vacinacao_vacina t2 ON t2.co_fat_vacinacao = t1.co_seq_fat_vacinacao
        LEFT JOIN tb_dim_unidade_saude t8_us ON t8_us.co_seq_dim_unidade_saude = t1.co_dim_unidade_saude
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause}
        GROUP BY t8_us.no_unidade_saude
        ORDER BY count DESC;
    """
    dist_vac_unidade = execute_query(vac_unidade_query, params) or []

    # Query 10: Vacinas por Equipe
    vac_equipe_query = f"""
        SELECT t9_eq.no_equipe, COUNT(t2.co_seq_fat_vacinacao_vacina) as count
        FROM tb_fat_vacinacao t1
        JOIN tb_fat_vacinacao_vacina t2 ON t2.co_fat_vacinacao = t1.co_seq_fat_vacinacao
        LEFT JOIN tb_dim_equipe t9_eq ON t9_eq.co_seq_dim_equipe = t1.co_dim_equipe
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause}
        GROUP BY t9_eq.no_equipe
        ORDER BY count DESC;
    """
    dist_vac_equipe = execute_query(vac_equipe_query, params) or []

    summary_data = {
        "total_cidadaos": total_cidadaos,
        "total_doses": total_doses,
        "total_transcricoes": total_transcricoes,
        "distribuicao_turno": dist_turno,
        "distribuicao_sexo": dist_sexo,
        "distribuicao_local": dist_local,
        "distribuicao_origem": dist_origem,
        "distribuicao_estrategia": dist_estrategia,
        "distribuicao_vac_unidade": dist_vac_unidade,
        "distribuicao_vac_equipe": dist_vac_equipe,
        "start_date": start_date_str,
        "end_date": end_date_str
    }

    return jsonify(summary_data)

# --- Previne Brasil Report Endpoints --- 

@reports_bp.route("/previne/summary", methods=["GET"])
def get_previne_summary():
    """ Endpoint to get summary data for Previne Brasil dashboard (Page 1). """
    # Placeholder implementation - Requires specific indicator logic and queries
    
    # Define target period (e.g., current quadrimestre or based on request args)
    # For simplicity, using placeholder values
    quad = "3º Quadrimestre 2024"
    
    # Placeholder calculations for each indicator (Numerator / Denominator * 100)
    # These queries need to be developed based on official Previne Brasil technical notes
    # and the e-SUS AB DW schema.
    
    # Example Placeholder Structure:
    indicadores = {
        "i1_prenatal": {"nome": "Pré-Natal (6 consultas, 1ª até 12ª sem)", "valor": 75.5, "numerador": 755, "denominador": 1000}, # Placeholder
        "i2_saude_bucal": {"nome": "Saúde Bucal de Gestantes", "valor": 60.2, "numerador": 602, "denominador": 1000}, # Placeholder
        "i3_citopatologico": {"nome": "Citopatológico", "valor": 55.0, "numerador": 5500, "denominador": 10000}, # Placeholder
        "i4_vacina_polio_penta": {"nome": "Vacinação (Poliomielite e Pentavalente)", "valor": 95.1, "numerador": 951, "denominador": 1000}, # Placeholder
        "i5_vacina_papiloma": {"nome": "Vacinação Papilomavírus Humano", "valor": 80.0, "numerador": 800, "denominador": 1000}, # Placeholder
        "i6_hipertensao": {"nome": "Hipertensão (PA aferida)", "valor": 88.9, "numerador": 4445, "denominador": 5000}, # Placeholder
        "i7_diabetes": {"nome": "Diabetes (Hemoglobina Glicada)", "valor": 70.3, "numerador": 1406, "denominador": 2000} # Placeholder
    }
    
    # Placeholder ISF calculation (Simple average for demonstration)
    isf_valor = round(sum(ind["valor"] for ind in indicadores.values()) / len(indicadores), 2)
    
    summary_data = {
        "periodo": quad,
        "isf": isf_valor,
        "indicadores": indicadores
    }
    
    return jsonify(summary_data)

# Add more routes for other pages/reports here...
# e.g., /previne/ranking, /vacinas/busca_ativa_crianca, /vacinas/busca_ativa_covid

