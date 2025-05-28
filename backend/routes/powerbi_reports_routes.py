# esus_project/backend/routes/powerbi_reports_routes.py
from Conexões import get_external_engine, get_local_engine # Added get_local_engine
from flask import Blueprint, jsonify, request
from sqlalchemy import text
import traceback # Import traceback for detailed error logging
from datetime import datetime, timedelta # Import datetime for date filtering

reports_bp = Blueprint("reports", __name__, url_prefix="/api/reports")

def execute_query(query_str, params={}):
    try:
        engine = get_external_engine()
        with engine.connect() as connection:
            result = connection.execute(text(query_str), params)
            if result.returns_rows:
                if result.rowcount == 1:
                    row = result.fetchone()
                    return dict(row._mapping) if row and hasattr(row, "_mapping") else (row if row else None)
                else:
                    return [dict(row._mapping) if hasattr(row, "_mapping") else row for row in result.fetchall()]
            else:
                return None # Should be result.rowcount or similar for non-returning queries
    except Exception as e:
        print(f"Error executing query on external DW: {e}")
        print(traceback.format_exc())
        return None

def execute_local_query(query_str, params={}):
    try:
        engine = get_local_engine()
        with engine.connect() as connection:
            result = connection.execute(text(query_str), params)
            if result.returns_rows:
                if result.rowcount == 1:
                    row = result.fetchone()
                    return dict(row._mapping) if row and hasattr(row, "_mapping") else (row if row else None)
                else:
                    return [dict(row._mapping) if hasattr(row, "_mapping") else row for row in result.fetchall()]
            else: 
                return {"rowcount": result.rowcount, "lastrowid": result.lastrowid if hasattr(result, 'lastrowid') else None}
    except Exception as e:
        print(f"Error executing query on local DB: {e}")
        print(traceback.format_exc())
        return None

@reports_bp.route("/cadastros_acs/summary", methods=["GET"])
def get_cadastros_summary():
    total_query = """
        SELECT COUNT(DISTINCT co_fat_cidadao_pec) as total_cadastros
        FROM tb_fat_cad_individual
        WHERE co_dim_tipo_saida_cadastro = 3;
    """
    total_result = execute_query(total_query)
    total_cadastros = total_result["total_cadastros"] if total_result else 0
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
    unidade_query = """
        SELECT us.no_unidade_saude, COUNT(DISTINCT ci.co_fat_cidadao_pec) as count
        FROM tb_fat_cad_individual ci
        JOIN tb_dim_unidade_saude us ON ci.co_dim_unidade_saude = us.co_seq_dim_unidade_saude
        WHERE ci.co_dim_tipo_saida_cadastro = 3
        GROUP BY us.no_unidade_saude
        ORDER BY count DESC;
    """
    dist_unidade = execute_query(unidade_query) or []
    equipe_query = """
        SELECT eq.no_equipe, COUNT(DISTINCT ci.co_fat_cidadao_pec) as count
        FROM tb_fat_cad_individual ci
        JOIN tb_dim_equipe eq ON ci.co_dim_equipe = eq.co_seq_dim_equipe
        WHERE ci.co_dim_tipo_saida_cadastro = 3
        GROUP BY eq.no_equipe
        ORDER BY count DESC;
    """
    dist_equipe = execute_query(equipe_query) or []
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
    unidade_query = """
        SELECT us.no_unidade_saude, COUNT(DISTINCT cd.co_seq_fat_cad_domiciliar) as count
        FROM tb_fat_cad_domiciliar cd
        JOIN tb_dim_unidade_saude us ON cd.co_dim_unidade_saude = us.co_seq_dim_unidade_saude
        GROUP BY us.no_unidade_saude
        ORDER BY count DESC;
    """
    dist_unidade = execute_query(unidade_query) or []
    equipe_query = """
        SELECT eq.no_equipe, COUNT(DISTINCT cd.co_seq_fat_cad_domiciliar) as count
        FROM tb_fat_cad_domiciliar cd
        JOIN tb_dim_equipe eq ON cd.co_dim_equipe = eq.co_seq_dim_equipe
        GROUP BY eq.no_equipe
        ORDER BY count DESC;
    """
    dist_equipe = execute_query(equipe_query) or []
    microarea_query = """
        SELECT cd.nu_micro_area, COUNT(DISTINCT cd.co_seq_fat_cad_domiciliar) as count
        FROM tb_fat_cad_domiciliar cd
        WHERE cd.nu_micro_area IS NOT NULL AND cd.nu_micro_area <> ''
        GROUP BY cd.nu_micro_area
        ORDER BY count DESC;
    """
    dist_microarea = execute_query(microarea_query) or []
    tipo_imovel_query = """
        SELECT ti.ds_tipo_imovel, COUNT(DISTINCT cd.co_seq_fat_cad_domiciliar) as count
        FROM tb_fat_cad_domiciliar cd
        JOIN tb_dim_tipo_imovel ti ON cd.co_dim_tipo_imovel = ti.co_seq_dim_tipo_imovel
        GROUP BY ti.ds_tipo_imovel
        ORDER BY count DESC;
    """
    dist_tipo_imovel = execute_query(tipo_imovel_query) or []
    situacao_moradia_query = """
        SELECT sm.ds_situacao_moradia, COUNT(DISTINCT cd.co_seq_fat_cad_domiciliar) as count
        FROM tb_fat_cad_domiciliar cd
        JOIN tb_dim_situacao_moradia sm ON cd.co_dim_situacao_moradia = sm.co_seq_dim_situacao_moradia
        GROUP BY sm.ds_situacao_moradia
        ORDER BY count DESC;
    """
    dist_situacao_moradia = execute_query(situacao_moradia_query) or []
    imoveis_data = {
        "distribuicao_unidade": dist_unidade,
        "distribuicao_equipe": dist_equipe,
        "distribuicao_microarea": dist_microarea,
        "distribuicao_tipo_imovel": dist_tipo_imovel,
        "distribuicao_situacao_moradia": dist_situacao_moradia,
    }
    return jsonify(imoveis_data)

@reports_bp.route("/visitas_acs/summary", methods=["GET"])
def get_visitas_summary():
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    if not start_date_str or not end_date_str:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=365)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
    params = {"start_date": start_date_str, "end_date": end_date_str}
    date_filter_clause = "AND t8.dt_registro BETWEEN :start_date AND :end_date"
    total_query = f"""
        SELECT COUNT(t0.co_seq_fat_visita_domiciliar) as total_visitas
        FROM tb_fat_visita_domiciliar t0
        JOIN tb_dim_tempo t8 ON t8.co_seq_dim_tempo = t0.co_dim_tempo
        JOIN tb_dim_cbo t1 ON t1.co_seq_dim_cbo = t0.co_dim_cbo
        WHERE t1.nu_cbo LIKE ANY (ARRAY['515105', '515140', '5151F1'])
        {date_filter_clause};
    """
    total_result = execute_query(total_query, params)
    total_visitas = total_result["total_visitas"] if total_result else 0
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

@reports_bp.route("/vacinas/summary", methods=["GET"])
def get_vacinas_summary():
    start_date_str = request.args.get("start_date") 
    end_date_str = request.args.get("end_date")    
    if not start_date_str or not end_date_str:
        end_date = datetime.now().date()
        start_date = datetime(2018, 1, 1).date() 
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
    params = {"start_date": start_date_str, "end_date": end_date_str}
    date_filter_clause = "AND to_date(to_char(t2.co_dim_tempo_vacina_aplicada, '99999999'), 'YYYYMMDD') BETWEEN :start_date AND :end_date"
    cidadaos_query = f"""
        SELECT COUNT(DISTINCT t1.co_fat_cidadao_pec) as total_cidadaos
        FROM tb_fat_vacinacao t1
        JOIN tb_fat_vacinacao_vacina t2 ON t2.co_fat_vacinacao = t1.co_seq_fat_vacinacao
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause};
    """
    cidadaos_result = execute_query(cidadaos_query, params)
    total_cidadaos = cidadaos_result["total_cidadaos"] if cidadaos_result else 0
    doses_query = f"""
        SELECT COUNT(t2.co_seq_fat_vacinacao_vacina) as total_doses
        FROM tb_fat_vacinacao_vacina t2
        WHERE t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause};
    """
    doses_result = execute_query(doses_query, params)
    total_doses = doses_result["total_doses"] if doses_result else 0
    transcricoes_query = f"""
        SELECT COUNT(t2.co_seq_fat_vacinacao_vacina) as total_transcricoes
        FROM tb_fat_vacinacao_vacina t2
        WHERE t2.st_registro_anterior = 1
        AND t2.co_dim_tempo_vacina_aplicada IS NOT NULL
        {date_filter_clause};
    """
    transcricoes_result = execute_query(transcricoes_query, params)
    total_transcricoes = transcricoes_result["total_transcricoes"] if transcricoes_result else 0
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
def _get_indicador_1_prenatal(period_start_date, period_end_date):
    # Placeholder
    return {"nome": "Gestantes com pelo menos 6 consultas pré-natal realizadas, sendo a 1ª até a 12ª semana de gestação", 
            "valor": 0, "numerador": 0, "denominador": 0, "meta": "60%", "status": "pending implementation"}

def _get_indicador_2_saude_bucal_gestantes(period_start_date, period_end_date):
    # Placeholder
    return {"nome": "Gestantes com atendimento odontológico realizado", 
            "valor": 0, "numerador": 0, "denominador": 0, "meta": "60%", "status": "pending implementation"}

def _get_indicador_3_citopatologico(period_start_date, period_end_date):
    # Placeholder
    return {"nome": "Mulheres com coleta de citopatológico na APS", 
            "valor": 0, "numerador": 0, "denominador": 0, "meta": "40%", "status": "pending implementation"}

def _get_indicador_4_vacina_polio_penta(period_start_date, period_end_date):
    # Placeholder
    return {"nome": "Crianças de 1 ano vacinadas contra Difteria, Tétano, Coqueluche, Hepatite B, infecções por haemophilus influenzae tipo b e Poliomielite inativada e oral", 
            "valor": 0, "numerador": 0, "denominador": 0, "meta": "95%", "status": "pending implementation"}

def _get_indicador_5_vacina_papiloma(period_start_date, period_end_date):
    # Placeholder, specific age range for HPV vaccine
    return {"nome": "Meninas de 9 a 14 anos vacinadas contra o Papilomavírus Humano (HPV)", 
            "valor": 0, "numerador": 0, "denominador": 0, "meta": "60%", "status": "pending implementation"}

def _get_indicador_6_hipertensao(period_start_date, period_end_date):
    query_denominador = """
        SELECT COUNT(DISTINCT tc.co_seq_fat_cad_individual) as denominador
        FROM tb_fat_cad_individual tc 
        WHERE tc.st_hipertensao = 1; 
    """ # Query uses tb_fat_cad_individual as per worker's last version
    query_numerador = f"""
        SELECT COUNT(DISTINCT fai.co_fat_cidadao_pec) as numerador
        FROM tb_fat_atend_individual fai
        JOIN tb_fat_cad_individual fci ON fai.co_fat_cidadao_pec = fci.co_fat_cidadao_pec 
        JOIN tb_fat_proced_atend fpa ON fai.co_seq_fat_atend_individual = fpa.co_fat_atend_individual
        JOIN tb_dim_procedimento dp ON fpa.co_dim_procedimento = dp.co_seq_dim_procedimento
        JOIN tb_dim_tempo dt ON fai.co_dim_tempo = dt.co_seq_dim_tempo
        WHERE fci.st_hipertensao = 1 
        AND dp.co_procedimento IN ('ABPG001', 'ABEX004', '0301100039') 
        AND dt.dt_registro BETWEEN '{period_start_date}' AND '{period_end_date}';
    """
    
    den_result = execute_local_query(query_denominador)
    num_result = execute_local_query(query_numerador)
    
    denominador = den_result["denominador"] if den_result and den_result.get("denominador") is not None else 0
    numerador = num_result["numerador"] if num_result and num_result.get("numerador") is not None else 0
    valor = (numerador / denominador * 100) if denominador > 0 else 0
    
    return {
        "nome": "Pessoas hipertensas com Pressão Arterial (PA) aferida no semestre", 
        "valor": round(valor, 2), 
        "numerador": numerador, 
        "denominador": denominador,
        "meta": "70%", 
        "status": "partial" 
    }

def _get_indicador_7_diabetes(period_start_date, period_end_date):
    return {"nome": "Pessoas com Diabetes Mellitus (DM) com solicitação de hemoglobina glicada no semestre", 
            "valor": 0, "numerador": 0, "denominador": 0, "meta": "50%", "status": "pending implementation"}

@reports_bp.route("/previne/summary", methods=["GET"])
def get_previne_summary(): 
    current_year = datetime.now().year
    # Definindo o período para o primeiro quadrimestre do ano atual para consistência.
    # Em um cenário real, isso seria mais dinâmico (ex: baseado na data atual ou parâmetros da requisição)
    period_name = f"1º Quadrimestre {current_year}" 
    period_start_date = f"{current_year}-01-01"
    period_end_date = f"{current_year}-04-30"
    
    # Para indicadores semestrais como Hipertensão (Ind6) e Diabetes (Ind7),
    # o semestre de referência precisa ser definido. Ex: S1 (Jan-Jun), S2 (Jul-Dec).
    # Se o quadrimestre atual for Q1 ou Q2, usa-se S1. Se Q3, usa-se S2.
    # Aqui, simplificaremos e usaremos as datas do quadrimestre para todos,
    # mas uma implementação real precisaria de lógica mais precisa para datas semestrais.
    semester_start_date = period_start_date # Simplificação
    semester_end_date = period_end_date   # Simplificação

    indicadores_data = [
        _get_indicador_1_prenatal(period_start_date, period_end_date),
        _get_indicador_2_saude_bucal_gestantes(period_start_date, period_end_date),
        _get_indicador_3_citopatologico(period_start_date, period_end_date),
        _get_indicador_4_vacina_polio_penta(period_start_date, period_end_date),
        _get_indicador_5_vacina_papiloma(period_start_date, period_end_date),
        _get_indicador_6_hipertensao(semester_start_date, semester_end_date), # Usando datas semestrais (simplificadas)
        _get_indicador_7_diabetes(semester_start_date, semester_end_date)    # Usando datas semestrais (simplificadas)
    ]
    
    valid_valores = [ind["valor"] for ind in indicadores_data if isinstance(ind["valor"], (int, float)) and ind.get("status") != "pending implementation"]
    
    if not valid_valores:
        isf_valor = 0.0
    else:
        isf_valor = round(sum(valid_valores) / len(valid_valores), 2)

    summary_data = {
        "periodo_apuracao": period_name,
        "isf_valor_municipal": isf_valor,
        "indicadores": indicadores_data
    }
    return jsonify(summary_data)

# Add more routes for other pages/reports here...
# e.g., /previne/ranking, /vacinas/busca_ativa_crianca, /vacinas/busca_ativa_covid
