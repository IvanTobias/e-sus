from flask import Blueprint, request, jsonify
from sqlalchemy import text
import logging

# Imports from the project structure (..)
from ..Conexões import get_local_engine
# Import cache from app.py where it's initialized
from ..app import cache # Adjusted import for cache

stats_bp = Blueprint('stats_api', __name__, url_prefix='/api')
logger = logging.getLogger(__name__) # Local logger for this blueprint

@stats_bp.route('/contagens', methods=['GET'])
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
        condition_str = " AND " + " AND ".join(query_filters)
        base_query_geral += condition_str
        domicilio_query += condition_str # Assuming same filters apply

    with engine.connect() as connection:
        result_geral = connection.execute(text(base_query_geral), params)
        counts_geral_row = result_geral.fetchone() # Use fetchone() if expecting one row
        
        result_domicilio = connection.execute(text(domicilio_query), params)
        counts_domicilio_row = result_domicilio.fetchone()

    counts_dict = {
        "Cadastros Individuais": counts_geral_row[0] if counts_geral_row else 0,
        "Moradores de Rua": counts_geral_row[1] if counts_geral_row else 0,
        "Óbitos": counts_geral_row[2] if counts_geral_row else 0,
        "Mudou-se": counts_geral_row[3] if counts_geral_row else 0,
        "Cadastros Ativos": counts_geral_row[4] if counts_geral_row else 0,
        "Fora de Área": counts_geral_row[5] if counts_geral_row else 0,
        "Cadastros Desatualizados": counts_geral_row[6] if counts_geral_row else 0,
        "Cadastros com Cns": counts_geral_row[7] if counts_geral_row else 0,
        "Cadastros com Cpf": counts_geral_row[8] if counts_geral_row else 0,
        "Cadastros Domiciliares": counts_domicilio_row[0] if counts_domicilio_row else 0
    }
    return jsonify(counts_dict)

@stats_bp.route('/unidades-saude', methods=['GET'])
@cache.cached(timeout=300) # cache object imported from ..app
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
        data = [{'unidadeSaude': row._mapping['no_unidade_saude'], 'equipe': row._mapping['no_equipe'], 'profissional': row._mapping['no_profissional']} for row in result.fetchall()]
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error in fetchUnidadesSaude: {e}")
        return jsonify({'error': str(e)}), 500

@stats_bp.route('/detalhes', methods=['GET'])
def fetch_detalhes():
    tipo = request.args.get('tipo', default='', type=str).strip().lower()
    unidades = request.args.get('unidade_saude', default='', type=str).strip()
    equipes = request.args.get('equipe', default='', type=str).strip()
    profissionais = request.args.get('profissional', default='', type=str).strip()

    engine = get_local_engine()
    query_filters = []
    params = {}

    tipo_map = {
        'responsável familiar': 'st_responsavel_familiar', 'frequenta creche': 'st_frequenta_creche',
        'frequenta cuidador': 'st_frequenta_cuidador', 'participa de grupo comunitário': 'st_participa_grupo_comunitario',
        'plano de saúde privado': 'st_plano_saude_privado', 'deficiência': 'st_deficiencia',
        'deficiência auditiva': 'st_defi_auditiva', 'deficiência intelectual/cognitiva': 'st_defi_intelectual_cognitiva',
        'deficiência visual': 'st_defi_visual', 'deficiência física': 'st_defi_fisica',
        'outra deficiência': 'st_defi_outra', 'gestante': 'st_gestante',
        'doença respiratória': 'st_doenca_respiratoria', 'doença respiratória (asma)': 'st_doenca_respira_asma',
        'doença respiratória (dpoc/enfisema)': 'st_doenca_respira_dpoc_enfisem',
        'outra doença respiratória': 'st_doenca_respira_outra', 'doença respiratória (não sabe)': 'st_doenca_respira_n_sabe',
        'fumante': 'st_fumante', 'consome álcool': 'st_alcool', 'consome outra droga': 'st_outra_droga',
        'hipertensão arterial': 'st_hipertensao_arterial', 'diabetes': 'st_diabete', 'avc': 'st_avc',
        'hanseníase': 'st_hanseniase', 'tuberculose': 'st_tuberculose', 'câncer': 'st_cancer',
        'internação nos últimos 12 meses': 'st_internacao_12', 'tratamento psiquiátrico': 'st_tratamento_psiquiatra',
        'acamado': 'st_acamado', 'domiciliado': 'st_domiciliado', 'usa planta medicinal': 'st_usa_planta_medicinal',
        'doença cardíaca': 'st_doenca_cardiaca', 'insuficiência cardíaca': 'st_doenca_card_insuficiencia',
        'outra doença cardíaca': 'st_doenca_card_outro', 'doença cardíaca (não sabe)': 'st_doenca_card_n_sabe',
        'problema renal': 'st_problema_rins', 'insuficiência renal': 'st_problema_rins_insuficiencia',
        'outro problema renal': 'st_problema_rins_outro', 'problema renal (não sabe)': 'st_problema_rins_nao_sabe'
    }

    if tipo not in ['óbitos', 'mudou-se']: query_filters.append("t1.co_dim_tipo_saida_cadastro = '3'")
    if tipo in tipo_map: query_filters.append(f"t1.{tipo_map[tipo]} = '1'")
    
    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',') if u.strip()]
        if unidade_saude_list: 
            query_filters.append("no_unidade_saude = ANY(:unidades)")
            params["unidades"] = unidade_saude_list
    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',') if e.strip()]
        if equipe_list:
            query_filters.append("no_equipe = ANY(:equipes)")
            params["equipes"] = equipe_list
    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',') if p.strip()]
        if profissional_list:
            query_filters.append("no_profissional = ANY(:profissionais)")
            params["profissionais"] = profissional_list
    
    specific_tipo_filters = {
        "moradores de rua": "t1.st_morador_rua = '1'", "óbitos": "t1.co_dim_tipo_saida_cadastro = '1'",
        "mudou-se": "t1.co_dim_tipo_saida_cadastro = '2'", "cadastros ativos": "t1.co_dim_tipo_saida_cadastro = '3'",
        "fora de área": "t1.nu_micro_area = 'FA'",
        "cadastros desatualizados": "t1.dt_atualizado < (CURRENT_DATE - INTERVAL '1 year')",
        "cadastros com cns": "(LENGTH(TRIM(t1.nu_cns)) = 15 AND t1.nu_cns != '0') AND t1.co_dim_tipo_saida_cadastro = '3'",
        "cadastros com cpf": "((LENGTH(TRIM(t1.nu_cns)) != 15 OR t1.nu_cns = '0') AND t1.co_dim_tipo_saida_cadastro = '3')"
    }
    if tipo in specific_tipo_filters: query_filters.append(specific_tipo_filters[tipo])

    base_query = "SELECT no_cidadao, nu_cpf_cidadao, nu_cns, to_char(dt_nascimento, 'dd/mm/yyyy') AS dt_nascimento, no_unidade_saude, no_profissional, no_equipe, co_cidadao, to_char(dt_atualizado, 'dd/mm/yyyy') AS dt_atualizado FROM tb_cadastro t1 WHERE st_ativo = 1 "
    if query_filters: base_query += " AND " + " AND ".join(query_filters)

    with engine.connect() as connection:
        result = connection.execute(text(base_query), params)
        data = [{'nome': r['no_cidadao'], 'cpf': r['nu_cpf_cidadao'], 'cns': r['nu_cns'], 'data_nascimento': r['dt_nascimento'], 'unidade_saude': r['no_unidade_saude'], 'profissional': r['no_profissional'], 'equipe': r['no_equipe'], 'co_cidadao': r['co_cidadao'], 'dt_atualizado': r['dt_atualizado']} for r in result.mappings()]
    return jsonify(data)

@stats_bp.route('/cadastros-domiciliares', methods=['GET'])
def fetch_cadastros_domiciliares():
    unidades = request.args.get('unidade_saude', default='', type=str).strip()
    equipes = request.args.get('equipe', default='', type=str).strip()
    profissionais = request.args.get('profissional', default='', type=str).strip()
    engine = get_local_engine()
    query_filters, params = [], {}

    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',') if u.strip()]
        if unidade_saude_list: query_filters.append("no_unidade_saude = ANY(:unidades)"); params["unidades"] = unidade_saude_list
    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',') if e.strip()]
        if equipe_list: query_filters.append("no_equipe = ANY(:equipes)"); params["equipes"] = equipe_list
    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',') if p.strip()]
        if profissional_list: query_filters.append("no_profissional = ANY(:profissionais)"); params["profissionais"] = profissional_list
    
    domicilio_query = "SELECT no_logradouro AS rua, nu_domicilio AS numero, ds_complemento AS complemento, no_bairro AS bairro, nu_cep AS cep, no_unidade_saude, no_profissional, no_equipe FROM tb_domicilio q2 WHERE q2.st_ativo = 1"
    if query_filters: domicilio_query += " AND " + " AND ".join(query_filters)

    with engine.connect() as connection:
        result = connection.execute(text(domicilio_query), params)
        data = [{'rua': r['rua'], 'numero': r['numero'], 'complemento': r['complemento'], 'bairro': r['bairro'], 'cep': r['cep'], 'profissional': r['no_profissional'], 'unidade_saude': r['no_unidade_saude'], 'equipe': r['no_equipe']} for r in result.mappings()]
    return jsonify(data)

@stats_bp.route('/detalhes-hover', methods=['GET'])
def fetch_detalhes_hover():
    co_cidadao = request.args.get('co_cidadao', default='', type=str).strip()
    # unidade, equipe, profissional filters were in app.py but seem unused for this specific query by co_cidadao.
    # Kept them here in case they were intended for future use or context.
    unidades = request.args.get('unidade_saude', default='', type=str).strip()
    equipes = request.args.get('equipe', default='', type=str).strip()
    profissionais = request.args.get('profissional', default='', type=str).strip()
    engine = get_local_engine()

    query = """
    SELECT jsonb_strip_nulls(jsonb_build_object(
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
        )) AS details
    FROM tb_cadastro WHERE co_cidadao = :co_cidadao """
    
    params = {"co_cidadao": co_cidadao}
    query_filters = [] # These filters were not part of original query logic with co_cidadao, but kept for structure
    if unidades: query_filters.append("no_unidade_saude = ANY(:unidades)"); params["unidades"] = [u.strip() for u in unidades.split(',') if u.strip()]
    if equipes: query_filters.append("no_equipe = ANY(:equipes)"); params["equipes"] = [e.strip() for e in equipes.split(',') if e.strip()]
    if profissionais: query_filters.append("no_profissional = ANY(:profissionais)"); params["profissionais"] = [p.strip() for p in profissionais.split(',') if p.strip()]
    if query_filters: query += " AND " + " AND ".join(query_filters) # This line would only apply if co_cidadao was not unique or needed further filtering

    with engine.connect() as connection:
        result = connection.execute(text(query), params)
        data_row = result.fetchone()
    return jsonify(details=data_row._mapping['details'] if data_row else {})


@stats_bp.route('/contagem-detalhes', methods=['GET'])
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
    FROM tb_cadastro WHERE st_ativo = 1 AND co_dim_tipo_saida_cadastro = '3' """
    
    query_filters, params = [], {}
    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',') if u.strip()]
        if unidade_saude_list: query_filters.append("no_unidade_saude = ANY(:unidades)"); params["unidades"] = unidade_saude_list
    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',') if e.strip()]
        if equipe_list: query_filters.append("no_equipe = ANY(:equipes)"); params["equipes"] = equipe_list
    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',') if p.strip()]
        if profissional_list: query_filters.append("no_profissional = ANY(:profissionais)"); params["profissionais"] = profissional_list
    
    if query_filters: base_query += " AND " + " AND ".join(query_filters)

    with engine.connect() as connection:
        result = connection.execute(text(base_query), params)
        data_row = result.fetchone() # Assuming this query returns a single row of counts

    if data_row:
        # Map by index as column names are dynamic in the query (AS clauses)
        count_data = {
            'Responsável Familiar': data_row[0], 'Frequenta Creche': data_row[1], 'Frequenta Cuidador': data_row[2],
            'Participa de Grupo Comunitário': data_row[3], 'Plano de Saúde Privado': data_row[4], 'Deficiência': data_row[5],
            'Deficiência Auditiva': data_row[6], 'Deficiência Intelectual/Cognitiva': data_row[7], 'Deficiência Visual': data_row[8],
            'Deficiência Física': data_row[9], 'Outra Deficiência': data_row[10], 'Gestante': data_row[11],
            'Doença Respiratória': data_row[12], 'Doença Respiratória (Asma)': data_row[13],
            'Doença Respiratória (DPOC/Enfisema)': data_row[14], 'Outra Doença Respiratória': data_row[15],
            'Doença Respiratória (Não Sabe)': data_row[16], 'Fumante': data_row[17], 'Consome Álcool': data_row[18],
            'Consome Outra Droga': data_row[19], 'Hipertensão Arterial': data_row[20], 'Diabetes': data_row[21],
            'AVC': data_row[22], 'Hanseníase': data_row[23], 'Tuberculose': data_row[24], 'Câncer': data_row[25],
            'Internação nos Últimos 12 Meses': data_row[26], 'Tratamento Psiquiátrico': data_row[27],
            'Acamado': data_row[28], 'Domiciliado': data_row[29], 'Usa Planta Medicinal': data_row[30],
            'Doença Cardíaca': data_row[31], 'Insuficiência Cardíaca': data_row[32], 'Outra Doença Cardíaca': data_row[33],
            'Doença Cardíaca (Não Sabe)': data_row[34], 'Problema Renal': data_row[35], 'Insuficiência Renal': data_row[36],
            'Outro Problema Renal': data_row[37],'Problema Renal (Não Sabe)': data_row[38]
        }
    else:
        count_data = {} # Should not happen if query is correct, but good for safety

    return jsonify(count_data)
