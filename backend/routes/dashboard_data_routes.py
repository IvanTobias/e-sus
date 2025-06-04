import logging
from flask import Blueprint, request, jsonify
from sqlalchemy import text
from datetime import datetime # For visitas-domiciliares

from Conexoes import get_local_engine # Changed from Conexões
from init import cache

logger = logging.getLogger(__name__)
dashboard_data_bp = Blueprint('dashboard_data_bp', __name__, url_prefix='/api')

def _build_common_filters(request_args):
    """
    Builds common SQL filter clauses and parameters for unidade_saude, equipe, and profissional.
    Uses 'IN' operator for tuple-based parameter binding.
    """
    query_filters = []
    params = {}

    unidade_saude_str = request_args.get('unidade_saude', default='', type=str)
    if unidade_saude_str:
        unidade_saude_list = tuple(u.strip() for u in unidade_saude_str.split(',') if u.strip())
        if unidade_saude_list:
            query_filters.append("no_unidade_saude IN :unidades_saude_param")
            params["unidades_saude_param"] = unidade_saude_list

    equipe_str = request_args.get('equipe', default='', type=str)
    if equipe_str:
        equipe_list = tuple(e.strip() for e in equipe_str.split(',') if e.strip())
        if equipe_list:
            query_filters.append("no_equipe IN :equipes_param")
            params["equipes_param"] = equipe_list

    profissional_str = request_args.get('profissional', default='', type=str)
    if profissional_str:
        profissional_list = tuple(p.strip() for p in profissional_str.split(',') if p.strip())
        if profissional_list:
            query_filters.append("no_profissional IN :profissionais_param")
            params["profissionais_param"] = profissional_list

    return query_filters, params

def _build_tipo_filters(tipo_str: str) -> list[str]:
    """
    Builds SQL filter clauses based on the 'tipo' parameter.
    """
    tipo_filters = []
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

    if tipo_str not in ['óbitos', 'mudou-se']:
        tipo_filters.append("t1.co_dim_tipo_saida_cadastro = '3'")

    if tipo_str in tipo_map:
        tipo_filters.append(f"t1.{tipo_map[tipo_str]} = '1'")

    # Specific 'tipo' conditions that are not in tipo_map
    if tipo_str == "moradores de rua": tipo_filters.append("t1.st_morador_rua = '1'")
    elif tipo_str == "óbitos": tipo_filters.append("t1.co_dim_tipo_saida_cadastro = '1'")
    elif tipo_str == "mudou-se": tipo_filters.append("t1.co_dim_tipo_saida_cadastro = '2'")
    # "cadastros ativos" is implicitly handled by "t1.co_dim_tipo_saida_cadastro = '3'" or is the default state
    elif tipo_str == "fora de área": tipo_filters.append("t1.nu_micro_area = 'FA'")
    elif tipo_str == "cadastros desatualizados": tipo_filters.append("t1.dt_atualizado < (CURRENT_DATE - INTERVAL '1 year')")
    elif tipo_str == "cadastros com cns":
        # Ensure this doesn't conflict with the default "t1.co_dim_tipo_saida_cadastro = '3'"
        if "t1.co_dim_tipo_saida_cadastro = '3'" not in tipo_filters:
             tipo_filters.append("t1.co_dim_tipo_saida_cadastro = '3'")
        tipo_filters.append("(LENGTH(TRIM(t1.nu_cns)) = 15 AND t1.nu_cns != '0')")
    elif tipo_str == "cadastros com cpf":
        if "t1.co_dim_tipo_saida_cadastro = '3'" not in tipo_filters:
             tipo_filters.append("t1.co_dim_tipo_saida_cadastro = '3'")
        tipo_filters.append("(LENGTH(TRIM(t1.nu_cns)) != 15 OR t1.nu_cns = '0')")

    return tipo_filters

@dashboard_data_bp.route('/contagens', methods=['GET'])
def fetch_contagens():
    engine = get_local_engine()
    query_filters, params = _build_common_filters(request.args)

    base_query_geral_parts = [
        """SELECT
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
        AND t1.st_ficha_inativa = 0"""
    ]
    domicilio_query_parts = [
        """SELECT COUNT(q2.co_seq_cds_cad_domiciliar) as "Cadastros Domiciliares"
        FROM tb_domicilio q2
        WHERE q2.st_ativo = 1"""
    ]

    if query_filters:
        filter_string = " AND " + " AND ".join(query_filters)
        base_query_geral_parts.append(filter_string)
        domicilio_query_parts.append(filter_string)

    base_query_geral = "".join(base_query_geral_parts)
    domicilio_query = "".join(domicilio_query_parts)

    with engine.connect() as connection:
        result_geral = connection.execute(text(base_query_geral), params)
        counts_geral = result_geral.fetchone()
        result_domicilio = connection.execute(text(domicilio_query), params)
        counts_domicilio = result_domicilio.fetchone()

    counts_dict = {
        "Cadastros Individuais": counts_geral[0] if counts_geral else 0,
        "Moradores de Rua": counts_geral[1] if counts_geral else 0,
        "Óbitos": counts_geral[2] if counts_geral else 0,
        "Mudou-se": counts_geral[3] if counts_geral else 0,
        "Cadastros Ativos": counts_geral[4] if counts_geral else 0,
        "Fora de Área": counts_geral[5] if counts_geral else 0,
        "Cadastros Desatualizados": counts_geral[6] if counts_geral else 0,
        "Cadastros com Cns": counts_geral[7] if counts_geral else 0,
        "Cadastros com Cpf": counts_geral[8] if counts_geral else 0,
        "Cadastros Domiciliares": counts_domicilio[0] if counts_domicilio else 0
    }
    return jsonify(counts_dict)

@dashboard_data_bp.route('/unidades-saude', methods=['GET'])
@cache.cached(timeout=300)
def fetchUnidadesSaude():
    engine = get_local_engine()
    query_filters, params = _build_common_filters(request.args)

    query_parts = [
        """SELECT no_unidade_saude, no_equipe, no_profissional
        FROM tb_cadastro"""
    ]
    if query_filters:
        query_parts.append("WHERE " + " AND ".join(query_filters))
    query_parts.append(" GROUP BY no_unidade_saude, no_equipe, no_profissional")
    query = "".join(query_parts)

    try:
        with engine.connect() as connection:
            result = connection.execute(text(query), params)
        data = [{'unidadeSaude': row._mapping['no_unidade_saude'], 'equipe': row._mapping['no_equipe'], 'profissional': row._mapping['no_profissional']} for row in result.fetchall()]
        return jsonify(data)
    except Exception as e:
        logger.error(f"Erro em fetchUnidadesSaude: {e}")
        return jsonify({'error': str(e)}), 500

@dashboard_data_bp.route('/detalhes', methods=['GET'])
def fetch_detalhes():
    tipo = request.args.get('tipo', default='', type=str).strip().lower()
    engine = get_local_engine()

    # Initialize params for this route, to be updated by common filter helper
    params = {}

    # Get tipo-specific filters
    tipo_filter_clauses = _build_tipo_filters(tipo)

    # Get common filters and update params
    common_filter_clauses, common_params = _build_common_filters(request.args)
    params.update(common_params)

    # Combine all filter clauses
    all_query_filters = tipo_filter_clauses + common_filter_clauses

    base_query = "SELECT no_cidadao, nu_cpf_cidadao, nu_cns, to_char(dt_nascimento, 'dd/mm/yyyy') AS dt_nascimento, no_unidade_saude, no_profissional, no_equipe, co_cidadao, to_char(dt_atualizado, 'dd/mm/yyyy') AS dt_atualizado FROM tb_cadastro t1 WHERE st_ativo = 1"
    if all_query_filters:
        base_query += " AND " + " AND ".join(all_query_filters)

    with engine.connect() as connection:
        result = connection.execute(text(base_query), params)
        data = [{'nome': r._mapping['no_cidadao'], 'cpf': r._mapping['nu_cpf_cidadao'], 'cns': r._mapping['nu_cns'], 'data_nascimento': r._mapping['dt_nascimento'], 'unidade_saude': r._mapping['no_unidade_saude'], 'profissional': r._mapping['no_profissional'], 'equipe': r._mapping['no_equipe'], 'co_cidadao': r._mapping['co_cidadao'], 'dt_atualizado': r._mapping['dt_atualizado']} for r in result.fetchall()]
    return jsonify(data)

@dashboard_data_bp.route('/cadastros-domiciliares', methods=['GET'])
def fetch_cadastros_domiciliares():
    engine = get_local_engine()
    query_filters, params = _build_common_filters(request.args)

    domicilio_query_parts = ["SELECT no_logradouro AS rua, nu_domicilio AS numero, ds_complemento AS complemento, no_bairro AS bairro, nu_cep AS cep, no_unidade_saude, no_profissional, no_equipe FROM tb_domicilio q2 WHERE q2.st_ativo = 1"]
    if query_filters:
        # Ensure that if query_filters is not empty, we add AND correctly.
        # The base query already has a WHERE clause.
        domicilio_query_parts.append("AND " + " AND ".join(query_filters))

    domicilio_query = " ".join(domicilio_query_parts)

    with engine.connect() as connection:
        result = connection.execute(text(domicilio_query), params)
        data = [{'rua': r._mapping['rua'], 'numero': r._mapping['numero'], 'complemento': r._mapping['complemento'], 'bairro': r._mapping['bairro'], 'cep': r._mapping['cep'], 'profissional': r._mapping['no_profissional'], 'unidade_saude': r._mapping['no_unidade_saude'], 'equipe': r._mapping['no_equipe']} for r in result.fetchall()]
    return jsonify(data)

@dashboard_data_bp.route('/detalhes-hover', methods=['GET'])
def fetch_detalhes_hover():
    co_cidadao = request.args.get('co_cidadao', default='', type=str).strip()
    engine = get_local_engine()
    params = {"co_cidadao": co_cidadao}

    query = """
    SELECT jsonb_strip_nulls(
        jsonb_build_object(
            'Responsável Familiar', CASE WHEN st_responsavel_familiar = '1' THEN 'Sim' ELSE NULL END, 'Frequenta Creche', CASE WHEN st_frequenta_creche = '1' THEN 'Sim' ELSE NULL END,
            'Frequenta Cuidador', CASE WHEN st_frequenta_cuidador = '1' THEN 'Sim' ELSE NULL END, 'Participa de Grupo Comunitário', CASE WHEN st_participa_grupo_comunitario = '1' THEN 'Sim' ELSE NULL END,
            'Plano de Saúde Privado', CASE WHEN st_plano_saude_privado = '1' THEN 'Sim' ELSE NULL END, 'Deficiência', CASE WHEN st_deficiencia = '1' THEN 'Sim' ELSE NULL END,
            'Deficiência Auditiva', CASE WHEN st_defi_auditiva = '1' THEN 'Sim' ELSE NULL END, 'Deficiência Intelectual/Cognitiva', CASE WHEN st_defi_intelectual_cognitiva = '1' THEN 'Sim' ELSE NULL END,
            'Deficiência Visual', CASE WHEN st_defi_visual = '1' THEN 'Sim' ELSE NULL END, 'Deficiência Física', CASE WHEN st_defi_fisica = '1' THEN 'Sim' ELSE NULL END,
            'Outra Deficiência', CASE WHEN st_defi_outra = '1' THEN 'Sim' ELSE NULL END, 'Gestante', CASE WHEN st_gestante = '1' THEN 'Sim' ELSE NULL END,
            'Doença Respiratória', CASE WHEN st_doenca_respiratoria = '1' THEN 'Sim' ELSE NULL END, 'Doença Respiratória (Asma)', CASE WHEN st_doenca_respira_asma = '1' THEN 'Sim' ELSE NULL END,
            'Doença Respiratória (DPOC/Enfisema)', CASE WHEN st_doenca_respira_dpoc_enfisem = '1' THEN 'Sim' ELSE NULL END, 'Outra Doença Respiratória', CASE WHEN st_doenca_respira_outra = '1' THEN 'Sim' ELSE NULL END,
            'Doença Respiratória (Não Sabe)', CASE WHEN st_doenca_respira_n_sabe = '1' THEN 'Sim' ELSE NULL END, 'Fumante', CASE WHEN st_fumante = '1' THEN 'Sim' ELSE NULL END,
            'Consome Álcool', CASE WHEN st_alcool = '1' THEN 'Sim' ELSE NULL END, 'Consome Outra Droga', CASE WHEN st_outra_droga = '1' THEN 'Sim' ELSE NULL END,
            'Hipertensão Arterial', CASE WHEN st_hipertensao_arterial = '1' THEN 'Sim' ELSE NULL END, 'Diabetes', CASE WHEN st_diabete = '1' THEN 'Sim' ELSE NULL END,
            'AVC', CASE WHEN st_avc = '1' THEN 'Sim' ELSE NULL END, 'Hanseníase', CASE WHEN st_hanseniase = '1' THEN 'Sim' ELSE NULL END,
            'Tuberculose', CASE WHEN st_tuberculose = '1' THEN 'Sim' ELSE NULL END, 'Câncer', CASE WHEN st_cancer = '1' THEN 'Sim' ELSE NULL END,
            'Internação nos Últimos 12 Meses', CASE WHEN st_internacao_12 = '1' THEN 'Sim' ELSE NULL END, 'Tratamento Psiquiátrico', CASE WHEN st_tratamento_psiquiatra = '1' THEN 'Sim' ELSE NULL END,
            'Acamado', CASE WHEN st_acamado = '1' THEN 'Sim' ELSE NULL END, 'Domiciliado', CASE WHEN st_domiciliado = '1' THEN 'Sim' ELSE NULL END,
            'Usa Planta Medicinal', CASE WHEN st_usa_planta_medicinal = '1' THEN 'Sim' ELSE NULL END, 'Doença Cardíaca', CASE WHEN st_doenca_cardiaca = '1' THEN 'Sim' ELSE NULL END,
            'Insuficiência Cardíaca', CASE WHEN st_doenca_card_insuficiencia = '1' THEN 'Sim' ELSE NULL END, 'Outra Doença Cardíaca', CASE WHEN st_doenca_card_outro = '1' THEN 'Sim' ELSE NULL END,
            'Doença Cardíaca (Não Sabe)', CASE WHEN st_doenca_card_n_sabe = '1' THEN 'Sim' ELSE NULL END, 'Problema Renal', CASE WHEN st_problema_rins = '1' THEN 'Sim' ELSE NULL END,
            'Insuficiência Renal', CASE WHEN st_problema_rins_insuficiencia = '1' THEN 'Sim' ELSE NULL END, 'Outro Problema Renal', CASE WHEN st_problema_rins_outro = '1' THEN 'Sim' ELSE NULL END,
            'Problema Renal (Não Sabe)', CASE WHEN st_problema_rins_nao_sabe = '1' THEN 'Sim' ELSE NULL END
        )
    ) AS details
    FROM tb_cadastro
    WHERE co_cidadao = :co_cidadao"""

    with engine.connect() as connection:
        result = connection.execute(text(query), params)
        data = result.fetchone()
        details = data._mapping['details'] if data and data._mapping['details'] else {}
        return jsonify(details=details)

@dashboard_data_bp.route('/contagem-detalhes', methods=['GET'])
def fetch_detalhes_count():
    engine = get_local_engine()
    query_filters, params = _build_common_filters(request.args)

    base_query_parts = ["""
    SELECT
        COUNT(CASE WHEN st_responsavel_familiar = '1' THEN 1 END) AS responsavel_familiar, COUNT(CASE WHEN st_frequenta_creche = '1' THEN 1 END) AS frequenta_creche,
        COUNT(CASE WHEN st_frequenta_cuidador = '1' THEN 1 END) AS frequenta_cuidador, COUNT(CASE WHEN st_participa_grupo_comunitario = '1' THEN 1 END) AS participa_grupo_comunitario,
        COUNT(CASE WHEN st_plano_saude_privado = '1' THEN 1 END) AS plano_saude_privado, COUNT(CASE WHEN st_deficiencia = '1' THEN 1 END) AS deficiencia,
        COUNT(CASE WHEN st_defi_auditiva = '1' THEN 1 END) AS deficiencia_auditiva, COUNT(CASE WHEN st_defi_intelectual_cognitiva = '1' THEN 1 END) AS deficiencia_intelectual_cognitiva,
        COUNT(CASE WHEN st_defi_visual = '1' THEN 1 END) AS deficiencia_visual, COUNT(CASE WHEN st_defi_fisica = '1' THEN 1 END) AS deficiencia_fisica,
        COUNT(CASE WHEN st_defi_outra = '1' THEN 1 END) AS outra_deficiencia, COUNT(CASE WHEN st_gestante = '1' THEN 1 END) AS gestante,
        COUNT(CASE WHEN st_doenca_respiratoria = '1' THEN 1 END) AS doenca_respiratoria, COUNT(CASE WHEN st_doenca_respira_asma = '1' THEN 1 END) AS asma,
        COUNT(CASE WHEN st_doenca_respira_dpoc_enfisem = '1' THEN 1 END) AS dpoc, COUNT(CASE WHEN st_doenca_respira_outra = '1' THEN 1 END) AS outra_respiratoria,
        COUNT(CASE WHEN st_doenca_respira_n_sabe = '1' THEN 1 END) AS nao_sabe_respiratoria, COUNT(CASE WHEN st_fumante = '1' THEN 1 END) AS fumante,
        COUNT(CASE WHEN st_alcool = '1' THEN 1 END) AS alcool, COUNT(CASE WHEN st_outra_droga = '1' THEN 1 END) AS outra_droga,
        COUNT(CASE WHEN st_hipertensao_arterial = '1' THEN 1 END) AS hipertensao_arterial, COUNT(CASE WHEN st_diabete = '1' THEN 1 END) AS diabetes,
        COUNT(CASE WHEN st_avc = '1' THEN 1 END) AS AVC, COUNT(CASE WHEN st_hanseniase = '1' THEN 1 END) AS hanseniase,
        COUNT(CASE WHEN st_tuberculose = '1' THEN 1 END) AS tuberculose, COUNT(CASE WHEN st_cancer = '1' THEN 1 END) AS cancer,
        COUNT(CASE WHEN st_internacao_12 = '1' THEN 1 END) AS internacao_12, COUNT(CASE WHEN st_tratamento_psiquiatra = '1' THEN 1 END) AS tratamento_psiquiatrico,
        COUNT(CASE WHEN st_acamado = '1' THEN 1 END) AS acamado, COUNT(CASE WHEN st_domiciliado = '1' THEN 1 END) AS domiciliado,
        COUNT(CASE WHEN st_usa_planta_medicinal = '1' THEN 1 END) AS planta_medicinal, COUNT(CASE WHEN st_doenca_cardiaca = '1' THEN 1 END) AS doenca_cardiaca,
        COUNT(CASE WHEN st_doenca_card_insuficiencia = '1' THEN 1 END) AS insuficiencia_cardiaca, COUNT(CASE WHEN st_doenca_card_outro = '1' THEN 1 END) AS outra_cardiaca,
        COUNT(CASE WHEN st_doenca_card_n_sabe = '1' THEN 1 END) AS nao_sabe_cardiaca, COUNT(CASE WHEN st_problema_rins = '1' THEN 1 END) AS problema_renal,
        COUNT(CASE WHEN st_problema_rins_insuficiencia = '1' THEN 1 END) AS insuficiencia_renal, COUNT(CASE WHEN st_problema_rins_outro = '1' THEN 1 END) AS outro_renal,
        COUNT(CASE WHEN st_problema_rins_nao_sabe = '1' THEN 1 END) AS nao_sabe_renal
    FROM tb_cadastro WHERE st_ativo = 1 and co_dim_tipo_saida_cadastro = '3'"""]

    if query_filters:
        base_query_parts.append("AND " + " AND ".join(query_filters))

    base_query = " ".join(base_query_parts)

    with engine.connect() as connection:
        result = connection.execute(text(base_query), params)
        data = result.fetchone()
        count_data = {
            'Responsável Familiar': data[0] if data else 0, 'Frequenta Creche': data[1] if data else 0, 'Frequenta Cuidador': data[2] if data else 0,
            'Participa de Grupo Comunitário': data[3] if data else 0, 'Plano de Saúde Privado': data[4] if data else 0, 'Deficiência': data[5] if data else 0,
            'Deficiência Auditiva': data[6] if data else 0, 'Deficiência Intelectual/Cognitiva': data[7] if data else 0, 'Deficiência Visual': data[8] if data else 0,
            'Deficiência Física': data[9] if data else 0, 'Outra Deficiência': data[10] if data else 0, 'Gestante': data[11] if data else 0,
            'Doença Respiratória': data[12] if data else 0, 'Doença Respiratória (Asma)': data[13] if data else 0, 'Doença Respiratória (DPOC/Enfisema)': data[14] if data else 0,
            'Outra Doença Respiratória': data[15] if data else 0, 'Doença Respiratória (Não Sabe)': data[16] if data else 0, 'Fumante': data[17] if data else 0,
            'Consome Álcool': data[18] if data else 0, 'Consome Outra Droga': data[19] if data else 0, 'Hipertensão Arterial': data[20] if data else 0,
            'Diabetes': data[21] if data else 0, 'AVC': data[22] if data else 0, 'Hanseníase': data[23] if data else 0, 'Tuberculose': data[24] if data else 0,
            'Câncer': data[25] if data else 0, 'Internação nos Últimos 12 Meses': data[26] if data else 0, 'Tratamento Psiquiátrico': data[27] if data else 0,
            'Acamado': data[28] if data else 0, 'Domiciliado': data[29] if data else 0, 'Usa Planta Medicinal': data[30] if data else 0,
            'Doença Cardíaca': data[31] if data else 0, 'Insuficiência Cardíaca': data[32] if data else 0, 'Outra Doença Cardíaca': data[33] if data else 0,
            'Doença Cardíaca (Não Sabe)': data[34] if data else 0, 'Problema Renal': data[35] if data else 0, 'Insuficiência Renal': data[36] if data else 0,
            'Outro Problema Renal': data[37] if data else 0, 'Problema Renal (Não Sabe)': data[38] if data else 0,
        }
    return jsonify(count_data)

@dashboard_data_bp.route('/visitas-domiciliares', methods=['GET'])
def fetch_visitas_domiciliares():
    start_date_str = request.args.get('start_date', default='', type=str)
    end_date_str = request.args.get('end_date', default='', type=str)
    tipo_consulta = request.args.get('tipo_consulta', default='filtros', type=str)

    engine = get_local_engine()
    query_filters, params = _build_common_filters(request.args)

    if start_date_str and end_date_str:
        try:
            start = datetime.strptime(start_date_str, "%Y-%m-%d")
            end = datetime.strptime(end_date_str, "%Y-%m-%d")
            query_filters.append("dt_visita_mcaf BETWEEN :start_date AND :end_date")
            params["start_date"] = start
            params["end_date"] = end
        except ValueError:
            return jsonify({"error": "Data inválida. Use o formato YYYY-MM-DD."}), 400

    if tipo_consulta == 'filtros':
        base_query_parts = ["SELECT initcap(no_unidade_saude) AS no_unidade_saude, initcap(no_profissional) AS no_profissional, initcap(no_equipe) AS no_equipe FROM tb_visitas"]
        if query_filters:
            base_query_parts.append("WHERE " + " AND ".join(query_filters))
        base_query_parts.append("GROUP BY no_unidade_saude, no_profissional, no_equipe")
        query = " ".join(base_query_parts)
        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            data = [{'no_unidade_saude': r._mapping['no_unidade_saude'], 'no_profissional': r._mapping['no_profissional'], 'no_equipe': r._mapping['no_equipe']} for r in result.fetchall()]
        return jsonify(data)
    elif tipo_consulta == 'mapa':
        base_query_parts = ["SELECT nu_latitude, nu_longitude, initcap(no_unidade_saude) AS no_unidade_saude, initcap(no_profissional) AS no_profissional, initcap(no_equipe) AS no_equipe, co_dim_desfecho_visita, to_char(dt_visita_mcaf, 'DD/MM/YYYY') as dt_visita, sg_sexo, case when com_localizacao = '0' then 'Não' else 'Sim' end as com_localizacao FROM tb_visitas"]
        if query_filters:
            base_query_parts.append("WHERE " + " AND ".join(query_filters))
        query = " ".join(base_query_parts)
        with engine.connect() as connection:
            result = connection.execute(text(query), params)
            data = [{'nu_latitude': r._mapping['nu_latitude'], 'nu_longitude': r._mapping['nu_longitude'], 'no_unidade_saude': r._mapping['no_unidade_saude'], 'no_profissional': r._mapping['no_profissional'], 'no_equipe': r._mapping['no_equipe'], 'co_dim_desfecho_visita': r._mapping['co_dim_desfecho_visita'], 'dt_visita': r._mapping['dt_visita'], 'sg_sexo': r._mapping['sg_sexo'], 'com_localizacao': r._mapping['com_localizacao']} for r in result.fetchall()]
        return jsonify(data)
    else:
        return jsonify({'error': 'tipo_consulta inválido. Use "filtros" ou "mapa".'}), 400

@dashboard_data_bp.route('/data', methods=['POST'])
def get_data():
    try:
        data = request.get_json()
        tipo = data.get('tipo')
        ano = data.get('ano')
        mes = data.get('mes')

        if not tipo or not ano or not mes:
            return jsonify({'error': 'Tipo, ano e mês são obrigatórios!'}), 400

        params = {'ano': ano, 'mes': mes}
        queries = {
            'iaf': "SELECT profissional, cbo, nome_da_unidade, total_de_participantes, total_participantes_registrados, total_de_atividades FROM tb_iaf WHERE nu_ano = :ano AND nu_mes = :mes",
            'pse': "SELECT inep, nome_da_escola, total_de_atividades, indicador_1, indicador_2 FROM tb_pse WHERE ano = :ano AND mes = :mes",
            'pse_prof': "SELECT profissional, nome_cbo, nome_da_unidade, inep, nome_da_escola, total_de_participantes, total_de_participantes_registrados, total_de_atividades FROM tb_pse_prof WHERE ano = :ano AND mes = :mes"
        }
        query_template = queries.get(tipo)
        if not query_template:
            return jsonify({'error': 'Tipo inválido!'}), 400

        final_query = query_template

        with get_local_engine().connect() as conn:
            result = conn.execute(text(final_query), params)
            rows = result.fetchall()
            columns = result.keys()
            data_list = [{col: str(value) if not isinstance(value, (str, int, float, bool, type(None))) else value for col, value in zip(columns, row)} for row in rows]
        return jsonify({'columns': list(columns), 'data': data_list})
    except Exception as e:
        logger.error(f"Erro ao buscar os dados para o tipo {tipo}: {str(e)}")
        return jsonify({'error': f'Erro ao buscar os dados para o tipo {tipo}!'}), 500
