# backend/app.py
from init import app, CORS  # Importa o app e o CORS do módulo init.py
from socketio_config import socketio  # Importa o socketio da configuração
import Gerar_BPA
import pandas as pd
import threading
import os
import importdados
from Conexões import get_local_engine
from Consultas import send_progress_update, execute_long_task, get_progress, execute_and_store_queries
import json
from flask import make_response, request, jsonify, send_file, send_from_directory
from sqlalchemy import create_engine, text
import logging
from flask_caching import Cache
from apscheduler.schedulers.background import BackgroundScheduler

# Configuração básica de logging
logging.basicConfig(level=logging.DEBUG)
CORS(app)  # Ativa o CORS

# Definições de variáveis globais
cancel_requests = {}

#Configuração do cache
cache = Cache(config={'CACHE_TYPE': 'SimpleCache'})
cache.init_app(app)
# =================== Endpoints da API ===================

@app.route('/api/save-config', methods=['POST'])
def save_config():
    config_data = request.json
    try:
        # Modificação: adicionando codificação UTF-8
        engine = create_engine(
            f"postgresql://{config_data['username']}:{config_data['password']}@{config_data['ip']}:{config_data['port']}/{config_data['database']}",
            connect_args={'options': '-c client_encoding=utf8'})
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))  # Testa a conexão
        with open('config.json', 'w') as config_file:
            json.dump(config_data, config_file)  # Salva configuração em arquivo
        return jsonify({"status": "Configuração salva com sucesso!"})
    except Exception as e:
        logging.error(f"Erro ao testar a conexão: {e}")
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
        # Modificação: adicionando codificação UTF-8
        engine = create_engine(
            f"postgresql://{config_data['username']}:{config_data['password']}@{config_data['ip']}:{config_data['port']}/{config_data['database']}",
            connect_args={'options': '-c client_encoding=utf8'})
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))
        return jsonify({"message": "Conexão bem-sucedida!"})
    except Exception as e:
        logging.error(f"Erro na conexão: {e}")
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
    progress = 0
    send_progress_update(tipo, progress)

    with open('config.json', 'r') as config_file:
        config_data = json.load(config_file)

    if request.method == 'POST':
        incoming_data = request.json
        #logging.debug(f"Dados recebidos na requisição POST: {incoming_data}")
        if 'ano' in incoming_data:
            config_data['ano'] = incoming_data['ano']
        if 'mes' in incoming_data:
            config_data['mes'] = incoming_data['mes']

    if tipo in ['cadastro', 'domiciliofcd', 'bpa', 'visitas']:
        #logging.debug(f"Iniciando a thread para o tipo {tipo}")
        thread = threading.Thread(target=execute_long_task, args=(config_data, tipo))
        thread.start()
        return jsonify({"status": "success", "message": f"Consultas {tipo} em execução."})
    else:
        #logging.error(f"Tipo desconhecido recebido: {tipo}")
        return jsonify({"status": "error", "message": "Tipo desconhecido."})

@app.route('/progress/<tipo>', methods=['GET'])
def get_progress_endpoint(tipo):
    progress = get_progress(tipo)
    response = make_response(jsonify({"progress": progress}))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    return response

@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store'
    return response

@app.route('/export-xls', methods=['GET'])
def export_xls():
    engine = get_local_engine()
    query = "SELECT * FROM query_1"
    df = pd.read_sql(query, engine)
    filename = "cadastros_exportados.xlsx"
    filepath = os.path.join(os.getcwd(), filename)
    df.to_excel(filepath, index=False)
    
    response = make_response(send_file(filepath, as_attachment=True))
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return response

@app.route('/export-xls2', methods=['GET'])
def export_xls2():
    engine = get_local_engine()
    query = "SELECT * FROM query_2"
    df = pd.read_sql(query, engine)
    filename = "domiciliofcd_exportadas.xlsx"
    filepath = os.path.join(os.getcwd(), filename)
    df.to_excel(filepath, index=False)
    
    response = make_response(send_file(filepath, as_attachment=True))
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return response

@app.route('/export-visitas', methods=['GET'])
def export_visitas():
    engine = get_local_engine()
    query = "SELECT * FROM query_4"
    df = pd.read_sql(query, engine)
    filename = "visitas_exportadas.xlsx"
    filepath = os.path.join(os.getcwd(), filename)
    df.to_excel(filepath, index=False)
    
    response = make_response(send_file(filepath, as_attachment=True))
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return response

@app.route('/export-bpa', methods=['GET'])
def export_xls3():
    engine = get_local_engine()
    query = "SELECT * FROM query_3"
    df = pd.read_sql(query, engine)
    filename = "bpa.xlsx"
    filepath = os.path.join(os.getcwd(), filename)
    df.to_excel(filepath, index=False)
    
    response = make_response(send_file(filepath, as_attachment=True))
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return response

@app.route('/api/gerar-bpa', methods=['POST'])
def gerar_bpa_route():
    try:
        filepath = Gerar_BPA.criar_arquivo_bpa()
        
        if filepath:
            # Envia o arquivo diretamente ao frontend para download
            return send_file(filepath, as_attachment=True)
        else:
            return jsonify({"message": "Erro ao gerar BPA: Arquivo não foi criado."}), 500
    except Exception as e:
        print(f"Erro ao gerar BPA: {str(e)}")
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
        # Lista todos os arquivos no diretório atual
        for filename in os.listdir('.'):
            # Verifica se o arquivo começa com 'bpa_' e termina com '.txt'
            if filename.startswith('bpa_') and filename.endswith('.txt'):
                files.append(filename)
        return jsonify({"files": files})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para deletar um arquivo BPA específico
@app.route('/api/delete-bpa-file', methods=['DELETE'])
def delete_bpa_file():
    filename = request.args.get('filename')
    try:
        # Caminho completo para o arquivo
        file_path = os.path.join(os.getcwd(), filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            return jsonify({"message": f"Arquivo {filename} deletado com sucesso!"})
        else:
            return jsonify({"message": "Arquivo não encontrado."}), 404
    except Exception as e:
        return jsonify({"message": f"Erro ao deletar arquivo: {str(e)}"}), 500

# Endpoint para baixar um arquivo BPA específico
@app.route('/api/download-bpa-file', methods=['GET'])
def download_bpa_file():
    filename = request.args.get('filename')
    try:
        # Envia o arquivo para download
        return send_from_directory(os.getcwd(), filename, as_attachment=True)
    except Exception as e:
        return jsonify({"message": f"Erro ao baixar o arquivo: {str(e)}"}), 500

@app.route('/api/contagens', methods=['GET'])
def fetch_contagens():
    # Obtém os parâmetros da requisição
    unidade_saude = request.args.get('unidade_saude', default='', type=str)
    equipe = request.args.get('equipe', default='', type=str)
    profissional = request.args.get('profissional', default='', type=str)

    engine = get_local_engine()

    # Filtros e parâmetros para ambas as consultas
    query_filters_geral = []
    query_filters_domicilio = []
    params_geral = {}
    params_domicilio = {}

    # Filtros para cadastros gerais (query_1)
    if unidade_saude:
        unidade_saude_list = unidade_saude.split(',')
        query_filters_geral.append("initcap_2 IN :unidade_saude")  # query_1 usa initcap_2
        params_geral["unidade_saude"] = tuple(unidade_saude_list)

    if equipe:
        equipe_list = equipe.split(',')
        query_filters_geral.append("initcap_4 IN :equipe")  # query_1 usa initcap_4
        params_geral["equipe"] = tuple(equipe_list)

    if profissional:
        profissional_list = profissional.split(',')
        query_filters_geral.append("initcap_3 IN :profissional")  # query_1 usa initcap_3
        params_geral["profissional"] = tuple(profissional_list)

    # Filtros para cadastros domiciliares (query_2)
    if unidade_saude:
        query_filters_domicilio.append("initcap_1 IN :unidade_saude")  # query_2 usa initcap_1
        params_domicilio["unidade_saude"] = tuple(unidade_saude_list)

    if equipe:
        query_filters_domicilio.append("initcap_3 IN :equipe")  # query_2 usa initcap_3
        params_domicilio["equipe"] = tuple(equipe_list)

    if profissional:
        query_filters_domicilio.append("initcap_2 IN :profissional")  # query_2 usa initcap_2
        params_domicilio["profissional"] = tuple(profissional_list)

    # Consulta de cadastros gerais (query_1)
    base_query_geral = """
    SELECT 
        COUNT(t1.co_seq_fat_cad_individual) as "Cadastros Individuais",
        COUNT(CASE WHEN t1.st_morador_rua = '1' THEN 1 END) as "Moradores de Rua",
        COUNT(CASE WHEN t1.co_dim_tipo_saida_cadastro = '1' THEN 1 END) as "Óbitos",
        COUNT(CASE WHEN t1.co_dim_tipo_saida_cadastro = '2' THEN 1 END) as "Mudou-se",
        COUNT(CASE WHEN t1.co_dim_tipo_saida_cadastro = '3' THEN 1 END) as "Cadastros Ativos",
        COUNT(CASE WHEN t1.nu_micro_area_1 = 'FA' THEN 1 END) as "Fora de Área",
        COUNT(CASE WHEN dt_atualizado < (CURRENT_DATE - INTERVAL '1 year') THEN 1 END) as "Cadastros Desatualizados",
        COUNT(CASE WHEN LENGTH(TRIM(nu_cns_3)) = 15 AND nu_cns_3 != '0' THEN 1 END) as "Cadastros com Cns",
        COUNT(CASE WHEN LENGTH(TRIM(nu_cns_3)) != 15 OR nu_cns_3 = '0' THEN 1 END) as "Cadastros com Cpf"
    FROM query_1 t1
    WHERE t1.st_ativo = 1 
    AND t1.co_dim_tipo_saida_cadastro IS NOT NULL 
    AND t1.st_ficha_inativa_1 = 0
    """

    if query_filters_geral:
        base_query_geral += " AND " + " AND ".join(query_filters_geral)

    # Consulta de cadastros domiciliares (query_2)
    domicilio_query = """
    SELECT COUNT(q2.co_seq_cds_cad_domiciliar) as "Cadastros Domiciliares"
    FROM query_2 q2
    WHERE q2.st_ativo = 1
    """

    if query_filters_domicilio:
        domicilio_query += " AND " + " AND ".join(query_filters_domicilio)

    # Executando as consultas
    with engine.connect() as connection:
        # Executa a consulta para cadastros gerais
        result_geral = connection.execute(text(base_query_geral), params_geral)
        counts_geral = result_geral.fetchone()

        # Executa a consulta para cadastros domiciliares
        result_domicilio = connection.execute(text(domicilio_query), params_domicilio)
        counts_domicilio = result_domicilio.fetchone()

    # Converte os resultados para um dicionário
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
        "Cadastros Domiciliares": counts_domicilio[0]  # Adiciona o resultado da segunda consulta
    }

    # Retorna os resultados em formato JSON
    return jsonify(counts_dict)

@app.route('/api/unidades-saude', methods=['GET'])
@cache.cached(timeout=300)  # Cache por 5 minutos (300 segundos)
def fetchUnidadesSaude():
    # Obtendo os parâmetros da requisição
    unidade_saude = request.args.get('unidade_saude', default='', type=str)
    equipe = request.args.get('equipe', default='', type=str)
    profissional = request.args.get('profissional', default='', type=str)
    engine = get_local_engine()

    # SQL base para a consulta
    query = """
    SELECT 
    initcap_2, 
    initcap_3, 
    initcap_4
    FROM query_1
    """

    # Adiciona a condição de filtro pela unidade de saúde, se fornecida
    conditions = []
    params = {}

    if unidade_saude:
        conditions.append("initcap_2 = :unidade_saude")
        params["unidade_saude"] = unidade_saude

    if equipe:
        conditions.append("initcap_4 = :equipe")
        params["equipe"] = equipe

    if profissional:
        conditions.append("initcap_3 = :profissional")
        params["profissional"] = profissional

    # Concatena todas as condições SQL na query base
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    # Adiciona o agrupamento após as condições
    query += " GROUP BY initcap_2, initcap_3, initcap_4"

    try:
        with engine.connect() as connection:
            result = connection.execute(text(query), params)
        
        # Converte o resultado para um dicionário agrupando por unidade de saúde, equipe e profissional
        data = [
            {
                'unidadeSaude': row._mapping['initcap_2'],
                'equipe': row._mapping['initcap_4'],
                'profissional': row._mapping['initcap_3']
            }
            for row in result.fetchall()
        ]

        # Retorna os dados no formato JSON
        return jsonify(data)

    except Exception as e:
        # Em caso de erro, retorna a mensagem de erro com status 500
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
        'responsável familiar': 'st_responsavel_familiar_1',
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

    # Verifica se o tipo está no mapeamento
    if tipo in tipo_map:
        query_filters.append(f"t1.{tipo_map[tipo]} = '1'")

    # Filtros adicionais para unidade de saúde, equipe e profissional
    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',')]
        query_filters.append("initcap_2 = ANY(:unidades)")
        params["unidades"] = unidade_saude_list

    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',')]
        query_filters.append("initcap_4 = ANY(:equipes)")
        params["equipes"] = equipe_list

    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',')]
        query_filters.append("initcap_3 = ANY(:profissionais)")
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
            query_filters.append("t1.nu_micro_area_1 = 'FA'")
        elif tipo == "cadastros desatualizados":
            query_filters.append("t1.dt_atualizado < (CURRENT_DATE - INTERVAL '1 year')")
        elif tipo == "cadastros com cns":
            query_filters.append("LENGTH(TRIM(t1.nu_cns_3)) = 15 AND t1.nu_cns_3 != '0'")
        elif tipo == "cadastros com cpf":
            query_filters.append("LENGTH(TRIM(t1.nu_cns_3)) != 15 OR t1.nu_cns_3 = '0'")

    # SQL base para cadastros gerais
    base_query = """
    SELECT 
        initcap_1 AS no_cidadao_1,
        t1.nu_cpf_cidadao_1,
        t1.nu_cns_1,
        to_char(t1.dt_nascimento_1, 'dd/mm/yyyy') AS dt_nascimento_1,
        initcap_2 AS no_unidade_saude_1,
        initcap_3 AS no_profissional_1,
        initcap_4 AS no_equipe_1,
        t1.co_cidadao,
        to_char(t1.dt_atualizado, 'dd/mm/yyyy') AS dt_atualizado
    FROM query_1 t1
    WHERE t1.st_ativo = 1
    """

    # Adiciona os filtros à consulta
    if query_filters:
        base_query += " AND " + " AND ".join(query_filters)

    # Executa a consulta e retorna os resultados
    with engine.connect() as connection:
        result = connection.execute(text(base_query), params)
        data = []
        for row in result.fetchall():
            item = {
                'nome': row._mapping['no_cidadao_1'],
                'cpf': row._mapping['nu_cpf_cidadao_1'],
                'cns': row._mapping['nu_cns_1'],
                'data_nascimento': row._mapping['dt_nascimento_1'],
                'unidade_saude': row._mapping['no_unidade_saude_1'],
                'profissional': row._mapping['no_profissional_1'],
                'equipe': row._mapping['no_equipe_1'],
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

    # Filtros para cadastros domiciliares
    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',')]
        query_filters.append("initcap_1 = ANY(:unidades)")
        params["unidades"] = unidade_saude_list
    
    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',')]
        query_filters.append("initcap_3 = ANY(:equipes)")
        params["equipes"] = equipe_list

    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',')]
        query_filters.append("initcap_2 = ANY(:profissionais)")
        params["profissionais"] = profissional_list

    # SQL base para cadastros domiciliares
    domicilio_query = """
    SELECT 
        no_logradouro_2 AS rua,
        nu_domicilio AS numero,
        ds_complemento_1 AS complemento,
        no_bairro_2 AS bairro,
        nu_cep_2 AS cep,
        initcap_1 AS unidade_saude,
        initcap_2 AS profissional,
        initcap_3 AS equipe
    FROM query_2 q2
    WHERE q2.st_ativo = 1
    """

    # Adiciona os filtros à consulta de cadastros domiciliares
    if query_filters:
        domicilio_query += " AND " + " AND ".join(query_filters)

    # Executa a consulta e retorna os resultados
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
                'profissional': row._mapping['profissional'],
                'unidade_saude': row._mapping['unidade_saude'],
                'equipe': row._mapping['equipe']
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

    # Consulta SQL modificada para JSON
    query = """
    SELECT jsonb_strip_nulls(
        jsonb_build_object(
            'Responsável Familiar', CASE WHEN st_responsavel_familiar_1 = '1' THEN 'Sim' ELSE NULL END,
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
    FROM query_1
    WHERE co_cidadao = :co_cidadao
    """

    # Condições de filtro adicionais
    query_filters = []
    params = {"co_cidadao": co_cidadao}

    # Filtro por unidade de saúde
    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',')]
        query_filters.append("initcap_2 = ANY(:unidades)")
        params["unidades"] = unidade_saude_list

    # Filtro por equipe
    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',')]
        query_filters.append("initcap_4 = ANY(:equipes)")
        params["equipes"] = equipe_list

    # Filtro por profissional
    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',')]
        query_filters.append("initcap_3 = ANY(:profissionais)")
        params["profissionais"] = profissional_list

    # Adiciona os filtros à consulta base
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
    # Obtém os parâmetros da requisição
    unidades = request.args.get('unidade_saude', default='', type=str).strip()
    equipes = request.args.get('equipe', default='', type=str).strip()
    profissionais = request.args.get('profissional', default='', type=str).strip()

    engine = get_local_engine()

    # SQL base para a consulta agregada
    base_query = """
    SELECT
        COUNT(CASE WHEN st_responsavel_familiar_1 = '1' THEN 1 END) AS responsavel_familiar,
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
    FROM query_1
    WHERE st_ativo = 1
    """

    # Condições de filtro adicionais
    query_filters = []
    params = {}

    # Filtro por unidade de saúde
    if unidades:
        unidade_saude_list = [u.strip() for u in unidades.split(',')]
        query_filters.append("initcap_2 = ANY(:unidades)")
        params["unidades"] = unidade_saude_list

    # Filtro por equipe
    if equipes:
        equipe_list = [e.strip() for e in equipes.split(',')]
        query_filters.append("initcap_4 = ANY(:equipes)")
        params["equipes"] = equipe_list

    # Filtro por profissional
    if profissionais:
        profissional_list = [p.strip() for p in profissionais.split(',')]
        query_filters.append("initcap_3 = ANY(:profissionais)")
        params["profissionais"] = profissional_list

    # Adiciona os filtros à consulta base
    if query_filters:
        base_query += " AND " + " AND ".join(query_filters)

    # Executa a consulta
    with engine.connect() as connection:
        result = connection.execute(text(base_query), params)
        data = result.fetchone()

        # Acesse os resultados usando índices inteiros
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

# Rota para obter a configuração de autoatualização
@app.route('/api/get-import-config', methods=['GET'])
def get_import_config():
    config_data = importdados.ensure_auto_update_config()
    return jsonify(config_data)

@app.route('/api/save-auto-update-config', methods=['POST'])
def save_auto_update_config_route():
    data = request.json
    is_auto_update_on = data['isAutoUpdateOn']
    auto_update_time = data['autoUpdateTime']

    # Salvar a configuração no arquivo
    importdados.save_auto_update_config(is_auto_update_on, auto_update_time)

    # Se a autoatualização estiver ligada, agendar a importação
    if is_auto_update_on:
        # Verificar se já existe um job para evitar múltiplos jobs
        if len(importdados.scheduler.get_jobs()) > 0:
            importdados.scheduler.remove_all_jobs()

        importdados.schedule_auto_import(importdados.scheduler, auto_update_time)
        print(f"Autoatualização agendada para {auto_update_time}")
    else:
        importdados.scheduler.remove_all_jobs()
        print("Autoatualização desativada")

    return jsonify({"status": "Configuração de autoatualização salva com sucesso!"})

@app.route('/check-file/<import_type>', methods=['GET'])
def check_file(import_type):
    if importdados.is_file_available(import_type):
        return jsonify({"available": True})
    return jsonify({"available": False})



# Carregar a configuração na inicialização e agendar a importação, se necessário
if __name__ == '__main__':
    config = importdados.ensure_auto_update_config()
    if config['isAutoUpdateOn']:
        importdados.schedule_auto_import(importdados.scheduler, config['autoUpdateTime'])

    socketio.run(app, host='0.0.0.0', port=5000, debug=True)  # Executando com suporte ao WebSocket
    print("Executando importação automática de dados...")


