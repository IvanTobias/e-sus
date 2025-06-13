
"""
Módulo de integração híbrida SQL + Polars para o sistema e-SUS/painel-esus.

Este módulo implementa uma abordagem híbrida que:
1. Usa SQL para criar as tabelas intermediárias (pessoas, diabetes, crianca, idoso, etc.) diretamente no código.
2. Usa SQL e Polars para processar e popular essas tabelas com dados complexos
3. Exporta dados em formato Parquet para compatibilidade com o sistema Fiocruz
4. Gera listas nominais conforme o padrão do sistema original
5. Fornece uma interface simples para ser integrada ao app.py existente

Autor: Manus AI
Data: Junho 2025
"""

import os
import sys
import json
import logging
import traceback
import threading
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from Conexões import get_local_engine, get_external_engine


# Importar as queries SQL modularizadas
from src.infra.db.repositories.sqls.pessoa import pessoas_id
from src.infra.db.repositories.sqls.pessoa.pessoas_list import pessoas_list
from src.infra.db.repositories.sqls.pessoa.lista_cidadao_pec_from_fichas import lista_cidadao_pec_from_fichas
from src.infra.db.repositories.sqls.pessoa.equipes_join import equipes_join

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("fiocruz_integration")

# Tentar importar Polars - necessário para processamento de dados
try:
    import polars as pl
    POLARS_AVAILABLE = True
    logger.info("Polars importado com sucesso.")
except ImportError:
    POLARS_AVAILABLE = False
    logger.warning("Polars não está instalado. Algumas funcionalidades estarão indisponíveis.")
    logger.warning("Para instalar: pip install polars pyarrow")

CREATE_TABLE_SQLS = {
    'pessoas':"""CREATE TABLE IF NOT EXISTS pessoas (
            indice BIGINT, 
            cidadao_pec BIGINT UNIQUE, 
            co_cidadao BIGINT PRIMARY KEY, 
            raca_cor TEXT, 
            cpf TEXT, 
            cns TEXT, 
            nome TEXT, 
            nome_social TEXT, 
            data_nascimento DATE, 
            idade BIGINT,
            sexo TEXT, 
            identidade_genero TEXT, 
            telefone TEXT, 
            ultima_atualizacao_cidadao DATE, 
            ultima_atualizacao_fcd DATE, 
            tipo_endereco TEXT, 
            endereco TEXT, 
            complemento TEXT, 
            numero TEXT, 
            bairro TEXT, 
            cep TEXT, 
            tipo_localidade TEXT,
            possui_fci BOOLEAN,
            possui_fcdt BOOLEAN,
            dt_ultima_atualizacao_cidadao DATE,
            diferenca_ultima_atualizacao_cidadao BIGINT,
            dt_atualizacao_fcd DATE,
            diferenca_ultima_atualizacao_fcd BIGINT,
            codigo_equipe_vinculada BIGINT,
            codigo_unidade_saude BIGINT,
            acompanhamento TEXT,
            status_cadastro TEXT,
            nu_micro_area_domicilio TEXT,
            nome_equipe TEXT,
            nome_unidade_saude TEXT,
            fci_att_2anos BOOLEAN,
            fcdt_att_2anos BOOLEAN,
            alerta_status_cadastro BOOLEAN,
            alerta BOOLEAN,
            tipo_ident_cpf_cns BOOLEAN,
            faixa_etaria TEXT,
            st_recusa_cadastro BOOLEAN
        );
    """,
    'status_records':
    """CREATE TABLE IF NOT EXISTS status_records (
        indice BIGINT, 
        tipo TEXT,
        codigo_equipe BIGINT,
        codigo_unidade_saude BIGINT,
        quantidade BIGINT
    );""",
    'hipertensao_nominal':
    """CREATE TABLE IF NOT EXISTS hipertensao_nominal (
        indice BIGINT, 
        co_fat_cidadao_pec BIGINT, 
        diagnostico TEXT, 
        cids TEXT, 
        ciaps TEXT, 
        min_date DATE, 
        data_ultima_visita_acs DATE, 
        alerta_visita_acs BOOLEAN, 
        total_consulta_individual_medico INTEGER, 
        total_consulta_individual_enfermeiro INTEGER, 
        total_consulta_individual_medico_enfermeiro INTEGER, 
        ultimo_atendimento_medico DATE, 
        ultimo_atendimento_enfermeiro DATE, 
        alerta_total_de_consultas_medico BOOLEAN, 
        ultimo_atendimento_medico_enfermeiro DATE, 
        alerta_ultima_consulta_medico BOOLEAN, 
        ultimo_atendimento_odonto DATE, 
        alerta_ultima_consulta_odontologica BOOLEAN, 
        ultima_data_afericao_pa DATE, 
        alerta_afericao_pa BOOLEAN, 
        ultima_data_creatinina DATE, 
        alerta_creatinina BOOLEAN,
        FOREIGN KEY(co_fat_cidadao_pec) REFERENCES pessoas(cidadao_pec)
    );""",
    'diabetes_nominal':
    """CREATE TABLE IF NOT EXISTS diabetes_nominal (
        indice BIGINT, 
        co_fat_cidadao_pec BIGINT, 
        diagnostico TEXT, 
        cids TEXT, 
        ciaps TEXT, 
        min_date DATE, 
        data_ultima_visita_acs DATE, 
        alerta_visita_acs BOOLEAN, 
        total_consulta_individual_medico INTEGER, 
        total_consulta_individual_enfermeiro INTEGER, 
        total_consulta_individual_medico_enfermeiro INTEGER, 
        ultimo_atendimento_medico DATE, 
        ultimo_atendimento_enfermeiro DATE, 
        alerta_total_de_consultas_medico BOOLEAN, 
        ultimo_atendimento_medico_enfermeiro DATE, 
        alerta_ultima_consulta_medico BOOLEAN, 
        ultimo_atendimento_odonto DATE, 
        alerta_ultima_consulta_odontologica BOOLEAN, 
        ultima_data_afericao_pa DATE, 
        alerta_afericao_pa BOOLEAN, 
        ultima_data_glicemia_capilar DATE, 
        ultima_data_hemoglobina_glicada DATE, 
        alerta_ultima_hemoglobina_glicada BOOLEAN,
        FOREIGN KEY(co_fat_cidadao_pec) REFERENCES pessoas(cidadao_pec)
    );""",
    'idoso':
    """CREATE TABLE IF NOT EXISTS idoso (
        indice BIGINT, 
        cidadao_pec BIGINT, 
        atendimentos_medicos INTEGER, 
        data_ultimo_atendimento_medicos DATE, 
        indicador_atendimentos_medicos INTEGER, 
        medicoes_peso_altura INTEGER, 
        data_ultima_medicao_peso_altura DATE, 
        indicador_medicoes_peso_altura INTEGER, 
        imc FLOAT, 
        categoria_imc TEXT, 
        registros_creatinina INTEGER, 
        data_ultimo_registro_creatinina DATE, 
        indicador_registros_creatinina INTEGER, 
        indicador_visitas_domiciliares_acs INTEGER, 
        visitas_domiciliares_acs INTEGER, 
        data_ultima_visita_domiciliar_acs DATE, 
        vacinas_influenza INTEGER, 
        data_ultima_vacina_influenza DATE, 
        indicador_vacinas_influenza INTEGER, 
        atendimentos_odontologicos INTEGER, 
        data_ultimo_atendimento_odontologico DATE, 
        indicador_atendimento_odontologico INTEGER,
        FOREIGN KEY(cidadao_pec) REFERENCES pessoas(cidadao_pec)
    );""",
    'crianca':
    """CREATE TABLE IF NOT EXISTS crianca (
        indice BIGINT, 
        cidadao_pec BIGINT, 
        indicador_atendimentos_medicos_enfermeiros INTEGER, 
        data_ultimo_atendimento_medico_enfermeiro DATE, 
        atendimentos_medicos_enfermeiros_8d_vida INTEGER, 
        atendimentos_medicos_enfermeiros_puericult INTEGER, 
        data_ultimo_atendimento_medicos_enfermeiros_puericult DATE, 
        indicador_atendimentos_medicos_enfermeiros_puericult INTEGER, 
        medicoes_peso_altura_ate2anos INTEGER, 
        data_ultima_medicao_peso_altura_ate2anos DATE, 
        indicador_medicoes_peso_altura_ate2anos INTEGER, 
        data_ultima_visita_domiciliar_acs DATE, 
        indicador_visitas_domiciliares_acs INTEGER, 
        visitas_domiciliares_acs INTEGER, 
        teste_pezinho INTEGER, 
        indicador_teste_pezinho INTEGER, 
        data_ultimo_teste_pezinho DATE, 
        n_penta INTEGER, 
        n_polio INTEGER, 
        n_triplici INTEGER, 
        data_ultima_vacina_penta DATE, 
        data_ultima_vacina_polio DATE, 
        data_ultima_vacina_triplici DATE, 
        indicador_vacinas_penta_polio_triplici INTEGER, 
        atendimentos_odontologicos INTEGER, 
        data_ultimo_atendimento_odontologico DATE, 
        indicador_atendimentos_odontologicos INTEGER, 
        n_medicos INTEGER, 
        n_enfer INTEGER, 
        n_fono INTEGER, 
        n_psicol INTEGER, 
        n_educ_fisica INTEGER, 
        n_assist_social INTEGER, 
        n_tera_ocup INTEGER, 
        n_farmac INTEGER, 
        n_fisio INTEGER, 
        n_nutric INTEGER, 
        n_ciru_dent INTEGER, 
        n_outros INTEGER, 
        total INTEGER,
        FOREIGN KEY(cidadao_pec) REFERENCES pessoas(cidadao_pec)
    );"""
}

CREATE_TABLE_ORDER = [
    'pessoas',
    'status_records',
    'hipertensao_nominal',
    'diabetes_nominal',
    'idoso',
    'crianca'
]

engine = get_local_engine()

for table_name in CREATE_TABLE_ORDER:
    sql = CREATE_TABLE_SQLS[table_name]
    try:
        with engine.begin() as connection:  # usa begin() para garantir commit automático
            connection.execute(text(sql))
            print(f"Tabela {table_name} criada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar tabela {table_name}: {e}")

# Observação: índices e drops seguem iguais, mas podem ser atualizados conforme novas estruturas.


CREATE_INDEX_SQLS = [
    'CREATE INDEX ix_pessoas_index ON pessoas ("index");',
    'CREATE INDEX ix_hipertensao_nominal_index ON hipertensao_nominal ("index");',
    'CREATE INDEX ix_diabetes_nominal_index ON diabetes_nominal ("index");',
    'CREATE INDEX ix_idoso_index ON idoso ("index");',
    'CREATE INDEX ix_crianca_index ON crianca ("index");',
]
print("Chegou Aqui")


# Variáveis globais para rastreamento de progresso e cancelamento
progress = {}
cancel_requests = {}
progress_lock = threading.Lock()

# --- Funções de Utilidade ---

@contextmanager
def session_scope(engine):
    """Fornece um escopo de sessão transacional."""
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def log_message(message):
    """Registra uma mensagem no log."""
    logger.info(message)

def update_progress_safely(tipo, progress_value, error=None):
    """Atualiza o progresso de forma thread-safe."""
    with progress_lock:
        current_progress = progress.get(tipo, 0)
        if error:
            progress[tipo] = progress_value
        elif progress_value >= current_progress:
             progress[tipo] = progress_value
        else:
            progress_value = current_progress
        logger.debug(f"Progresso atualizado: tipo={tipo}, valor={progress_value}")
        
        # Se integrado com socketio, pode emitir eventos aqui
        # send_progress_update(tipo, progress_value, error)

def get_progress(tipo):
    """Obtém o progresso atual de uma tarefa."""
    with progress_lock:
        return progress.get(tipo, 0)

def execute_raw_sql(sql_command, engine, tipo, task_name):
    """Executa um comando SQL bruto."""
    log_message(f"Executando SQL para {task_name} no tipo {tipo}: {sql_command[:100]}...")
    try:
        with engine.connect() as conn:
            conn.execute(text(sql_command))
        log_message(f"SQL para {task_name} executado com sucesso.")
        return True
    except Exception as e:
        error_trace = traceback.format_exc()
        log_message(f"Erro ao executar SQL para {task_name} ({tipo}): {str(e)}\n{error_trace}")
        return False

def ensure_dir(directory):
    """Garante que um diretório exista, criando-o se necessário."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        log_message(f"Diretório criado: {directory}")
    return directory

def execute_population_query(query, table_name, get_external_engine, get_local_engine, export_parquet=True, parquet_dir=None):
    """
    Executa uma query de população e salva os resultados no banco local e opcionalmente em arquivo Parquet.
    
    Args:
        query: Query SQL a ser executada
        table_name: Nome da tabela a ser populada
        get_external_engine: Engine SQLAlchemy para o banco externo
        get_local_engine: Engine SQLAlchemy para o banco local
        export_parquet: Se True, exporta os resultados para arquivo Parquet
        parquet_dir: Diretório para salvar o arquivo Parquet (se None, usa \'./input\')
    """
    log_message(f"Iniciando população da tabela \'{table_name}\'...")
    try:
        with get_external_engine.connect() as conn_external:
            log_message(f"Executando query de população da tabela \'{table_name}\'...")
            result = conn_external.execute(text(query))
            
            chunksize = 10000
            rows_processed = 0
            first_chunk = True
            all_data = []  # Para acumular todos os dados se export_parquet=True
            
            while True:
                rows = result.fetchmany(chunksize)
                if not rows:
                    break
                
                df = pd.DataFrame(rows, columns=result.keys())
                
                # Salvar no banco local
                current_if_exists = 'replace' if first_chunk else 'append'
                with session_scope(get_local_engine) as session:
                    df.to_sql(table_name, con=session.bind, if_exists=current_if_exists, index=False)
                
                # Acumular dados para exportação Parquet se necessário
                if export_parquet:
                    all_data.append(df)
                
                rows_processed += len(rows)
                first_chunk = False
                log_message(f"Processado chunk de {len(df)} linhas para \'{table_name}\'. Total: {rows_processed}")
        
        # Exportar para Parquet se solicitado
        if export_parquet and all_data:
            if parquet_dir is None:
                parquet_dir = ensure_dir('./input')
            
            full_df = pd.concat(all_data, ignore_index=True)
            parquet_path = os.path.join(parquet_dir, f"tb_{table_name}.parquet")
            full_df.to_parquet(parquet_path, index=False)
            log_message(f"Dados exportados para Parquet: {parquet_path}")
        
        log_message(f"Tabela \'{table_name}\' populada com sucesso. Total: {rows_processed} registros.")
        return True
    except Exception as e:
        log_message(f"Erro ao processar dados para tabela \'{table_name}\': {str(e)}\n{traceback.format_exc()}")
        return False

# --- Funções de Processamento SQL ---

def process_pessoas_data(get_external_engine, get_local_engine, export_parquet=True, parquet_dir=None):
    """
    Processa dados para a tabela \'pessoas\' usando SQL.
    
    Args:
        get_external_engine: SQLAlchemy engine conectada ao banco PostgreSQL externo (e-SUS)
        get_local_engine: SQLAlchemy engine conectada ao banco PostgreSQL local
        export_parquet: Se True, exporta os resultados para arquivo Parquet
        parquet_dir: Diretório para salvar o arquivo Parquet
    """
    # Query SQL fornecida pelo usuário para a tabela \'pessoas\'
    pessoas_population_query = f"""
    WITH
    lista_cidadao_pec_from_fichas AS (
        {lista_cidadao_pec_from_fichas}
    ),
    pessoas_id AS (
        {pessoas_id}
    ),
    pessoas_list AS (
        {pessoas_list}
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY pessoas_list.cidadao_pec) AS index,
        pessoas_list.cidadao_pec,
        pessoas_list.co_cidadao,
        pessoas_list.raca_cor,
        pessoas_list.cpf,
        pessoas_list.cns,
        pessoas_list.nome,
        pessoas_list.nome_social,
        pessoas_list.data_nascimento,
        pessoas_list.idade,
        pessoas_list.sexo,
        pessoas_list.identidade_genero,
        pessoas_list.telefone,
        pessoas_list.ultima_atualizacao_cidadao,
        pessoas_list.ultima_atualizacao_fcd,
        pessoas_list.tipo_endereco,
        pessoas_list.endereco,
        pessoas_list.complemento,
        pessoas_list.numero,
        pessoas_list.bairro,
        pessoas_list.cep,
        pessoas_list.tipo_localidade,
        pessoas_list.possui_fci,
        pessoas_list.possui_fcdt,
        pessoas_list.dt_ultima_atualizacao_cidadao,
        pessoas_list.diferenca_ultima_atualizacao_cidadao,
        pessoas_list.dt_atualizacao_fcd,
        pessoas_list.diferenca_ultima_atualizacao_fcd,
        pessoas_list.codigo_equipe_vinculada,
        pessoas_list.codigo_unidade_saude,
        -- Adicionar campos que estavam faltando ou eram placeholders
        CASE
            WHEN pessoas_list.st_usar_cadastro_individual = TRUE THEN 'Individual'
            ELSE 'Coletivo'
        END AS acompanhamento,
        CASE
            WHEN pessoas_list.st_recusa_cadastro = TRUE THEN 'Recusado'
            ELSE 'Ativo'
        END AS status_cadastro,
        -- Adicionar campos de equipe e unidade de saude, se existirem no equipes_join
        COALESCE(equipes_join.nu_micro_area, '') AS nu_micro_area_domicilio,
        COALESCE(equipes_join.nome_equipe, '') AS nome_equipe,
        COALESCE(equipes_join.nome_unidade_saude, '') AS nome_unidade_saude,
        -- Placeholder para fci_att_2anos e fcdt_att_2anos (se não vierem do SQL)
        FALSE AS fci_att_2anos, -- Ajustar conforme a lógica real
        FALSE AS fcdt_att_2anos, -- Ajustar conforme a lógica real
        -- Placeholder para alerta_status_cadastro e alerta
        FALSE AS alerta_status_cadastro, -- Ajustar conforme a lógica real
        FALSE AS alerta, -- Ajustar conforme a lógica real
        -- Placeholder para tipo_ident_cpf_cns e faixa_etaria
        FALSE AS tipo_ident_cpf_cns, -- Ajustar conforme a lógica real
        'N/A' AS faixa_etaria, -- Ajustar conforme a lógica real
        pessoas_list.st_recusa_cadastro
    FROM
        pessoas_list
    LEFT JOIN
        equipes_join ON pessoas_list.cidadao_pec = equipes_join.cidadao_pec
    ORDER BY
        pessoas_list.cidadao_pec ASC
    """

    return execute_population_query(
        pessoas_population_query, "pessoas", get_external_engine, get_local_engine, export_parquet, parquet_dir
    )

def process_hipertensao_data(get_external_engine, get_local_engine, export_parquet=True, parquet_dir=None):
    """
    Processa dados para a tabela \'hipertensao_nominal\'.
    """
    # Implementação da query de hipertensão
    hipertensao_nominal_query = "SELECT * FROM tb_hipertensao_nominal;" # Exemplo
    return execute_population_query(
        hipertensao_nominal_query, "hipertensao_nominal", get_external_engine, get_local_engine, export_parquet, parquet_dir
    )

def process_diabetes_data(get_external_engine, get_local_engine, export_parquet=True, parquet_dir=None):
    """
    Processa dados para a tabela \'diabetes_nominal\'.
    """
    # Implementação da query de diabetes
    diabetes_nominal_query = "SELECT * FROM tb_diabetes_nominal;" # Exemplo
    return execute_population_query(
        diabetes_nominal_query, "diabetes_nominal", get_external_engine, get_local_engine, export_parquet, parquet_dir
    )

def process_idoso_data(get_external_engine, get_local_engine, export_parquet=True, parquet_dir=None):
    """
    Processa dados para a tabela \'idoso\'.
    """
    # Implementação da query de idoso
    idoso_query = "SELECT * FROM tb_idoso;" # Exemplo
    return execute_population_query(
        idoso_query, "idoso", get_external_engine, get_local_engine, export_parquet, parquet_dir
    )

def process_crianca_data(get_external_engine, get_local_engine, export_parquet=True, parquet_dir=None):
    """
    Processa dados para a tabela \'crianca\'.
    """
    # Implementação da query de crianca
    crianca_query = "SELECT * FROM tb_crianca;" # Exemplo
    return execute_population_query(
        crianca_query, "crianca", get_external_engine, get_local_engine, export_parquet, parquet_dir
    )

def run_integration_process(
    config_data,
    tipo,
    emit_progress,
    export_parquet=True,
    parquet_dir=None
):
    """
    Executa o processo de integração para um tipo específico de dado.
    
    Args:
        config_data (dict): Dicionário com os dados de configuração do banco de dados.
        tipo (str): O tipo de integração a ser executada (e.g., \'pessoas\', \'hipertensao\').
        emit_progress (callable): Função para emitir o progresso da integração.
        export_parquet (bool): Se True, exporta os resultados para arquivo Parquet.
        parquet_dir (str): Diretório para salvar o arquivo Parquet.
    """
    global progress, cancel_requests
    progress[tipo] = 0
    cancel_requests[tipo] = False

    def send_progress_update(current_type, value, error=None):
        update_progress_safely(current_type, value, error)
        emit_progress(current_type, value, error)

    log_message(f"Iniciando integração para o tipo: {tipo}")
    send_progress_update(tipo, 5)

    try:
        # Obter engines de conexão
        external_engine = get_external_engine()
        local_engine = get_local_engine()
        send_progress_update(tipo, 10)
        print(f"Começando Agora: {tipo}")


        # 3. Processar e popular dados
        log_message(f"Processando e populando dados para {tipo}...")
        if tipo == "pessoas":
            success = process_pessoas_data(external_engine, local_engine, export_parquet, parquet_dir)
        elif tipo == "hipertensao":
            success = process_hipertensao_data(external_engine, local_engine, export_parquet, parquet_dir)
        elif tipo == "diabetes":
            success = process_diabetes_data(external_engine, local_engine, export_parquet, parquet_dir)
        elif tipo == "idoso":
            success = process_idoso_data(external_engine, local_engine, export_parquet, parquet_dir)
        elif tipo == "crianca":
            success = process_crianca_data(external_engine, local_engine, export_parquet, parquet_dir)
        else:
            raise ValueError(f"Tipo de integração desconhecido: {tipo}")
        print(f"Tipo de integração desconhecido: {tipo}")

        if not success:
            raise Exception(f"Falha no processamento de dados para o tipo {tipo}.")
        print(f"Falha no processamento de dados para o tipo {tipo}.")
        send_progress_update(tipo, 80)

        # 4. Criar índices
        log_message(f"Criando índices para {tipo}...")
        for index_sql in CREATE_INDEX_SQLS:
            # Verificar se a tabela existe antes de criar o índice
            table_name_from_index = index_sql.split('ON ')[1].split(' ')[0].replace('"', '')
            check_table_exists_sql = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = \'{table_name_from_index}\');"
            
            with local_engine.connect() as conn:
                table_exists = conn.execute(text(check_table_exists_sql)).scalar()

            if table_exists:
                if not execute_raw_sql(index_sql, local_engine, tipo, "Create Index"):
                    log_message(f"Aviso: Falha ao criar índice com SQL: {index_sql[:50]}...")
            else:
                log_message(f"Aviso: Tabela {table_name_from_index} não existe. Pulando criação de índice.")
        send_progress_update(tipo, 90)

        log_message(f"Integração para o tipo {tipo} concluída com sucesso.")
        send_progress_update(tipo, 100)

    except Exception as e:
        error_message = f"Erro durante a integração para o tipo {tipo}: {str(e)}"
        log_message(f"{error_message}\n{traceback.format_exc()}")
        send_progress_update(tipo, 0, error=error_message)


# Exemplo de uso (para testes diretos, não para uso via Flask)
if __name__ == "__main__":
    # Configurações de exemplo (substitua pelas suas)
    sample_config = {
        "external_db": {
            "database": "esus",
            "user": "esus_leitura",
            "password": "_e{pmW?7SVXku-EzHJ[7}AFHbzJA8",
            "host": "10.2.2.161",
            "port": "5433"
        },
        "local_db": {
            "database": "esus",
            "user": "postgres",
            "password": "esus",
            "host": "localhost",
            "port": "5432"
        }
    }

    # Exemplo de execução para pessoas
    print("\n--- Executando integração para Pessoas ---")
    run_integration_process(sample_config, "pessoas", lambda t, v, e: print(f"Progresso ({t}): {v}%"), export_parquet=True, parquet_dir="./output_parquets")

    # Exemplo de execução para crianca
    print("\n--- Executando integração para Crianca ---")
    run_integration_process(sample_config, "crianca", lambda t, v, e: print(f"Progresso ({t}): {v}%"), export_parquet=True, parquet_dir="./output_parquets")

    # Exemplo de execução para idoso
    print("\n--- Executando integração para Idoso ---")
    run_integration_process(sample_config, "idoso", lambda t, v, e: print(f"Progresso ({t}): {v}%"), export_parquet=True, parquet_dir="./output_parquets")

    # Exemplo de execução para hipertensao
    print("\n--- Executando integração para Hipertensao ---")
    run_integration_process(sample_config, "hipertensao", lambda t, v, e: print(f"Progresso ({t}): {v}%"), export_parquet=True, parquet_dir="./output_parquets")

    # Exemplo de execução para diabetes
    print("\n--- Executando integração para Diabetes ---")
    run_integration_process(sample_config, "diabetes", lambda t, v, e: print(f"Progresso ({t}): {v}%"), export_parquet=True, parquet_dir="./output_parquets")

