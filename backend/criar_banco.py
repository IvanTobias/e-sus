import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def ensure_database_exists(dbname, user, password, host='localhost', port='5432'):
    """
    Garante que o banco de dados exista com ENCODING UTF8.
    """
    conn = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Verifica se o banco já existe
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
    exists = cur.fetchone()

    if not exists:
        print(f"[SETUP] Banco '{dbname}' não encontrado. Criando com ENCODING 'UTF8'...")
        cur.execute("CREATE DATABASE %s ENCODING 'UTF8' TEMPLATE template0" % psycopg2.extensions.quote_ident(dbname, cur))
        print(f"[SETUP] Banco '{dbname}' criado com sucesso.")
    else:
        # Verifica o encoding real do banco existente
        cur.execute("SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = %s", (dbname,))
        encoding_name = cur.fetchone()[0].upper()
        if encoding_name != 'UTF8':
            print(f"[AVISO] O banco '{dbname}' existe, mas não está com encoding UTF8 (encoding='{encoding_name}').")
            print("[ERRO] Isso pode causar erros de acentuação ou UnicodeDecodeError.")
            print("[SOLUÇÃO] Dropar e recriar com: CREATE DATABASE esus ENCODING 'UTF8' TEMPLATE template0;")

    cur.close()
    conn.close()





from sqlalchemy import text, inspect
from Conexões import get_local_engine

def ensure_tables_exist():
    engine = get_local_engine()
    inspector = inspect(engine)
    required_tables = {
        'tb_cadastro': """
            CREATE TABLE IF NOT EXISTS tb_cadastro (
                co_seq_fat_cad_individual SERIAL PRIMARY KEY,
                no_cidadao VARCHAR,
                no_unidade_saude VARCHAR,
                no_profissional VARCHAR,
                no_equipe VARCHAR,
                nu_cns VARCHAR,
                nu_cpf_cidadao VARCHAR,
                co_cidadao VARCHAR,
                dt_atualizado DATE,
                dt_nascimento DATE,
                co_dim_tipo_saida_cadastro VARCHAR,
                nu_micro_area VARCHAR,
                nu_micro_area_cidadao VARCHAR,
                st_ativo BOOLEAN,
                st_ficha_inativa BOOLEAN
            );
        """,
        'tb_domicilio': """
            CREATE TABLE IF NOT EXISTS tb_domicilio (
                co_seq_cds_cad_domiciliar SERIAL PRIMARY KEY,
                no_unidade_saude VARCHAR,
                no_profissional VARCHAR,
                no_equipe VARCHAR,
                no_logradouro VARCHAR,
                nu_domicilio VARCHAR,
                ds_complemento VARCHAR,
                no_bairro VARCHAR,
                nu_cep VARCHAR,
                st_ativo BOOLEAN
            );
        """,
        'tb_bpa': """
            CREATE TABLE IF NOT EXISTS tb_bpa (
                prd_uid VARCHAR,
                prd_cmp VARCHAR,
                prd_cnsmed VARCHAR,
                prd_cbo VARCHAR,
                prd_flh VARCHAR,
                prd_seq VARCHAR,
                prd_pa VARCHAR,
                prd_cnsplac VARCHAR,
                prd_cpf_pcnte VARCHAR,
                prd_nmpac VARCHAR,
                prd_dtnasc VARCHAR,
                prd_sexo VARCHAR,
                prd_ibge VARCHAR,
                prd_dtaten VARCHAR,
                prd_cid VARCHAR,
                prd_idade VARCHAR,
                prd_qt_p INTEGER,
                prd_end_pcnte VARCHAR,
                prd_bairro_pcnte VARCHAR,
                prd_cep_pcnte VARCHAR
            );
        """,
        'tb_visitas': """
            CREATE TABLE IF NOT EXISTS tb_visitas (
                co_seq_dim_profissional INTEGER,
                no_unidade_saude VARCHAR,
                no_profissional VARCHAR,
                no_equipe VARCHAR,
                dt_visita_mcaf DATE,
                nu_latitude FLOAT,
                nu_longitude FLOAT,
                co_dim_desfecho_visita INTEGER
            );
        """,
        'tb_iaf': """
            CREATE TABLE IF NOT EXISTS tb_iaf (
                nu_ano INTEGER,
                nu_mes VARCHAR,
                cnes VARCHAR,
                nome_da_unidade VARCHAR,
                profissional VARCHAR,
                cbo VARCHAR,
                total_de_participantes INTEGER,
                total_participantes_registrados INTEGER,
                total_de_atividades INTEGER
            );
        """,
        'tb_pse': """
            CREATE TABLE IF NOT EXISTS tb_pse (
                inep VARCHAR,
                nome_da_escola VARCHAR,
                total_de_atividades INTEGER,
                indicador_1 VARCHAR,
                indicador_2 VARCHAR,
                ano INTEGER,
                mes VARCHAR
            );
        """,
        'tb_pse_prof': """
            CREATE TABLE IF NOT EXISTS tb_pse_prof (
                inep VARCHAR,
                nome_da_escola VARCHAR,
                profissional VARCHAR,
                nome_cbo VARCHAR,
                nome_da_unidade VARCHAR,
                total_de_participantes INTEGER,
                total_de_participantes_registrados INTEGER,
                total_de_atividades INTEGER,
                ano INTEGER,
                mes VARCHAR
            );
        """,
        'tb_atendimentos': """
            CREATE TABLE IF NOT EXISTS tb_atendimentos (
                co_seq_fat_atd_ind SERIAL PRIMARY KEY,
                dt_atendimento DATE,
                no_unidade_saude_atend VARCHAR,
                no_profissional_atend VARCHAR,
                no_cidadao VARCHAR,
                nu_cns_cidadao VARCHAR,
                nu_cpf_cidadao VARCHAR
            );
        """
    }

    print("[SETUP] Verificando/criando tabelas...")
    with engine.begin() as conn:
        for table_name, create_sql in required_tables.items():
            if table_name not in inspector.get_table_names():
                print(f"[SETUP] Criando tabela {table_name}...")
                conn.execute(text(create_sql))
            else:
                print(f"[SETUP] Tabela {table_name} já existe.")


def setup_local_database():
    ensure_database_exists('esus', 'postgres', '1234', host='localhost', port='5432')
    ensure_tables_exist()

    # Finaliza conexões abertas
    engine = get_local_engine()
    engine.dispose()
