from sqlalchemy import text

def create_application_tables(engine):
    """
    Creates the application tables in the database if they do not already exist.

    Args:
        engine: SQLAlchemy engine instance.
    """
    with engine.connect() as connection:
        with connection.begin():
            # 1. correios_ceps
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS correios_ceps (
                    cep VARCHAR(8) PRIMARY KEY,
                    logradouro TEXT,
                    bairro TEXT,
                    cidade TEXT,
                    uf CHAR(2),
                    ibge VARCHAR(10),
                    origem TEXT
                );
            """))

            # 2. tb_cadastro
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS tb_cadastro (
                    no_cidadao TEXT,
                    no_unidade_saude TEXT,
                    no_profissional TEXT,
                    no_equipe TEXT,
                    st_responsavel_familiar BOOLEAN,
                    st_morador_rua BOOLEAN,
                    st_frequenta_creche BOOLEAN,
                    st_frequenta_cuidador BOOLEAN,
                    st_participa_grupo_comunitario BOOLEAN,
                    st_plano_saude_privado BOOLEAN,
                    st_deficiencia BOOLEAN,
                    st_defi_auditiva BOOLEAN,
                    st_defi_intelectual_cognitiva BOOLEAN,
                    st_defi_visual BOOLEAN,
                    st_defi_fisica BOOLEAN,
                    st_defi_outra BOOLEAN,
                    st_gestante BOOLEAN,
                    st_doenca_respiratoria BOOLEAN,
                    st_doenca_respira_asma BOOLEAN,
                    st_doenca_respira_dpoc_enfisem BOOLEAN,
                    st_doenca_respira_outra BOOLEAN,
                    st_doenca_respira_n_sabe BOOLEAN,
                    st_fumante BOOLEAN,
                    st_alcool BOOLEAN,
                    st_outra_droga BOOLEAN,
                    st_hipertensao_arterial BOOLEAN,
                    st_diabete BOOLEAN,
                    st_avc BOOLEAN,
                    st_hanseniase BOOLEAN,
                    st_tuberculose BOOLEAN,
                    st_cancer BOOLEAN,
                    st_internacao_12 BOOLEAN,
                    st_tratamento_psiquiatra BOOLEAN,
                    st_acamado BOOLEAN,
                    st_domiciliado BOOLEAN,
                    st_usa_planta_medicinal BOOLEAN,
                    st_doenca_cardiaca BOOLEAN,
                    st_doenca_card_insuficiencia BOOLEAN,
                    st_doenca_card_outro BOOLEAN,
                    st_doenca_card_n_sabe BOOLEAN,
                    st_problema_rins BOOLEAN,
                    st_problema_rins_insuficiencia BOOLEAN,
                    st_problema_rins_outro BOOLEAN,
                    st_problema_rins_nao_sabe BOOLEAN,
                    nu_cpf_cidadao TEXT,
                    nu_cns TEXT,
                    co_cidadao TEXT,
                    dt_atualizado TIMESTAMP,
                    dt_nascimento TIMESTAMP,
                    co_seq_fat_cad_individual BIGINT,
                    co_dim_tipo_saida_cadastro INTEGER,
                    nu_micro_area TEXT,
                    nu_micro_area_cidadao TEXT,
                    st_ativo BOOLEAN,
                    st_ficha_inativa BOOLEAN
                );
            """))

            # 3. tb_domicilio
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS tb_domicilio (
                    no_unidade_saude TEXT,
                    no_profissional TEXT,
                    no_equipe TEXT,
                    no_logradouro TEXT,
                    nu_domicilio TEXT,
                    ds_complemento TEXT,
                    no_bairro TEXT,
                    nu_cep VARCHAR(8),
                    st_ativo BOOLEAN,
                    co_seq_cds_cad_domiciliar BIGINT
                );
            """))

            # 4. tb_bpa
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS tb_bpa (
                    prd_uid TEXT,
                    prd_cmp VARCHAR(6),
                    prd_cnsmed VARCHAR(15),
                    prd_cbo VARCHAR(6),
                    prd_flh VARCHAR(3),
                    prd_seq VARCHAR(2),
                    prd_pa TEXT,
                    prd_cnspac VARCHAR(15),
                    prd_cpf_pcnte VARCHAR(11),
                    prd_nmpac VARCHAR(30),
                    prd_dtnasc VARCHAR(8),
                    prd_sexo CHAR(1),
                    prd_ibge VARCHAR(6),
                    prd_dtaten VARCHAR(8),
                    prd_cid VARCHAR(4),
                    prd_idade VARCHAR(3),
                    prd_qt_p INTEGER,
                    prd_caten VARCHAR(2),
                    prd_naut TEXT,
                    prd_org VARCHAR(3),
                    prd_mvm VARCHAR(6),
                    prd_flpa CHAR(1),
                    prd_flcbo CHAR(1),
                    prd_flca CHAR(1),
                    prd_flida CHAR(1),
                    prd_flqt CHAR(1),
                    prd_fler CHAR(1),
                    prd_flmun CHAR(1),
                    prd_flcid CHAR(1),
                    prd_raca VARCHAR(2),
                    prd_servico TEXT,
                    prd_classificacao TEXT,
                    prd_etnia TEXT,
                    prd_nac VARCHAR(3),
                    prd_advqt VARCHAR(2),
                    prd_cnpj TEXT,
                    prd_eqp_area TEXT,
                    prd_eqp_seq TEXT,
                    prd_lograd_pcnte VARCHAR(3),
                    prd_cep_pcnte VARCHAR(8),
                    prd_end_pcnte VARCHAR(30),
                    prd_compl_pcnte VARCHAR(10),
                    prd_num_pcnte VARCHAR(5),
                    prd_bairro_pcnte VARCHAR(30),
                    prd_ddtel_pcnte VARCHAR(2),
                    prd_tel_pcnte VARCHAR(9),
                    prd_email_pcnte TEXT,
                    prd_ine TEXT
                );
            """))

            # 5. tb_visitas
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS tb_visitas (
                    nu_cpf_cidadao TEXT,
                    nu_cns TEXT,
                    co_dim_tipo_ficha INTEGER,
                    co_dim_municipio INTEGER,
                    co_dim_profissional INTEGER,
                    nu_peso NUMERIC,
                    nu_altura NUMERIC,
                    dt_nascimento TIMESTAMP,
                    co_dim_faixa_etaria INTEGER,
                    st_visita_compartilhada BOOLEAN,
                    st_mot_vis_cad_att BOOLEAN,
                    nu_latitude NUMERIC,
                    nu_longitude NUMERIC,
                    com_localizacao INTEGER,
                    sem_localizacao INTEGER,
                    registro_cds INTEGER,
                    ds_dia_semana TEXT,
                    nu_dia INTEGER,
                    nu_mes INTEGER,
                    nu_ano INTEGER,
                    sab_dom INTEGER,
                    ds_turno TEXT,
                    ds_sexo TEXT,
                    no_equipe TEXT,
                    nu_ine TEXT,
                    nu_cnes TEXT,
                    no_unidade_saude TEXT,
                    no_profissional TEXT,
                    dt_visita_mcaf TIMESTAMP,
                    hr_min_seg TIME,
                    anotacao TEXT,
                    idade_atual INTEGER,
                    idade_na_visita INTEGER,
                    origem_visita TEXT,
                    ds_tipo_imovel TEXT
                );
            """))

            # 6. tb_iaf
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS tb_iaf (
                    "Ano" INTEGER,
                    "Mes" INTEGER,
                    "Cnes" TEXT,
                    "Nome da Unidade" TEXT,
                    "Profissional" TEXT,
                    "Cbo" TEXT,
                    "Total de Participantes" INTEGER,
                    "Total Participantes Registrados" INTEGER,
                    "Total de Atividades" INTEGER
                );
            """))

            # 7. tb_pse
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS tb_pse (
                    "Ano" INTEGER,
                    "Mes" INTEGER,
                    "Inep" TEXT,
                    "Nome da Escola" TEXT,
                    "Verificação da Situação Vacinal" INTEGER,
                    "Aplicação de Fluor" INTEGER,
                    "Saude Auditiva" INTEGER,
                    "Saude Ocular" INTEGER,
                    "Praticas Corporais Atividade Fisica" INTEGER,
                    "Antropometria" INTEGER,
                    "Saude Mental" INTEGER,
                    "Saude Sexual e Reprodutiva" INTEGER,
                    "Saude Bucal" INTEGER,
                    "Cidadania e Direitos Humanos" INTEGER,
                    "Agravos e Doenças Negligenciadas" INTEGER,
                    "Prevenção da Violencia" INTEGER,
                    "Prevenção ao Uso de Alcool" INTEGER,
                    "Ações de Combate a Dengue" INTEGER,
                    "Saude Ambiental" INTEGER,
                    "Alimentação Saudavel" INTEGER,
                    "Total de Atividades" INTEGER,
                    "Indicador 1" TEXT,
                    "Indicador 2" TEXT
                );
            """))

            # 8. tb_pse_prof
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS tb_pse_prof (
                    "Ano" INTEGER,
                    "Mes" INTEGER,
                    "Inep" TEXT,
                    "Nome da Escola" TEXT,
                    "PSE" TEXT,
                    "Cns" TEXT,
                    "Profissional" TEXT,
                    "Cbo" TEXT,
                    "Nome Cbo" TEXT,
                    "Ine" TEXT,
                    "Nome da Equipe" TEXT,
                    "Cnes" TEXT,
                    "Nome da Unidade" TEXT,
                    "Total de Participantes" INTEGER,
                    "Total de Participantes Registrados" INTEGER,
                    "Total de Atividades" INTEGER
                );
            """))

            # 9. tb_atendimentos
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS tb_atendimentos (
                    co_seq_fat_atd_ind BIGINT,
                    co_dim_tempo_atend INTEGER,
                    dt_atendimento TIMESTAMP,
                    co_dim_unidade_saude_atend INTEGER,
                    no_unidade_saude_atend TEXT,
                    nu_cnes_atend TEXT,
                    no_equipe_atend TEXT,
                    no_profissional_atend TEXT,
                    nu_peso NUMERIC,
                    nu_altura NUMERIC,
                    ds_filtro_ciaps TEXT,
                    ds_filtro_cids TEXT,
                    co_seq_fat_cidadao_pec BIGINT,
                    no_cidadao TEXT,
                    nu_cns_cidadao TEXT,
                    dt_nascimento TIMESTAMP,
                    ds_sexo TEXT,
                    st_faleceu BOOLEAN,
                    co_seq_fat_cad_individual BIGINT,
                    dt_cadastro_individual TIMESTAMP,
                    st_gestante BOOLEAN,
                    st_hipertensao_arterial BOOLEAN,
                    st_diabete BOOLEAN,
                    ds_dim_tipo_escolaridade TEXT,
                    ds_tipo_localizacao TEXT
                );
            """))
        connection.commit()
    print("Application tables checked/created successfully.")

if __name__ == '__main__':
    # Example usage (requires database connection setup)
    # This part is for testing and might need adjustment based on your project structure
    from sqlalchemy import create_engine
    from backend.init import app  # Assuming your Flask app is initialized here

    # Ensure the app context is active if using app.config
    with app.app_context():
        db_uri = app.config.get('SQLALCHEMY_DATABASE_URI')
        if not db_uri:
            raise ValueError("SQLALCHEMY_DATABASE_URI is not set in the Flask app config.")
        
        engine = create_engine(db_uri)
        
        # Before calling create_application_tables, ensure the database itself exists.
        # The init_db function from models.database should handle database creation.
        from backend.models.database import init_db
        init_db(app) # This will create the 'esus' database if it doesn't exist.

        create_application_tables(engine)
        print("Finished example usage of create_application_tables.")

    # To run this script directly for testing:
    # Ensure your Flask app and its configurations are accessible.
    # You might need to adjust python path or imports.
    # Example: PYTHONPATH=. python backend/database_setup.py
    # (This assumes your 'backend' directory is in the PYTHONPATH or current working directory)

    # Important: The init_db function in models/database.py is responsible for
    # creating the database itself (e.g., 'esus').
    # The create_application_tables function in this file is responsible for
    # creating the tables within that database.
    # Ensure init_db is called before create_application_tables if the database
    # might not exist yet.
    pass
