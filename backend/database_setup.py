from sqlalchemy import text

# Schemas obtained from schemas_definition.py and analysis of Consultas.py
schemas = {
    "tb_cadastro": {
        "no_cidadao": "TEXT",
        "no_unidade_saude": "TEXT",
        "no_profissional": "TEXT",
        "no_equipe": "TEXT",
        "st_responsavel_familiar": "INTEGER", # Assuming 0 or 1
        "st_morador_rua": "INTEGER",
        "st_frequenta_creche": "INTEGER",
        "st_frequenta_cuidador": "INTEGER",
        "st_participa_grupo_comunitario": "INTEGER",
        "st_plano_saude_privado": "INTEGER",
        "st_deficiencia": "INTEGER",
        "st_defi_auditiva": "INTEGER",
        "st_defi_intelectual_cognitiva": "INTEGER",
        "st_defi_visual": "INTEGER",
        "st_defi_fisica": "INTEGER",
        "st_defi_outra": "INTEGER",
        "st_gestante": "INTEGER",
        "st_doenca_respiratoria": "INTEGER",
        "st_doenca_respira_asma": "INTEGER",
        "st_doenca_respira_dpoc_enfisem": "INTEGER",
        "st_doenca_respira_outra": "INTEGER",
        "st_doenca_respira_n_sabe": "INTEGER",
        "st_fumante": "INTEGER",
        "st_alcool": "INTEGER",
        "st_outra_droga": "INTEGER",
        "st_hipertensao_arterial": "INTEGER",
        "st_diabete": "INTEGER",
        "st_avc": "INTEGER",
        "st_hanseniase": "INTEGER",
        "st_tuberculose": "INTEGER",
        "st_cancer": "INTEGER",
        "st_internacao_12": "INTEGER",
        "st_tratamento_psiquiatra": "INTEGER",
        "st_acamado": "INTEGER",
        "st_domiciliado": "INTEGER",
        "st_usa_planta_medicinal": "INTEGER",
        "st_doenca_cardiaca": "INTEGER",
        "st_doenca_card_insuficiencia": "INTEGER",
        "st_doenca_card_outro": "INTEGER",
        "st_doenca_card_n_sabe": "INTEGER",
        "st_problema_rins": "INTEGER",
        "st_problema_rins_insuficiencia": "INTEGER",
        "st_problema_rins_outro": "INTEGER",
        "st_problema_rins_nao_sabe": "INTEGER",
        "nu_cpf_cidadao": "VARCHAR(11)",
        "nu_cns": "VARCHAR(15)",
        "co_cidadao": "BIGINT", # Assuming co_seq_cidadao from tb_cidadao is BIGINT
        "dt_atualizado": "TIMESTAMP",
        "dt_nascimento": "DATE",
        "co_seq_fat_cad_individual": "BIGINT PRIMARY KEY", # Assuming this is the primary key for the table
        "co_dim_tipo_saida_cadastro": "INTEGER",
        "nu_micro_area": "VARCHAR(10)", # Max length observed in some systems
        "nu_micro_area_cidadao": "VARCHAR(10)",
        "st_ativo": "INTEGER", # 0 or 1
        "st_ficha_inativa": "INTEGER" # 0 or 1
    },
    "tb_domicilio": {
        "no_unidade_saude": "TEXT",
        "no_profissional": "TEXT",
        "no_equipe": "TEXT",
        "no_logradouro": "TEXT",
        "nu_domicilio": "VARCHAR(10)",
        "ds_complemento": "TEXT",
        "no_bairro": "TEXT",
        "nu_cep": "VARCHAR(8)",
        "st_ativo": "INTEGER", # From tb_equipe, likely 0 or 1
        "co_seq_cds_cad_domiciliar": "BIGINT PRIMARY KEY" # Assuming this is the primary key
    },
    "tb_bpa": {
        "PRD_IDENT": "VARCHAR(2)",
        "PRD_UID": "VARCHAR(7)",
        "PRD_CMP": "VARCHAR(6)",
        "PRD_CNSMED": "VARCHAR(15)",
        "PRD_CBO": "VARCHAR(6)",
        "PRD_DTATEN": "VARCHAR(8)",
        "PRD_FLH": "VARCHAR(3)",
        "PRD_SEQ": "VARCHAR(2)",
        "PRD_PA": "VARCHAR(10)",
        "PRD_CNSPAC": "VARCHAR(15)",
        "PRD_SEXO": "CHAR(1)",
        "PRD_IBGE": "VARCHAR(6)",
        "PRD_CID": "VARCHAR(4)",
        "PRD_IDADE": "VARCHAR(3)",
        "PRD_QT_P": "VARCHAR(6)",
        "PRD_CATEN": "VARCHAR(2)",
        "PRD_NAUT": "VARCHAR(13)",
        "PRD_ORG": "VARCHAR(3)",
        "PRD_NMPAC": "VARCHAR(30)",
        "PRD_DTNASC": "VARCHAR(8)",
        "PRD_RACA": "VARCHAR(2)",
        "PRD_ETNIA": "VARCHAR(4)",
        "PRD_NAC": "VARCHAR(3)",
        "PRD_SERVICO": "VARCHAR(3)",
        "PRD_CLASSIFICACAO": "VARCHAR(3)",
        "PRD_EQP_SEQ": "VARCHAR(8)",
        "PRD_EQP_AREA": "VARCHAR(4)",
        "PRD_CNPJ": "VARCHAR(14)",
        "PRD_CEP_PCNTE": "VARCHAR(8)",
        "PRD_LOGRAD_PCNTE": "VARCHAR(3)",
        "PRD_END_PCNTE": "VARCHAR(30)",
        "PRD_COMPL_PCNTE": "VARCHAR(10)",
        "PRD_NUM_PCNTE": "VARCHAR(5)",
        "PRD_BAIRRO_PCNTE": "VARCHAR(30)",
        "PRD_DDTEL_PCNTE": "VARCHAR(2)",
        "PRD_TEL_PCNTE": "VARCHAR(9)",
        "PRD_EMAIL_PCNTE": "VARCHAR(40)",
        "PRD_INE": "VARCHAR(10)",
        "PRD_CPF_PCNTE": "VARCHAR(11)",
        "PRD_MVM": "VARCHAR(6)"
    },
    "tb_visitas": {
        "nu_cpf_cidadao": "TEXT",
        "nu_cns": "TEXT",
        "co_dim_tipo_ficha": "INTEGER",
        "co_dim_municipio": "INTEGER",
        "co_dim_profissional": "INTEGER",
        "co_dim_tipo_imovel": "INTEGER",
        "nu_micro_area": "VARCHAR(2)",
        "nu_peso": "DECIMAL",
        "nu_altura": "DECIMAL",
        "dt_nascimento": "DATE",
        "co_dim_faixa_etaria": "INTEGER",
        "st_visita_compartilhada": "INTEGER", # BOOLEAN
        "st_mot_vis_cad_att": "INTEGER", # BOOLEAN
        "st_mot_vis_visita_periodica": "INTEGER", # BOOLEAN
        "st_mot_vis_busca_ativa": "INTEGER", # BOOLEAN
        "st_mot_vis_acompanhamento": "INTEGER", # BOOLEAN
        "st_mot_vis_egresso_internacao": "INTEGER", # BOOLEAN
        "st_mot_vis_ctrl_ambnte_vetor": "INTEGER", # BOOLEAN
        "st_mot_vis_convte_atvidd_cltva": "INTEGER", # BOOLEAN
        "st_mot_vis_orintacao_prevncao": "INTEGER", # BOOLEAN
        "st_mot_vis_outros": "INTEGER", # BOOLEAN
        "st_busca_ativa_consulta": "INTEGER", # BOOLEAN
        "st_busca_ativa_exame": "INTEGER", # BOOLEAN
        "st_busca_ativa_vacina": "INTEGER", # BOOLEAN
        "st_busca_ativa_bolsa_familia": "INTEGER", # BOOLEAN
        "st_acomp_gestante": "INTEGER", # BOOLEAN
        "st_acomp_puerpera": "INTEGER", # BOOLEAN
        "st_acomp_recem_nascido": "INTEGER", # BOOLEAN
        "st_acomp_crianca": "INTEGER", # BOOLEAN
        "st_acomp_pessoa_desnutricao": "INTEGER", # BOOLEAN
        "st_acomp_pessoa_reabil_deficie": "INTEGER", # BOOLEAN
        "st_acomp_pessoa_hipertensao": "INTEGER", # BOOLEAN
        "st_acomp_pessoa_diabetes": "INTEGER", # BOOLEAN
        "st_acomp_pessoa_asma": "INTEGER", # BOOLEAN
        "st_acomp_pessoa_dpoc_enfisema": "INTEGER", # BOOLEAN
        "st_acomp_pessoa_cancer": "INTEGER", # BOOLEAN
        "st_acomp_pessoa_doenca_cronica": "INTEGER", # BOOLEAN
        "st_acomp_pessoa_hanseniase": "INTEGER", # BOOLEAN
        "st_acomp_pessoa_tuberculose": "INTEGER", # BOOLEAN
        "st_acomp_sintomaticos_respirat": "INTEGER", # BOOLEAN
        "st_acomp_tabagista": "INTEGER", # BOOLEAN
        "st_acomp_domiciliados_acamados": "INTEGER", # BOOLEAN
        "st_acomp_condi_vulnerab_social": "INTEGER", # BOOLEAN
        "st_acomp_condi_bolsa_familia": "INTEGER", # BOOLEAN
        "st_acomp_saude_mental": "INTEGER", # BOOLEAN
        "st_acomp_usuario_alcool": "INTEGER", # BOOLEAN
        "st_acomp_usuario_outras_drogra": "INTEGER", # BOOLEAN
        "st_ctrl_amb_vet_acao_educativa": "INTEGER", # BOOLEAN
        "st_ctrl_amb_vet_imovel_foco": "INTEGER", # BOOLEAN
        "st_ctrl_amb_vet_acao_mecanica": "INTEGER", # BOOLEAN
        "st_ctrl_amb_vet_tratamnt_focal": "INTEGER", # BOOLEAN
        "co_dim_desfecho_visita": "INTEGER",
        "co_dim_tipo_origem_dado_transp": "INTEGER",
        "co_dim_cds_tipo_origem": "INTEGER",
        "co_fat_cidadao_pec": "INTEGER",
        "co_dim_tipo_glicemia": "INTEGER",
        "nu_medicao_pressao_arterial": "TEXT",
        "nu_medicao_temperatura": "DECIMAL",
        "nu_medicao_glicemia": "INTEGER",
        "nu_latitude": "DECIMAL",
        "nu_longitude": "DECIMAL",
        "com_localizacao": "INTEGER",
        "sem_localizacao": "INTEGER",
        "registro_cds": "INTEGER",
        "registro_app_territorio": "INTEGER",
        "ds_dia_semana": "TEXT",
        "nu_dia": "INTEGER",
        "nu_mes": "INTEGER",
        "nu_ano": "INTEGER",
        "sab_dom": "INTEGER",
        "ds_turno": "TEXT",
        "ds_sexo": "TEXT",
        "sg_sexo": "CHAR(1)",
        "no_equipe": "TEXT",
        "nu_ine": "VARCHAR(10)",
        "nu_cnes": "VARCHAR(7)",
        "no_unidade_saude": "TEXT",
        "no_profissional": "TEXT",
        "co_seq_dim_profissional": "INTEGER",
        "st_visita_compartilhada_mcaf": "VARCHAR(3)",
        "CONTAGEM_ALTURA": "INTEGER",
        "CONTAGEM_PESO": "INTEGER",
        "CONTAGEM_TEMPERATURA": "INTEGER",
        "CONTAGEM_PRESSÃO_ARTERIAL": "INTEGER",
        "CONTAGEM_GLICEMIA": "INTEGER",
        "SEM_NU_CNS_MCAF": "INTEGER",
        "SEM_NU_CPF_MCAF": "INTEGER",
        "CBO_MCAF": "VARCHAR(3)",
        "soma_recem_nascido_crianca": "VARCHAR(1)",
        "DT_VISITA_MCAF": "TIMESTAMP",
        "hr_min_seg": "TEXT", # TIME
        "anotacao": "VARCHAR(3)",
        "DT_VISITA_MAP": "VARCHAR(10)",
        "DT_NASCIMENTO_MCAF": "VARCHAR(10)",
        "IDADE_ATUAL": "INTEGER",
        "IDADE_NA_VISITA": "INTEGER",
        "origem_visita": "TEXT",
        "ds_tipo_imovel": "TEXT"
    },
    "tb_atendimentos": {
        "co_seq_fat_atd_ind": "INTEGER PRIMARY KEY",
        "co_dim_tempo_atend": "INTEGER",
        "dt_atendimento": "TIMESTAMP", # DATE or TIMESTAMP
        "co_dim_unidade_saude_atend": "INTEGER",
        "no_unidade_saude_atend": "TEXT",
        "nu_cnes_atend": "VARCHAR(7)",
        "co_dim_equipe_atend": "INTEGER",
        "no_equipe_atend": "TEXT",
        "nu_ine_atend": "VARCHAR(10)",
        "co_dim_profissional_atend": "INTEGER",
        "no_profissional_atend": "TEXT",
        "nu_cns_profissional_atend": "VARCHAR(15)",
        "co_cbo_profissional_atend": "VARCHAR(6)",
        "no_cbo_profissional_atend": "TEXT",
        "co_dim_local_atendimento": "INTEGER",
        "ds_local_atendimento": "TEXT",
        "nu_peso": "DECIMAL",
        "nu_altura": "DECIMAL",
        "ds_filtro_ciaps": "TEXT",
        "ds_filtro_cids": "TEXT",
        "co_seq_fat_cidadao_pec": "INTEGER",
        "no_cidadao": "TEXT",
        "nu_cns_cidadao": "VARCHAR(15)",
        "nu_cpf_cidadao": "VARCHAR(11)",
        "co_dim_tempo_nascimento": "INTEGER",
        "dt_nascimento": "DATE",
        "co_dim_sexo": "INTEGER",
        "ds_sexo": "TEXT",
        "co_dim_raca_cor": "INTEGER",
        "ds_raca_cor": "TEXT",
        "co_dim_etnia": "INTEGER",
        "no_etnia": "TEXT",
        "co_dim_pais_nascimento": "INTEGER",
        "no_pais": "TEXT",
        "st_faleceu": "INTEGER", # BOOLEAN
        "co_seq_cidadao": "INTEGER",
        "st_ativo_cidadao": "INTEGER", # BOOLEAN
        "dt_atualizado_cidadao": "TIMESTAMP",
        "nu_micro_area_cidadao": "VARCHAR(2)",
        "co_seq_fat_cad_individual": "INTEGER",
        "co_dim_tempo_cad_ind": "INTEGER",
        "dt_cadastro_individual": "DATE",
        "co_dim_unidade_saude_cad_ind": "INTEGER",
        "no_unidade_saude_cad_ind": "TEXT",
        "co_dim_equipe_cad_ind": "INTEGER",
        "no_equipe_cad_ind": "TEXT",
        "co_dim_profissional_cad_ind": "INTEGER",
        "no_profissional_cad_ind": "TEXT",
        "nu_micro_area_cad_ind": "VARCHAR(2)",
        "st_responsavel_familiar": "INTEGER", "st_frequenta_creche": "INTEGER",
        "st_gestante": "INTEGER", "st_fumante": "INTEGER", "st_alcool": "INTEGER",
        "st_outra_droga": "INTEGER", "st_hipertensao_arterial": "INTEGER",
        "st_diabete": "INTEGER", "st_avc": "INTEGER", "st_infarto": "INTEGER",
        "st_doenca_cardiaca": "INTEGER", "st_problema_rins": "INTEGER",
        "st_doenca_respiratoria": "INTEGER", "st_hanseniase": "INTEGER",
        "st_tuberculose": "INTEGER", "st_cancer": "INTEGER",
        "st_internacao_12": "INTEGER", "st_tratamento_psiquiatra": "INTEGER",
        "st_acamado": "INTEGER", "st_domiciliado": "INTEGER", "st_deficiencia": "INTEGER",
        "ds_dim_tipo_escolaridade": "TEXT",
        "co_dim_situacao_trabalho": "INTEGER",
        "ds_dim_situacao_trabalho": "TEXT",
        "co_seq_fat_cad_domiciliar": "INTEGER",
        "co_dim_tempo_cad_dom": "INTEGER",
        "dt_cadastro_domiciliar": "DATE",
        "co_dim_unidade_saude_cad_dom": "INTEGER",
        "no_unidade_saude_cad_dom": "TEXT",
        "co_dim_equipe_cad_dom": "INTEGER",
        "no_equipe_cad_dom": "TEXT",
        "co_dim_profissional_cad_dom": "INTEGER",
        "no_profissional_cad_dom": "TEXT",
        "nu_micro_area_cad_dom": "VARCHAR(2)",
        "co_dim_tipo_localizacao": "INTEGER",
        "ds_tipo_localizacao": "TEXT",
        "co_dim_tipo_imovel": "INTEGER",
        "ds_tipo_imovel": "TEXT",
        "nu_comodo": "INTEGER",
        "qt_morador": "INTEGER",
        "co_dim_tipo_abastecimento_agua": "INTEGER",
        "ds_tipo_abastecimento_agua": "TEXT",
        "co_dim_tipo_tratamento_agua": "INTEGER",
        "ds_tipo_tratamento_agua": "TEXT",
        "co_dim_tipo_escoamento_sanitar": "INTEGER",
        "ds_tipo_escoamento_sanitario": "TEXT",
        "co_dim_tipo_destino_lixo": "INTEGER",
        "ds_tipo_destino_lixo": "TEXT",
        "st_disp_energia": "INTEGER" # BOOLEAN
    },
    "tb_iaf": {
        "nu_ano": "INTEGER",
        "nu_mes": "INTEGER",
        "Cnes": "VARCHAR(7)",
        "Nome da Unidade": "TEXT",
        "Profissional": "TEXT",
        "Cbo": "VARCHAR(6)",
        "Total de Participantes": "INTEGER",
        "Total Participantes Registrados": "INTEGER",
        "Total de Atividades": "INTEGER"
    },
    "tb_pse": {
        "Ano": "INTEGER",
        "Mes": "INTEGER",
        "Inep": "VARCHAR(8)",
        "Nome da Escola": "TEXT",
        "Verificação da Situação Vacinal": "INTEGER",
        "Aplicação de Fluor": "INTEGER",
        "Saude Auditiva": "INTEGER",
        "Saude Ocular": "INTEGER",
        "Praticas Corporais Atividade Fisica": "INTEGER",
        "Antropometria": "INTEGER",
        "Saude Mental": "INTEGER",
        "Saude Sexual e Reprodutiva": "INTEGER",
        "Saude Bucal": "INTEGER",
        "Cidadania e Direitos Humanos": "INTEGER",
        "Agravos e Doenças Negligenciadas": "INTEGER",
        "Prevenção da Violencia": "INTEGER",
        "Prevenção ao Uso de Alcool": "INTEGER",
        "Ações de Combate a Dengue": "INTEGER",
        "Saude Ambiental": "INTEGER",
        "Alimentação Saudavel": "INTEGER",
        "Total de Atividades": "INTEGER",
        "Indicador 1": "VARCHAR(3)",
        "Indicador 2": "VARCHAR(3)"
    },
    "tb_pse_prof": {
        "Ano": "INTEGER",
        "Mes": "INTEGER",
        "Inep": "VARCHAR(8)",
        "Nome da Escola": "TEXT",
        "PSE": "VARCHAR(15)",
        "Cns": "VARCHAR(15)",
        "Profissional": "TEXT",
        "Cbo": "VARCHAR(6)",
        "Nome Cbo": "TEXT",
        "Ine": "VARCHAR(10)",
        "Nome da Equipe": "TEXT",
        "Cnes": "VARCHAR(7)",
        "Nome da Unidade": "TEXT",
        "Total de Participantes": "INTEGER",
        "Total de Participantes Registrados": "INTEGER",
        "Total de Atividades": "INTEGER"
    },
    "correios_ceps": {
        "cep": "VARCHAR(8) PRIMARY KEY",
        "logradouro": "TEXT",
        "bairro": "TEXT",
        "cidade": "TEXT",
        "uf": "CHAR(2)",
        "ibge": "VARCHAR(10)",
        "origem": "TEXT"
    }
}

def generate_ddl_for_table(table_name, columns_dict):
    column_definitions = []
    for col_name, col_type in columns_dict.items():
        # Ensure column names are quoted if they contain spaces or are keywords
        quoted_col_name = f'"{col_name}"'
        column_definitions.append(f'    {quoted_col_name} {col_type}')
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{',\n'.join(column_definitions)}\n);"

def create_custom_tables(engine):
    with engine.connect() as connection:
        for table_name, columns_definition in schemas.items():
            ddl_statement = generate_ddl_for_table(table_name, columns_definition)
            try:
                connection.execute(text(ddl_statement))
                # print(f"Table {table_name} checked/created successfully.")
            except Exception as e:
                print(f"Error creating table {table_name}: {e}")
        try:
            connection.commit() # Commit after all tables are processed
        except Exception as e:
            print(f"Error during commit: {e}")

if __name__ == "__main__":
    # Example usage (requires a SQLAlchemy engine)
    # from sqlalchemy import create_engine
    # db_uri = "postgresql+psycopg2://postgres:esus@localhost:5432/esus"
    # engine = create_engine(db_uri)
    # create_custom_tables(engine)
    # print("All custom tables checked/created based on schemas.")
    pass
