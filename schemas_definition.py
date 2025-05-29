schemas = {
    "tb_bpa": {
        "PRD_IDENT": "VARCHAR(2)",  # From Gerar_BPA.py (hardcoded '02' or '03')
        "PRD_UID": "VARCHAR(7)",    # CNES
        "PRD_CMP": "VARCHAR(6)",    # Competência (YYYYMM)
        "PRD_CNSMED": "VARCHAR(15)", # CNS do profissional
        "PRD_CBO": "VARCHAR(6)",    # CBO do profissional
        "PRD_DTATEN": "VARCHAR(8)",  # Data do atendimento (YYYYMMDD)
        "PRD_FLH": "VARCHAR(3)",    # Folha
        "PRD_SEQ": "VARCHAR(2)",    # Sequência na folha
        "PRD_PA": "VARCHAR(10)",    # Procedimento Ambulatorial
        "PRD_CNSPAC": "VARCHAR(15)", # CNS do paciente
        "PRD_SEXO": "CHAR(1)",      # Sexo do paciente (M/F)
        "PRD_IBGE": "VARCHAR(6)",   # Código IBGE do município de residência do paciente
        "PRD_CID": "VARCHAR(4)",    # Código CID
        "PRD_IDADE": "VARCHAR(3)",  # Idade do paciente
        "PRD_QT_P": "VARCHAR(6)",   # Quantidade de procedimentos
        "PRD_CATEN": "VARCHAR(2)",  # Caráter do atendimento
        "PRD_NAUT": "VARCHAR(13)",  # Número da autorização
        "PRD_ORG": "VARCHAR(3)",    # Origem (BPA/BPI)
        "PRD_NMPAC": "VARCHAR(30)", # Nome do paciente
        "PRD_DTNASC": "VARCHAR(8)", # Data de nascimento do paciente (YYYYMMDD)
        "PRD_RACA": "VARCHAR(2)",   # Raça/cor do paciente
        "PRD_ETNIA": "VARCHAR(4)",  # Etnia do paciente
        "PRD_NAC": "VARCHAR(3)",    # Nacionalidade
        "PRD_SERVICO": "VARCHAR(3)",
        "PRD_CLASSIFICACAO": "VARCHAR(3)",
        "PRD_EQP_SEQ": "VARCHAR(8)", # Sequencial da equipe (provavelmente INE)
        "PRD_EQP_AREA": "VARCHAR(4)",# Área da equipe
        "PRD_CNPJ": "VARCHAR(14)",   # CNPJ do estabelecimento
        "PRD_CEP_PCNTE": "VARCHAR(8)", # CEP do paciente
        "PRD_LOGRAD_PCNTE": "VARCHAR(3)", # Tipo de logradouro do paciente (código DNE)
        "PRD_END_PCNTE": "VARCHAR(30)",  # Endereço do paciente
        "PRD_COMPL_PCNTE": "VARCHAR(10)",# Complemento do endereço
        "PRD_NUM_PCNTE": "VARCHAR(5)",   # Número do endereço
        "PRD_BAIRRO_PCNTE": "VARCHAR(30)",# Bairro do paciente
        "PRD_DDTEL_PCNTE": "VARCHAR(2)", # DDD do telefone
        "PRD_TEL_PCNTE": "VARCHAR(9)",   # Telefone do paciente
        "PRD_EMAIL_PCNTE": "VARCHAR(40)",# Email do paciente
        "PRD_INE": "VARCHAR(10)",       # INE da equipe (quando aplicável)
        "PRD_CPF_PCNTE": "VARCHAR(11)", # CPF do paciente (renamed from PRD_CPF_PCNT in Gerar_BPA)
        "PRD_MVM": "VARCHAR(6)",        # Movimento (YYYYMM) - Visto em Consultas.py
        # Flags de erro/validação (vistos em Consultas.py, mas podem não ser colunas diretas na tabela final)
        # "PRD_FLPA": "VARCHAR(1)",
        # "PRD_FLCBO": "VARCHAR(1)",
        # "PRD_FLCA": "VARCHAR(1)",
        # "PRD_FLIDA": "VARCHAR(1)",
        # "PRD_FLQT": "VARCHAR(1)",
        # "PRD_FLER": "VARCHAR(1)",
        # "PRD_FLMUN": "VARCHAR(1)",
        # "PRD_FLCID": "VARCHAR(1)",
        # "PRD_ADVQT": "VARCHAR(2)", # Visto em Consultas.py
    },
    "tb_visitas": {
        "nu_cpf_cidadao": "TEXT", # Ou VARCHAR, armazenando 'X' ou NULL
        "nu_cns": "TEXT", # Ou VARCHAR, armazenando 'X' ou NULL
        "co_dim_tipo_ficha": "INTEGER", # FK
        "co_dim_municipio": "INTEGER", # FK
        "co_dim_profissional": "INTEGER", # FK
        "co_dim_tipo_imovel": "INTEGER", # FK
        "nu_micro_area": "VARCHAR(2)", # Ou TEXT
        "nu_peso": "DECIMAL", # Assumindo decimal para peso
        "nu_altura": "DECIMAL", # Assumindo decimal para altura
        "dt_nascimento": "DATE", # Ou TIMESTAMP
        "co_dim_faixa_etaria": "INTEGER", # FK
        "st_visita_compartilhada": "BOOLEAN", # Ou INTEGER (0/1)
        "st_mot_vis_cad_att": "BOOLEAN",
        "st_mot_vis_visita_periodica": "BOOLEAN",
        "st_mot_vis_busca_ativa": "BOOLEAN",
        "st_mot_vis_acompanhamento": "BOOLEAN",
        "st_mot_vis_egresso_internacao": "BOOLEAN",
        "st_mot_vis_ctrl_ambnte_vetor": "BOOLEAN",
        "st_mot_vis_convte_atvidd_cltva": "BOOLEAN",
        "st_mot_vis_orintacao_prevncao": "BOOLEAN",
        "st_mot_vis_outros": "BOOLEAN",
        "st_busca_ativa_consulta": "BOOLEAN",
        "st_busca_ativa_exame": "BOOLEAN",
        "st_busca_ativa_vacina": "BOOLEAN",
        "st_busca_ativa_bolsa_familia": "BOOLEAN",
        "st_acomp_gestante": "BOOLEAN",
        "st_acomp_puerpera": "BOOLEAN",
        "st_acomp_recem_nascido": "BOOLEAN",
        "st_acomp_crianca": "BOOLEAN",
        "st_acomp_pessoa_desnutricao": "BOOLEAN",
        "st_acomp_pessoa_reabil_deficie": "BOOLEAN",
        "st_acomp_pessoa_hipertensao": "BOOLEAN",
        "st_acomp_pessoa_diabetes": "BOOLEAN",
        "st_acomp_pessoa_asma": "BOOLEAN",
        "st_acomp_pessoa_dpoc_enfisema": "BOOLEAN",
        "st_acomp_pessoa_cancer": "BOOLEAN",
        "st_acomp_pessoa_doenca_cronica": "BOOLEAN",
        "st_acomp_pessoa_hanseniase": "BOOLEAN",
        "st_acomp_pessoa_tuberculose": "BOOLEAN",
        "st_acomp_sintomaticos_respirat": "BOOLEAN",
        "st_acomp_tabagista": "BOOLEAN",
        "st_acomp_domiciliados_acamados": "BOOLEAN",
        "st_acomp_condi_vulnerab_social": "BOOLEAN",
        "st_acomp_condi_bolsa_familia": "BOOLEAN",
        "st_acomp_saude_mental": "BOOLEAN",
        "st_acomp_usuario_alcool": "BOOLEAN",
        "st_acomp_usuario_outras_drogra": "BOOLEAN",
        "st_ctrl_amb_vet_acao_educativa": "BOOLEAN",
        "st_ctrl_amb_vet_imovel_foco": "BOOLEAN",
        "st_ctrl_amb_vet_acao_mecanica": "BOOLEAN",
        "st_ctrl_amb_vet_tratamnt_focal": "BOOLEAN",
        "co_dim_desfecho_visita": "INTEGER", # FK
        "co_dim_tipo_origem_dado_transp": "INTEGER", # FK
        "co_dim_cds_tipo_origem": "INTEGER", # FK
        "co_fat_cidadao_pec": "INTEGER", # FK
        "co_dim_tipo_glicemia": "INTEGER", # FK
        "nu_medicao_pressao_arterial": "TEXT", # Pode ser algo como "120/80"
        "nu_medicao_temperatura": "DECIMAL",
        "nu_medicao_glicemia": "INTEGER", # Ou DECIMAL
        "nu_latitude": "DECIMAL", # Ou FLOAT
        "nu_longitude": "DECIMAL", # Ou FLOAT
        "com_localizacao": "INTEGER", # 0 ou 1
        "sem_localizacao": "INTEGER", # 0 ou 1
        "registro_cds": "INTEGER", # 0 ou 1
        "registro_app_territorio": "INTEGER", # 0 ou 1
        "ds_dia_semana": "TEXT",
        "nu_dia": "INTEGER",
        "nu_mes": "INTEGER",
        "nu_ano": "INTEGER",
        "sab_dom": "INTEGER", # 0 ou 1
        "ds_turno": "TEXT", # FK para tb_dim_turno
        "ds_sexo": "TEXT", # FK para tb_dim_sexo
        "sg_sexo": "CHAR(1)", # FK para tb_dim_sexo
        "no_equipe": "TEXT", # FK para tb_dim_equipe
        "nu_ine": "VARCHAR(10)", # FK para tb_dim_equipe
        "nu_cnes": "VARCHAR(7)", # FK para tb_dim_unidade_saude
        "no_unidade_saude": "TEXT", # FK para tb_dim_unidade_saude
        "no_profissional": "TEXT", # FK para tb_dim_profissional (via tb_prof)
        "co_seq_dim_profissional": "INTEGER", # FK para tb_dim_profissional (direto)
        "st_visita_compartilhada_mcaf": "VARCHAR(3)", # SIM/NÃO
        "CONTAGEM_ALTURA": "INTEGER", # 0 ou 1
        "CONTAGEM_PESO": "INTEGER", # 0 ou 1
        "CONTAGEM_TEMPERATURA": "INTEGER", # 0 ou 1
        "CONTAGEM_PRESSÃO_ARTERIAL": "INTEGER", # 0 ou 1
        "CONTAGEM_GLICEMIA": "INTEGER", # 0 ou 1
        "SEM_NU_CNS_MCAF": "INTEGER", # 0 ou 1
        "SEM_NU_CPF_MCAF": "INTEGER", # 0 ou 1
        "CBO_MCAF": "VARCHAR(3)", # ACS/ACE
        "soma_recem_nascido_crianca": "VARCHAR(1)", # 0 ou 1
        "DT_VISITA_MCAF": "TIMESTAMP", # Ou DATE
        "hr_min_seg": "TIME",
        "anotacao": "VARCHAR(3)", # SIM/NÃO
        "DT_VISITA_MAP": "VARCHAR(10)", # dd/mm/yyyy
        "DT_NASCIMENTO_MCAF": "VARCHAR(10)", # dd/mm/yyyy
        "IDADE_ATUAL": "INTEGER",
        "IDADE_NA_VISITA": "INTEGER",
        "origem_visita": "TEXT",
        "ds_tipo_imovel": "TEXT" # FK para tb_dim_tipo_imovel
    },
    "tb_atendimentos": {
        "co_seq_fat_atd_ind": "INTEGER PRIMARY KEY", # Chave primária da tb_fat_atendimento_individual
        "co_dim_tempo_atend": "INTEGER", # FK tb_dim_tempo
        "dt_atendimento": "DATE", # Ou TIMESTAMP
        "co_dim_unidade_saude_atend": "INTEGER", # FK tb_dim_unidade_saude
        "no_unidade_saude_atend": "TEXT",
        "nu_cnes_atend": "VARCHAR(7)",
        "co_dim_equipe_atend": "INTEGER", # FK tb_dim_equipe
        "no_equipe_atend": "TEXT",
        "nu_ine_atend": "VARCHAR(10)",
        "co_dim_profissional_atend": "INTEGER", # FK tb_dim_profissional
        "no_profissional_atend": "TEXT",
        "nu_cns_profissional_atend": "VARCHAR(15)",
        "co_cbo_profissional_atend": "VARCHAR(6)", # FK tb_dim_cbo
        "no_cbo_profissional_atend": "TEXT",
        "co_dim_local_atendimento": "INTEGER", # FK tb_dim_local_atendimento
        "ds_local_atendimento": "TEXT",
        "nu_peso": "DECIMAL",
        "nu_altura": "DECIMAL",
        "ds_filtro_ciaps": "TEXT", # Códigos CIAP concatenados
        "ds_filtro_cids": "TEXT",  # Códigos CID concatenados
        "co_seq_fat_cidadao_pec": "INTEGER", # FK tb_fat_cidadao_pec
        "no_cidadao": "TEXT",
        "nu_cns_cidadao": "VARCHAR(15)",
        "nu_cpf_cidadao": "VARCHAR(11)",
        "co_dim_tempo_nascimento": "INTEGER", # FK tb_dim_tempo
        "dt_nascimento": "DATE", # Ou TIMESTAMP
        "co_dim_sexo": "INTEGER", # FK tb_dim_sexo
        "ds_sexo": "TEXT",
        "co_dim_raca_cor": "INTEGER", # FK tb_dim_raca_cor (via fci)
        "ds_raca_cor": "TEXT",
        "co_dim_etnia": "INTEGER", # FK tb_dim_etnia (via fci)
        "no_etnia": "TEXT",
        "co_dim_pais_nascimento": "INTEGER", # FK tb_dim_pais (via fci)
        "no_pais": "TEXT",
        "st_faleceu": "BOOLEAN", # Ou INTEGER
        "co_seq_cidadao": "INTEGER", # FK tb_cidadao
        "st_ativo_cidadao": "BOOLEAN", # Ou INTEGER
        "dt_atualizado_cidadao": "TIMESTAMP",
        "nu_micro_area_cidadao": "VARCHAR(2)",
        "co_seq_fat_cad_individual": "INTEGER", # FK tb_fat_cad_individual
        "co_dim_tempo_cad_ind": "INTEGER", # FK tb_dim_tempo
        "dt_cadastro_individual": "DATE", # Ou TIMESTAMP
        "co_dim_unidade_saude_cad_ind": "INTEGER", # FK tb_dim_unidade_saude
        "no_unidade_saude_cad_ind": "TEXT",
        "co_dim_equipe_cad_ind": "INTEGER", # FK tb_dim_equipe
        "no_equipe_cad_ind": "TEXT",
        "co_dim_profissional_cad_ind": "INTEGER", # FK tb_dim_profissional
        "no_profissional_cad_ind": "TEXT",
        "nu_micro_area_cad_ind": "VARCHAR(2)",
        "st_responsavel_familiar": "BOOLEAN", "st_frequenta_creche": "BOOLEAN",
        "st_gestante": "BOOLEAN", "st_fumante": "BOOLEAN", "st_alcool": "BOOLEAN",
        "st_outra_droga": "BOOLEAN", "st_hipertensao_arterial": "BOOLEAN",
        "st_diabete": "BOOLEAN", "st_avc": "BOOLEAN", "st_infarto": "BOOLEAN",
        "st_doenca_cardiaca": "BOOLEAN", "st_problema_rins": "BOOLEAN",
        "st_doenca_respiratoria": "BOOLEAN", "st_hanseniase": "BOOLEAN",
        "st_tuberculose": "BOOLEAN", "st_cancer": "BOOLEAN",
        "st_internacao_12": "BOOLEAN", "st_tratamento_psiquiatra": "BOOLEAN",
        "st_acamado": "BOOLEAN", "st_domiciliado": "BOOLEAN", "st_deficiencia": "BOOLEAN",
        "ds_dim_tipo_escolaridade": "TEXT", # FK tb_dim_tipo_escolaridade
        "co_dim_situacao_trabalho": "INTEGER", # FK tb_dim_situacao_trabalho
        "ds_dim_situacao_trabalho": "TEXT",
        "co_seq_fat_cad_domiciliar": "INTEGER", # FK tb_fat_cad_domiciliar
        "co_dim_tempo_cad_dom": "INTEGER", # FK tb_dim_tempo
        "dt_cadastro_domiciliar": "DATE", # Ou TIMESTAMP
        "co_dim_unidade_saude_cad_dom": "INTEGER", # FK tb_dim_unidade_saude
        "no_unidade_saude_cad_dom": "TEXT",
        "co_dim_equipe_cad_dom": "INTEGER", # FK tb_dim_equipe
        "no_equipe_cad_dom": "TEXT",
        "co_dim_profissional_cad_dom": "INTEGER", # FK tb_dim_profissional
        "no_profissional_cad_dom": "TEXT",
        "nu_micro_area_cad_dom": "VARCHAR(2)",
        "co_dim_tipo_localizacao": "INTEGER", # FK tb_dim_tipo_localizacao
        "ds_tipo_localizacao": "TEXT",
        "co_dim_tipo_imovel": "INTEGER", # FK tb_dim_tipo_imovel
        "ds_tipo_imovel": "TEXT",
        "nu_comodo": "INTEGER",
        "qt_morador": "INTEGER",
        "co_dim_tipo_abastecimento_agua": "INTEGER", # FK
        "ds_tipo_abastecimento_agua": "TEXT",
        "co_dim_tipo_tratamento_agua": "INTEGER", # FK
        "ds_tipo_tratamento_agua": "TEXT",
        "co_dim_tipo_escoamento_sanitar": "INTEGER", # FK
        "ds_tipo_escoamento_sanitario": "TEXT",
        "co_dim_tipo_destino_lixo": "INTEGER", # FK
        "ds_tipo_destino_lixo": "TEXT",
        "st_disp_energia": "BOOLEAN" # Ou INTEGER
    },
    "tb_iaf": {
        "nu_ano": "INTEGER",
        "nu_mes": "INTEGER",
        "Cnes": "VARCHAR(7)",
        "Nome da Unidade": "TEXT",
        "Profissional": "TEXT",
        "Cbo": "VARCHAR(6)",
        "Total de Participantes": "INTEGER", # Ou DECIMAL se puder ser fracionado
        "Total Participantes Registrados": "INTEGER", # Ou DECIMAL
        "Total de Atividades": "INTEGER"
    },
    "tb_pse": {
        "Ano": "INTEGER",
        "Mes": "INTEGER",
        "Inep": "VARCHAR(8)", # Código INEP da escola
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
        "Indicador 1": "VARCHAR(3)", # SIM/NÃO
        "Indicador 2": "VARCHAR(3)"  # SIM/NÃO
    },
    "tb_pse_prof": {
        "Ano": "INTEGER",
        "Mes": "INTEGER",
        "Inep": "VARCHAR(8)",
        "Nome da Escola": "TEXT",
        "PSE": "VARCHAR(15)", # PSE EDUCAÇÃO / PSE SAÚDE
        "Cns": "VARCHAR(15)", # CNS do profissional
        "Profissional": "TEXT", # Nome do profissional
        "Cbo": "VARCHAR(6)", # Código CBO
        "Nome Cbo": "TEXT", # Nome do CBO
        "Ine": "VARCHAR(10)", # INE da equipe
        "Nome da Equipe": "TEXT",
        "Cnes": "VARCHAR(7)", # CNES da unidade
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
        "ibge": "VARCHAR(10)", # No script é VARCHAR(10), mas o uso em Gerar_BPA sugere 6 ou 7 dígitos.
        "origem": "TEXT"
    }
}

# Para gerar DDL (opcional, mas pode ser útil)
def generate_ddl(schemas_dict):
    ddl_statements = []
    for table_name, columns in schemas_dict.items():
        column_definitions = []
        for col_name, col_type in columns.items():
            column_definitions.append(f'    "{col_name}" {col_type}')
        ddl_statements.append(f"CREATE TABLE {table_name} (\n{',\n'.join(column_definitions)}\n);")
    return "\n\n".join(ddl_statements)

if __name__ == "__main__":
    # Printar os schemas em formato Python dict
    import pprint
    pprint.pprint(schemas)

    # Gerar e printar DDL
    # print("\n--- DDL Statements ---")
    # print(generate_ddl(schemas))
    pass
