#backend/criar_banco.py
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
from Conexoes import get_local_engine # Changed from Conexões

def ensure_tables_exist():
    engine = get_local_engine()
    try:
        with engine.connect() as conn:
            conn.execute(text("SET client_encoding TO 'UTF8'"))
        inspector = inspect(engine)
    except UnicodeDecodeError as e:
        print("[ERRO] Falha ao inspecionar tabelas (possível problema de encoding). Pulando verificação.")
        print(str(e))
        return

    required_tables = {
        'tb_cadastro': """
            CREATE TABLE IF NOT EXISTS tb_cadastro (    
                co_cidadao BIGINT PRIMARY KEY,
                no_cidadao TEXT,
                no_unidade_saude TEXT,
                no_profissional TEXT,
                no_equipe TEXT,
                st_responsavel_familiar BIGINT,
                st_morador_rua DOUBLE PRECISION,
                st_frequenta_creche DOUBLE PRECISION,
                st_frequenta_cuidador DOUBLE PRECISION,
                st_participa_grupo_comunitario DOUBLE PRECISION,
                st_plano_saude_privado DOUBLE PRECISION,
                st_deficiencia BIGINT,
                st_defi_auditiva BIGINT,
                st_defi_intelectual_cognitiva BIGINT,
                st_defi_visual BIGINT,
                st_defi_fisica BIGINT,
                st_defi_outra BIGINT,
                st_gestante DOUBLE PRECISION,
                st_doenca_respiratoria DOUBLE PRECISION,
                st_doenca_respira_asma BIGINT,
                st_doenca_respira_dpoc_enfisem BIGINT,
                st_doenca_respira_outra BIGINT,
                st_doenca_respira_n_sabe BIGINT,
                st_fumante DOUBLE PRECISION,
                st_alcool DOUBLE PRECISION,
                st_outra_droga DOUBLE PRECISION,
                st_hipertensao_arterial DOUBLE PRECISION,
                st_diabete DOUBLE PRECISION,
                st_avc DOUBLE PRECISION,
                st_hanseniase DOUBLE PRECISION,
                st_tuberculose DOUBLE PRECISION,
                st_cancer DOUBLE PRECISION,
                st_internacao_12 DOUBLE PRECISION,
                st_tratamento_psiquiatra DOUBLE PRECISION,
                st_acamado DOUBLE PRECISION,
                st_domiciliado DOUBLE PRECISION,
                st_usa_planta_medicinal DOUBLE PRECISION,
                st_doenca_cardiaca DOUBLE PRECISION,
                st_doenca_card_insuficiencia BIGINT,
                st_doenca_card_outro BIGINT,
                st_doenca_card_n_sabe BIGINT,
                st_problema_rins DOUBLE PRECISION,
                st_problema_rins_insuficiencia BIGINT,
                st_problema_rins_outro BIGINT,
                st_problema_rins_nao_sabe BIGINT,
                nu_cpf_cidadao TEXT,
                nu_cns TEXT,
                co_cidadao_2 BIGINT,
                dt_atualizado TIMESTAMP WITHOUT TIME ZONE,
                dt_nascimento DATE,
                co_seq_fat_cad_individual BIGINT,
                co_dim_tipo_saida_cadastro BIGINT,
                nu_micro_area TEXT,
                nu_micro_area_cidadao TEXT,
                st_ativo BIGINT,
                st_ficha_inativa BIGINT
            );
        """,
        'tb_domicilio': """
            CREATE TABLE IF NOT EXISTS tb_domicilio (
                co_seq_cds_cad_domiciliar BIGINT,
                no_unidade_saude TEXT,
                no_profissional TEXT,
                no_equipe TEXT,
                no_logradouro TEXT,
                nu_domicilio TEXT,
                ds_complemento TEXT,
                no_bairro TEXT,
                nu_cep TEXT,
                st_ativo BIGINT
            );
        """,
        'tb_bpa': """
            CREATE TABLE IF NOT EXISTS tb_bpa (
            prd_uid	TEXT,
            prd_cmp	TEXT,
            prd_cnsmed	TEXT,
            prd_cbo	TEXT,
            prd_flh	TEXT,
            prd_seq	TEXT,
            prd_pa	TEXT,
            prd_cnspac	TEXT,
            prd_cpf_pcnte	TEXT,
            prd_nmpac	TEXT,
            prd_dtnasc	TEXT,
            prd_sexo	TEXT,
            prd_ibge	TEXT,
            prd_dtaten	TEXT,
            prd_cid	TEXT,
            prd_idade	TEXT,
            prd_qt_p	BIGINT,
            prd_caten	TEXT,
            prd_naut	TEXT,
            prd_org	TEXT,
            prd_mvm	TEXT,
            prd_flpa	TEXT,
            prd_flcbo	TEXT,
            prd_flca	TEXT,
            prd_flida	TEXT,
            prd_flqt	TEXT,
            prd_fler	TEXT,
            prd_flmun	TEXT,
            prd_flcid	TEXT,
            prd_raca	TEXT,
            prd_servico	TEXT,
            prd_classificacao	TEXT,
            prd_etnia	TEXT,
            prd_nac	TEXT,
            prd_advqt	TEXT,
            prd_cnpj	TEXT,
            prd_eqp_area	TEXT,
            prd_eqp_seq	TEXT,
            prd_lograd_pcnte	TEXT,
            prd_cep_pcnte	TEXT,
            prd_end_pcnte	TEXT,
            prd_compl_pcnte	TEXT,
            prd_num_pcnte	TEXT,
            prd_bairro_pcnte	TEXT,
            prd_ddtel_pcnte	TEXT,
            prd_tel_pcnte	TEXT,
            prd_email_pcnte	TEXT,
            prd_ine	TEXT
            );
        """,
        'tb_visitas': """
            CREATE TABLE IF NOT EXISTS tb_visitas (
            co_seq_fat_visita_domiciliar BIGINT primary key,
            nu_cpf_cidadao	TEXT,
            nu_cns	TEXT,
            co_dim_tipo_ficha	BIGINT,
            co_dim_municipio	BIGINT,
            co_dim_profissional	BIGINT,
            co_dim_tipo_imovel	BIGINT,
            nu_micro_area	TEXT,
            nu_peso	TEXT,
            nu_altura	TEXT,
            dt_nascimento	date,
            co_dim_faixa_etaria	BIGINT,
            st_visita_compartilhada	TEXT,
            st_mot_vis_cad_att	BIGINT,
            st_mot_vis_visita_periodica	BIGINT,
            st_mot_vis_busca_ativa	BIGINT,
            st_mot_vis_acompanhamento	BIGINT,
            st_mot_vis_egresso_internacao	BIGINT,
            st_mot_vis_ctrl_ambnte_vetor	BIGINT,
            st_mot_vis_convte_atvidd_cltva	BIGINT,
            st_mot_vis_orintacao_prevncao	BIGINT,
            st_mot_vis_outros	BIGINT,
            st_busca_ativa_consulta	BIGINT,
            st_busca_ativa_exame	BIGINT,
            st_busca_ativa_vacina	BIGINT,
            st_busca_ativa_bolsa_familia	BIGINT,
            st_acomp_gestante	BIGINT,
            st_acomp_puerpera	BIGINT,
            st_acomp_recem_nascido	BIGINT,
            st_acomp_crianca	BIGINT,
            st_acomp_pessoa_desnutricao	BIGINT,
            st_acomp_pessoa_reabil_deficie	BIGINT,
            st_acomp_pessoa_hipertensao	BIGINT,
            st_acomp_pessoa_diabetes	BIGINT,
            st_acomp_pessoa_asma	BIGINT,
            st_acomp_pessoa_dpoc_enfisema	BIGINT,
            st_acomp_pessoa_cancer	BIGINT,
            st_acomp_pessoa_doenca_cronica	BIGINT,
            st_acomp_pessoa_hanseniase	BIGINT,
            st_acomp_pessoa_tuberculose	BIGINT,
            st_acomp_sintomaticos_respirat	BIGINT,
            st_acomp_tabagista	BIGINT,
            st_acomp_domiciliados_acamados	BIGINT,
            st_acomp_condi_vulnerab_social	BIGINT,
            st_acomp_condi_bolsa_familia	BIGINT,
            st_acomp_saude_mental	BIGINT,
            st_acomp_usuario_alcool	BIGINT,
            st_acomp_usuario_outras_drogra	BIGINT,
            st_ctrl_amb_vet_acao_educativa	BIGINT,
            st_ctrl_amb_vet_imovel_foco	BIGINT,
            st_ctrl_amb_vet_acao_mecanica	BIGINT,
            st_ctrl_amb_vet_tratamnt_focal	BIGINT,
            co_dim_desfecho_visita	BIGINT,
            co_dim_tipo_origem_dado_transp	BIGINT,
            co_dim_cds_tipo_origem	BIGINT,
            co_fat_cidadao_pec	BIGINT,
            co_dim_tipo_glicemia	BIGINT,
            nu_medicao_pressao_arterial	TEXT,
            nu_medicao_temperatura	TEXT,
            nu_medicao_glicemia	TEXT,
            nu_latitude	TEXT,
            nu_longitude	TEXT,
            com_localizacao	BIGINT,
            sem_localizacao	BIGINT,
            registro_cds	BIGINT,
            registro_app_territorio	BIGINT,
            ds_dia_semana	TEXT,
            nu_dia	BIGINT,
            nu_mes	BIGINT,
            nu_ano	BIGINT,
            sab_dom	BIGINT,
            ds_turno	TEXT,
            ds_sexo	TEXT,
            sg_sexo	TEXT,
            no_equipe	TEXT,
            nu_ine	TEXT,
            nu_cnes	TEXT,
            no_unidade_saude	TEXT,
            no_profissional	TEXT,
            co_seq_dim_profissional	BIGINT,
            st_visita_compartilhada_mcaf	TEXT,
            contagem_altura	BIGINT,
            contagem_peso	BIGINT,
            contagem_temperatura	BIGINT,
            contagem_pressão_arterial	BIGINT,
            contagem_glicemia	BIGINT,
            sem_nu_cns_mcaf	BIGINT,
            sem_nu_cpf_mcaf	BIGINT,
            cbo_mcaf	TEXT,
            soma_recem_nascido_crianca	TEXT,
            dt_visita_mcaf	date,
            hr_min_seg	TEXT,
            anotacao	TEXT,
            dt_visita_map	TEXT,
            dt_nascimento_mcaf	TEXT,
            idade_atual	double precision,
            idade_na_visita	double precision,
            origem_visita	TEXT,
            ds_tipo_imovel	TEXT
            );
        """,
        'tb_iaf': """
            CREATE TABLE IF NOT EXISTS tb_iaf (
            nu_ano	BIGINT,
            nu_mes	BIGINT,
            cnes	TEXT,
            nome_da_unidade	TEXT,
            profissional	TEXT,
            cbo	TEXT,
            total_de_participantes	double precision,
            total_participantes_registrados	BIGINT,
            total_de_atividades	BIGINT
            );
        """,
        'tb_pse': """
            CREATE TABLE IF NOT EXISTS tb_pse (
            ano	BIGINT,
            mes	BIGINT,
            inep	TEXT,
            nome_da_escola	TEXT,
            verificação_da_situação_vacinal	TEXT,
            aplicação_de_fluor	TEXT,
            saude_auditiva	TEXT,
            saude_ocular	TEXT,
            praticas_corporais_atividade_fisica	TEXT,
            antropometria	TEXT,
            saude_mental	TEXT,
            saude_sexual_e_reprodutiva	TEXT,
            saude_bucal	TEXT,
            cidadania_e_direitos_humanos	TEXT,
            agravos_e_doenças_negligenciadas	TEXT,
            prevenção_da_violencia	TEXT,
            prevenção_ao_uso_de_alcool	TEXT,
            ações_de_combate_a_dengue	TEXT,
            saude_ambiental	TEXT,
            alimentação_saudavel	TEXT,
            total_de_atividades	TEXT,
            indicador_1	TEXT,
            indicador_2	TEXT
            );
        """,
        'tb_pse_prof': """
            CREATE TABLE IF NOT EXISTS tb_pse_prof (
            ano	BIGINT,
            mes	BIGINT,
            inep	TEXT,
            nome_da_escola	TEXT,
            pse	TEXT,
            cns	TEXT,
            profissional	TEXT,
            cbo	TEXT,
            nome_cbo	TEXT,
            ine	TEXT,
            nome_da_equipe	TEXT,
            cnes	TEXT,
            nome_da_unidade	TEXT,
            total_de_participantes	BIGINT,
            total_de_participantes_registrados	BIGINT,
            total_de_atividades	BIGINT
            );
        """,
        'tb_atendimentos': """
            CREATE TABLE IF NOT EXISTS tb_atendimentos (
            co_seq_fat_atd_ind	BIGINT,
            co_dim_tempo_atend	BIGINT,
            dt_atendimento	date,
            co_dim_unidade_saude_atend	BIGINT,
            no_unidade_saude_atend	TEXT,
            nu_cnes_atend	TEXT,
            co_dim_equipe_atend	BIGINT,
            no_equipe_atend	TEXT,
            nu_ine_atend	TEXT,
            co_dim_profissional_atend	BIGINT,
            no_profissional_atend	TEXT,
            nu_cns_profissional_atend	TEXT,
            co_cbo_profissional_atend	TEXT,
            no_cbo_profissional_atend	TEXT,
            co_dim_local_atendimento	BIGINT,
            ds_local_atendimento	TEXT,
            nu_peso	double precision,
            nu_altura	double precision,
            ds_filtro_ciaps	TEXT,
            ds_filtro_cids	TEXT,
            co_seq_fat_cidadao_pec	BIGINT,
            no_cidadao	TEXT,
            nu_cns_cidadao	TEXT,
            nu_cpf_cidadao	TEXT,
            co_dim_tempo_nascimento	BIGINT,
            dt_nascimento	date,
            co_dim_sexo	BIGINT,
            ds_sexo	TEXT,
            co_dim_raca_cor	double precision,
            ds_raca_cor	TEXT,
            co_dim_etnia	double precision,
            no_etnia	TEXT,
            co_dim_pais_nascimento	double precision,
            no_pais	TEXT,
            st_faleceu	BIGINT,
            co_seq_cidadao	BIGINT,
            st_ativo_cidadao	BIGINT,
            dt_atualizado_cidadao	timestamp without time zone,
            nu_micro_area_cidadao	TEXT,
            co_seq_fat_cad_individual	double precision,
            co_dim_tempo_cad_ind	double precision,
            dt_cadastro_individual	date,
            co_dim_unidade_saude_cad_ind	double precision,
            no_unidade_saude_cad_ind	TEXT,
            co_dim_equipe_cad_ind	double precision,
            no_equipe_cad_ind	TEXT,
            co_dim_profissional_cad_ind	double precision,
            no_profissional_cad_ind	TEXT,
            nu_micro_area_cad_ind	TEXT,
            st_responsavel_familiar	double precision,
            st_frequenta_creche	double precision,
            st_gestante	double precision,
            st_fumante	double precision,
            st_alcool	double precision,
            st_outra_droga	double precision,
            st_hipertensao_arterial	double precision,
            st_diabete	double precision,
            st_avc	double precision,
            st_infarto	double precision,
            st_doenca_cardiaca	double precision,
            st_problema_rins	double precision,
            st_doenca_respiratoria	double precision,
            st_hanseniase	double precision,
            st_tuberculose	double precision,
            st_cancer	double precision,
            st_internacao_12	double precision,
            st_tratamento_psiquiatra	double precision,
            st_acamado	double precision,
            st_domiciliado	double precision,
            st_deficiencia	double precision,
            ds_dim_tipo_escolaridade	TEXT,
            co_dim_situacao_trabalho	double precision,
            ds_dim_situacao_trabalho	TEXT,
            co_seq_fat_cad_domiciliar	double precision,
            co_dim_tempo_cad_dom	double precision,
            dt_cadastro_domiciliar	date,
            co_dim_unidade_saude_cad_dom	double precision,
            no_unidade_saude_cad_dom	TEXT,
            co_dim_equipe_cad_dom	double precision,
            no_equipe_cad_dom	TEXT,
            co_dim_profissional_cad_dom	double precision,
            no_profissional_cad_dom	TEXT,
            nu_micro_area_cad_dom	TEXT,
            co_dim_tipo_localizacao	double precision,
            ds_tipo_localizacao	TEXT,
            co_dim_tipo_imovel	double precision,
            ds_tipo_imovel	TEXT,
            nu_comodo	double precision,
            qt_morador	double precision,
            co_dim_tipo_abastecimento_agua	double precision,
            ds_tipo_abastecimento_agua	TEXT,
            co_dim_tipo_tratamento_agua	double precision,
            ds_tipo_tratamento_agua	TEXT,
            co_dim_tipo_escoamento_sanitar	double precision,
            ds_tipo_escoamento_sanitario	TEXT,
            co_dim_tipo_destino_lixo	double precision,
            ds_tipo_destino_lixo	TEXT,
            st_disp_energia	double precision
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
    ensure_database_exists('esus', 'postgres', 'esus', host='localhost', port='5432')
    ensure_tables_exist()

    # Finaliza conexões abertas
    engine = get_local_engine()
    engine.dispose()
