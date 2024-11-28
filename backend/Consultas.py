import threading
import pandas as pd
from socketio_config import socketio
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from Conexões import get_external_engine, get_local_engine, log_message
from banco import cancel_query, clean_dataframe
import logging
import traceback
from Common import task_event, update_task_status, update_last_import
from contextlib import contextmanager

progress = {}  # Variável global para rastreamento de progresso
cancel_requests = {}  # Variável global para rastreamento de pedidos de cancelamento
progress_lock = threading.Lock()  # Lock para proteger acesso concorrente ao progresso

# Função que executa a tarefa e atualiza o status
def execute_long_task(config_data, tipo):
    try:
        update_task_status(tipo, "running")
        log_message(f"Iniciando tarefa de importação para {tipo}. Dados de configuração: {config_data}")

        def update_progress(progress_value):
            update_progress_safely(tipo, progress_value)

        execute_and_store_queries(config_data, tipo, update_progress)

        update_last_import(tipo)  # Atualiza a última importação
        update_progress_safely(tipo, 100)  # Certifica que o progresso está a 100%

        update_task_status(tipo, "completed")
        log_message(f"Tarefa de importação para {tipo} concluída com sucesso.")

    except Exception as e:
        error_message = traceback.format_exc()
        log_message(f"Erro na tarefa de importação para {tipo}: {error_message}")
        update_task_status(tipo, "failed")

    finally:
        task_event.set()  # Libera o evento
        log_message(f"Tarefa de importação para {tipo} finalizada.")

def update_progress_safely(tipo, progress_value):
    with progress_lock:
        progress[tipo] = progress_value
        send_progress_update(tipo, progress_value)

def send_progress_update(tipo, progress_value, error=None):
    payload = {'type': tipo, 'progress': progress_value}
    if error:
        payload['error'] = error
    socketio.emit('progress_update', payload, namespace='/', to=None)
    logging.debug(f"Progresso emitido para {tipo}: {progress_value}%")

def get_progress(tipo):
    with progress_lock:
        return progress.get(tipo, 0)

def get_db_type(engine):
    return engine.dialect.name

def execute_query_thread(query_name, query, external_engine, local_engine, step_size, tipo):
    cancel_requests[tipo] = False  # Reseta o estado de cancelamento
    thread = threading.Thread(target=execute_query, args=(query_name, query, external_engine, local_engine, step_size, tipo))
    thread.start()
    return thread

@contextmanager
def session_scope(Session):
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def execute_query(query_name, query, external_engine, local_engine, step_size, tipo, params=None, update_progress=None):
    global progress, cancel_requests
    Session = sessionmaker(bind=local_engine)

    try:
        rows_processed = 0
        chunksize = int(step_size if step_size > 0 else 1000)
        db_type = get_db_type(external_engine)

        with external_engine.connect() as conn:
            conn.execution_options(timeout=600)  # Timeout de 10 minutos
            if cancel_requests.get(tipo, False):
                cancel_query(conn, tipo, db_type)
                return "cancelled"

            log_message(f"Iniciando execução da query para {query_name} no tipo {tipo}.")
            
            count_query = f"SELECT COUNT(*) FROM ({query}) as total_count"
            total_rows = conn.execute(text(count_query), params).scalar()
            log_message(f"Total de linhas para {query_name}: {total_rows}")

            if total_rows is None or total_rows == 0:
                raise ValueError(f"Nenhuma linha encontrada para {query_name} no tipo {tipo}.")

            result = conn.execute(query, params)

            while True:
                if cancel_requests.get(tipo, False):
                    log_message(f"Cancelamento detectado durante a execução de {query_name} para {tipo}.")
                    cancel_query(conn, tipo, db_type)
                    return "cancelled"
                
                rows = result.fetchmany(chunksize)
                if not rows:
                    break

                df = pd.DataFrame(rows, columns=result.keys())
                df = clean_dataframe(df)
                log_message(f"{len(df)} linhas processadas para {query_name} no tipo {tipo}.")

                with session_scope(Session) as session:
                    try:
                        df.to_sql(query_name, con=session.bind, if_exists='replace' if rows_processed == 0 else 'append', index=False)
                    except Exception as e:
                        log_message(f"Erro ao salvar chunk no banco de dados para {query_name}: {str(e)}")
                        raise

                rows_processed += len(rows)

                if total_rows > 0:
                    progress_value = round((rows_processed / total_rows) * 100, 2)
                    update_progress_safely(tipo, progress_value)
                    log_message(f"Progresso atualizado para {query_name}: {progress_value}%")

                if rows_processed >= total_rows:
                    update_progress_safely(tipo, 100)

    except Exception as e:
        log_message(f"Erro ao executar {query_name} para {tipo}: {str(e)}")
        raise
    finally:
        log_message(f"Execução de {query_name} para {tipo} concluída. Total processado: {rows_processed} linhas.")
        cancel_requests[tipo] = False

def execute_and_store_queries(config_data, tipo, update_progress):

    global progress
    external_engine = get_external_engine()
    local_engine = get_local_engine()
    
    ano = config_data.get('ano') if tipo in ['bpa'] else None
    mes = config_data.get('mes') if tipo in ['bpa'] else None
    log_message(f"Parâmetros recebidos: ano={ano}, mes={mes}, tipo={tipo}")
        
    if tipo =='cadastro':
        queries = {
        'tb_cadastro': '''  
        SELECT      
        INITCAP(t1.no_cidadao) AS no_cidadao,
        INITCAP(t4_fci.no_unidade_saude) AS no_unidade_saude,
        INITCAP(t6.no_profissional) AS no_profissional,
        INITCAP(t5_fci.no_equipe) AS no_equipe,
        t3.st_responsavel_familiar,
        t3.st_morador_rua,
        t3.st_frequenta_creche,
        t3.st_frequenta_cuidador,
        t3.st_participa_grupo_comunitario,
        t3.st_plano_saude_privado,
        t3.st_deficiencia,
        t3.st_defi_auditiva,
        t3.st_defi_intelectual_cognitiva,
        t3.st_defi_visual,
        t3.st_defi_fisica,
        t3.st_defi_outra,
        t3.st_gestante,
        t3.st_doenca_respiratoria,
        t3.st_doenca_respira_asma,
        t3.st_doenca_respira_dpoc_enfisem,
        t3.st_doenca_respira_outra,
        t3.st_doenca_respira_n_sabe,
        t3.st_fumante ,
        t3.st_alcool,
        t3.st_outra_droga,
        t3.st_hipertensao_arterial,
        t3.st_diabete,
        t3.st_avc,
        t3.st_hanseniase,
        t3.st_tuberculose,
        t3.st_cancer,
        t3.st_internacao_12,
        t3.st_tratamento_psiquiatra,
        t3.st_acamado,
        t3.st_domiciliado,
        t3.st_usa_planta_medicinal,
        t3.st_doenca_cardiaca,
        t3.st_doenca_card_insuficiencia,
        t3.st_doenca_card_outro,
        t3.st_doenca_card_n_sabe,
        t3.st_problema_rins,
        t3.st_problema_rins_insuficiencia,
        t3.st_problema_rins_outro,
        t3.st_problema_rins_nao_sabe,
        t1.nu_cpf_cidadao,
        t3.nu_cns,
        t1.co_cidadao,
        t2.dt_atualizado,
        t2.dt_nascimento,
        t3.co_seq_fat_cad_individual,
        t3.co_dim_tipo_saida_cadastro,
        t3.nu_micro_area,
        t2.nu_micro_area as "nu_micro_area_cidadao",
        t2.st_ativo,
        t10.st_ficha_inativa
        from tb_fat_cidadao_pec t1
        inner join tb_cidadao t2 on t1.co_cidadao = t2.co_seq_cidadao
        left join tb_fat_cad_individual t3 on t2.co_unico_ultima_ficha  = t3.nu_uuid_ficha
        left join tb_dim_unidade_saude t4_fci on t3.co_dim_unidade_saude = t4_fci.co_seq_dim_unidade_saude
        left join tb_dim_equipe t5_fci on t3.co_dim_equipe = t5_fci.co_seq_dim_equipe
        left join tb_dim_profissional t6 on t3.co_dim_profissional = t6.co_seq_dim_profissional
        left join tb_dim_tipo_saida_cadastro t7 on t3.co_dim_tipo_saida_cadastro = t7.co_seq_dim_tipo_saida_cadastro
        left join tb_dim_unidade_saude t8_cadweb on t8_cadweb.co_seq_dim_unidade_saude = t1.co_dim_unidade_saude_vinc
        left join tb_dim_equipe t9_cadweb on t9_cadweb.co_seq_dim_equipe = t1.co_dim_equipe_vinc
        left join tb_fat_cidadao t10 on t10.co_fat_cad_individual = t3.co_seq_fat_cad_individual
        left join tb_dim_profissional t11 on t11.co_seq_dim_profissional = t3.co_dim_profissional
        left join tb_prof_historico_cns t12 on t12.nu_cns = t11.nu_cns
        left join tb_prof t13 on t13.co_seq_prof = t12.co_prof
        left join tb_dim_tipo_origem_dado_transp t14 on t14.co_seq_dim_tp_orgm_dado_transp = t3.co_dim_tipo_origem_dado_transp
        left join tb_dim_sexo t15 on  t15.co_seq_dim_sexo = t3.co_dim_sexo
        left join tb_dim_raca_cor t16 on t3.co_dim_raca_cor = t16.co_seq_dim_raca_cor
        left join tb_dim_situacao_trabalho t17 on t17.co_seq_dim_situacao_trabalho = t3.co_dim_situacao_trabalho
        left join tb_dim_cbo t18 on t18.co_seq_dim_cbo = t3.co_dim_cbo_cidadao
        left join tb_dim_tipo_escolaridade t19 on t19.co_seq_dim_tipo_escolaridade = t3.co_dim_tipo_escolaridade
        left join tb_dim_tipo_condicao_peso t20 on t20.co_seq_dim_tipo_condicao_peso = t3.co_dim_tipo_condicao_peso
        left join tb_nacionalidade t21 on t21.co_nacionalidade = t3.co_dim_nacionalidade
        left join tb_dim_tipo_orientacao_sexual t22 on t22.co_seq_dim_tipo_orientacao_sxl = t3.co_dim_tipo_orientacao_sexual
        left join tb_dim_povo_comunidad_trad t23 on t23.co_seq_dim_povo_comunidad_trad = t3.co_dim_povo_comunidad_trad
        left join tb_dim_municipio t24 on t24.co_seq_dim_municipio = t3.co_dim_municipio_cidadao
        left join tb_dim_uf t25 on t25.co_seq_dim_uf = t24.co_dim_uf
        left join tb_dim_identidade_genero t26 on t26.co_seq_dim_identidade_genero = t3.co_dim_identidade_genero
        where t2.st_ativo = 1 and t3.co_dim_tipo_saida_cadastro is not null and t3.st_ficha_inativa = 0
        order by 1''',}
    
    elif tipo =='domiciliofcd':
        queries ={'tb_domicilio': '''SELECT 
        INITCAP(t5.no_unidade_saude) as no_unidade_saude,
        INITCAP(t7.no_profissional) as no_profissional,
        INITCAP(t6.no_equipe) as no_equipe,
        INITCAP(t2.no_logradouro) as no_logradouro,
        t2.nu_domicilio,
        INITCAP(t2.ds_complemento) as ds_complemento,
        INITCAP(t2.no_bairro) as no_bairro,
        t2.nu_cep,
        tbe.st_ativo,
        co_seq_cds_cad_domiciliar
        FROM tb_fat_cad_domiciliar t1 
        LEFT JOIN tb_cds_cad_domiciliar t2 ON t2.co_unico_ficha = t1.nu_uuid_ficha 
        LEFT JOIN tb_dim_tipo_imovel t3 ON t3.co_seq_dim_tipo_imovel = t1.co_dim_tipo_imovel 
        LEFT JOIN tb_dim_tempo t4 ON t4.co_seq_dim_tempo = t1.co_dim_tempo 
        LEFT JOIN tb_dim_unidade_saude t5 ON t5.co_seq_dim_unidade_saude = t1.co_dim_unidade_saude 
        LEFT JOIN tb_dim_equipe t6 ON t6.co_seq_dim_equipe = t1.co_dim_equipe
        LEFT JOIN tb_equipe tbe ON tbe.nu_ine = t6.nu_ine
        LEFT JOIN tb_dim_profissional t7 ON t7.co_seq_dim_profissional = t1.co_dim_profissional 
        LEFT JOIN tb_prof_historico_cns t8 ON t8.nu_cns = t7.nu_cns 
        LEFT JOIN tb_prof t11 ON t11.co_seq_prof = t8.co_prof 
        LEFT JOIN tb_dim_tipo_logradouro t9 ON t9.co_seq_dim_tipo_logradouro = t1.co_dim_tipo_logradouro 
        LEFT JOIN tb_dim_tipo_situacao_moradia t12 ON t12.co_seq_dim_tipo_situacao_morad = t1.co_dim_tipo_situacao_moradia 
        LEFT JOIN tb_dim_tipo_localizacao t13 ON t13.co_seq_dim_tipo_localizacao = t1.co_dim_tipo_localizacao 
        LEFT JOIN tb_dim_tipo_posse_terra t14 ON t14.co_seq_dim_tipo_posse_terra = t1.co_dim_tipo_posse_terra 
        LEFT JOIN tb_dim_tipo_domicilio t10 ON t10.co_seq_dim_tipo_domicilio = t1.co_dim_tipo_domicilio
        LEFT JOIN tb_dim_tipo_acesso_domicilio t15 ON t15.co_seq_dim_tipo_acesso_domicil = t1.co_dim_tipo_acesso_domicilio
        LEFT JOIN tb_dim_tipo_material_parede t16 ON t16.co_seq_dim_tipo_material_pared = t1.co_dim_tipo_material_parede
        LEFT JOIN tb_dim_tipo_abastecimento_agua t17 ON t17.co_seq_dim_tipo_abastec_agua = t1.co_dim_tipo_abastecimento_agua 
        LEFT JOIN tb_dim_tipo_tratamento_agua t18 ON t18.co_seq_dim_tipo_tratament_agua = t1.co_dim_tipo_tratamento_agua
        LEFT JOIN tb_dim_tipo_escoamento_sanitar t19 ON t19.co_seq_dim_tipo_escoamento_snt = t1.co_dim_tipo_escoamento_sanitar
        LEFT JOIN tb_dim_tipo_destino_lixo t20 ON t20.co_seq_dim_tipo_destino_lixo = t1.co_dim_tipo_destino_lixo
        LEFT JOIN tb_dim_tipo_origem_dado_transp t21 ON t21.co_seq_dim_tp_orgm_dado_transp = t1.co_dim_tipo_origem_dado_transp
        WHERE t2.nu_micro_area <> 'FA' and t1.co_seq_fat_cad_domiciliar = (
        SELECT MAX(tb_fat_cad_domiciliar.co_seq_fat_cad_domiciliar)
        FROM tb_fat_cad_domiciliar
        WHERE tb_fat_cad_domiciliar.nu_uuid_ficha_origem = t1.nu_uuid_ficha_origem
        GROUP BY co_dim_tempo
        ORDER BY co_dim_tempo DESC LIMIT 1)''',}  
    
    elif tipo == 'bpa':
        queries ={'tb_bpa': '''SELECT DISTINCT  
    tb_unidade_saude.nu_cnes as PRD_UID,
    CAST(EXTRACT(YEAR FROM tb_atend.dt_inicio) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_atend.dt_inicio) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS varchar(6)) AS PRD_CMP,
    tb_prof.nu_cns AS PRD_CNSMED,
    tb_cbo.co_cbo_2002 AS PRD_CBO,
    '001' AS PRD_FLH,
    '1' AS PRD_SEQ,
    CASE
        WHEN tb_proced.co_proced LIKE 'A%' THEN SUBSTRING(tb_proced.co_proced_filtro, 9,10)
        ELSE tb_proced.co_proced
    END AS prd_pa,
    CASE 
        WHEN tb_cidadao.nu_cns IS NOT NULL THEN tb_cidadao.nu_cns 
        ELSE '' 
    END AS PRD_CNSPAC,
    CASE 
        WHEN tb_cidadao.nu_cns IS NULL THEN tb_cidadao.nu_cpf 
        ELSE '' 
    END AS PRD_CPF_PCNTE,
    SUBSTRING(tb_cidadao.no_cidadao, 1,30) AS PRD_NMPAC,
    EXTRACT(YEAR FROM tb_cidadao.dt_nascimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_cidadao.dt_nascimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) || 
        CAST(LPAD(CAST(EXTRACT(DAY FROM tb_cidadao.dt_nascimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_DTNASC,
    CASE
        WHEN tb_cidadao.no_sexo = 'FEMININO' THEN 'F'
        ELSE 'M'
    END AS PRD_SEXO,
    CASE 
        WHEN tb_localidade.co_ibge IS NULL THEN '350390' 
        ELSE SUBSTRING(tb_localidade.co_ibge,1,6) 
    END AS PRD_IBGE,
    EXTRACT(YEAR FROM tb_atend.dt_inicio) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_atend.dt_inicio) AS varchar(2)), 2, '0') AS VARCHAR(2)) || 
        CAST(LPAD(CAST(EXTRACT(DAY FROM tb_atend.dt_inicio) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_DTATEN,
    LEFT(string_agg(cast(TB_CID10.nu_cid10 AS varchar(4)), '  '), 4) AS PRD_CID,
    CAST(LPAD(CAST(EXTRACT(year FROM age(tb_cidadao.dt_nascimento)) AS varchar(3)), 3, '0') AS VARCHAR(3)) AS PRD_IDADE,
    000001 AS PRD_QT_P,
    '01' AS PRD_CATEN,
    '' AS PRD_NAUT,
    'BPI' AS PRD_ORG,
    EXTRACT(YEAR FROM tb_atend.dt_inicio) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_atend.dt_inicio) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_MVM,
    '0' AS PRD_FLPA,
    '0' AS PRD_FLCBO,
    '0' AS PRD_FLCA,
    '0' AS PRD_FLIDA,
    '0' AS PRD_FLQT,
    '0' AS PRD_FLER,
    '0' AS PRD_FLMUN,
    '0' AS PRD_FLCID,
    CASE 
        WHEN tb_cidadao.co_raca_cor = '1' THEN '01'
        WHEN tb_cidadao.co_raca_cor = '2' THEN '02'           
        WHEN tb_cidadao.co_raca_cor = '3' THEN '04'           
        WHEN tb_cidadao.co_raca_cor = '4' THEN '03'           
        WHEN tb_cidadao.co_raca_cor = '5' THEN '05'           
        WHEN tb_cidadao.co_raca_cor = '6' THEN '03'     
        ELSE LPAD(CAST(tb_cidadao.co_raca_cor AS VARCHAR(2)), 2, '0')
    END AS PRD_RACA,
    '' AS PRD_SERVICO,
    '' AS PRD_CLASSIFICACAO,
    '' AS PRD_ETNIA,
    '010' AS PRD_NAC,
    '00' AS PRD_ADVQT,
    '' AS PRD_CNPJ,
    '' AS PRD_EQP_AREA,
    '' AS PRD_EQP_SEQ,
    CASE 
        WHEN tb_tipo_logradouro.nu_dne IS NULL THEN '081' 
        ELSE tb_tipo_logradouro.nu_dne 
    END AS PRD_LOGRAD_PCNTE,
    CASE 
        WHEN tb_cidadao.ds_cep IS NULL THEN '07400970'
        WHEN tb_cidadao.ds_cep IN ('07400000') THEN '07400970'
        ELSE tb_cidadao.ds_cep 
    END AS PRD_CEP_PCNTE,
    CASE
        WHEN tb_cidadao.ds_logradouro IS NULL THEN SUBSTRING('Avenida dos Expedicionarios',1,30)
        ELSE SUBSTRING(tb_cidadao.ds_logradouro,1,30) 
    END AS PRD_END_PCNTE,
    SUBSTRING(tb_cidadao.ds_complemento,1,10) AS PRD_COMPL_PCNTE,
    CASE
        WHEN tb_cidadao.nu_numero IS NULL THEN  SUBSTRING('S/N',1,5) 
        ELSE SUBSTRING(tb_cidadao.nu_numero,1,5) 
    END AS PRD_NUM_PCNTE,
    CASE WHEN tb_cidadao.no_bairro IS NULL THEN 'Centro' 
        ELSE SUBSTRING(tb_cidadao.no_bairro,1,30) 
    END AS PRD_BAIRRO_PCNTE,
    substring(tb_cidadao.nu_telefone_residencial, 1 , 2) AS PRD_DDTEL_PCNTE,
    substring(tb_cidadao.nu_telefone_residencial, 3 , 9) AS PRD_TEL_PCNTE,
    '' AS PRD_EMAIL_PCNTE,
    '' AS PRD_INE
FROM 
    public.tb_atend
LEFT JOIN 
    public.tb_status_atend ON tb_status_atend.co_status_atend = tb_atend.st_atend
LEFT JOIN 
    public.tb_unidade_saude ON tb_unidade_saude.co_seq_unidade_saude = tb_atend.co_unidade_saude
LEFT JOIN 
    public.tb_atend_prof ON tb_atend_prof.co_atend = tb_atend.co_seq_atend
LEFT JOIN 
    public.tb_lotacao ON tb_lotacao.co_ator_papel = tb_atend_prof.co_lotacao
LEFT JOIN 
    public.tb_prof ON tb_prof.co_seq_prof = tb_lotacao.co_prof
LEFT JOIN 
    public.tb_uf ON tb_uf.co_uf = tb_prof.co_uf_emissora_conselho_classe
LEFT JOIN 
    public.tb_conselho_classe ON tb_conselho_classe.co_conselho_classe = tb_prof.co_conselho_classe
LEFT JOIN 
    public.tb_cbo ON tb_cbo.co_cbo = tb_lotacao.co_cbo
LEFT JOIN 
    public.rl_atend_proced ON rl_atend_proced.co_atend_prof = tb_atend_prof.co_seq_atend_prof
INNER JOIN 
    public.tb_proced ON tb_proced.co_seq_proced = Rl_atend_proced.co_proced
LEFT JOIN 
    public.rl_evolucao_avaliacao_ciap_cid ON rl_evolucao_avaliacao_ciap_cid.co_atend_prof = tb_atend_prof.co_seq_atend_prof
LEFT JOIN 
    public.tb_cid10 ON tb_cid10.co_cid10 = rl_evolucao_avaliacao_ciap_cid.co_cid10
LEFT JOIN 
    public.tb_prontuario ON tb_prontuario.co_seq_prontuario = tb_atend.co_prontuario
LEFT JOIN 
    public.tb_cidadao ON tb_cidadao.co_seq_cidadao = tb_prontuario.co_cidadao
LEFT JOIN 
    public.tb_tipo_logradouro ON tb_tipo_logradouro.co_tipo_logradouro = tb_cidadao.tp_logradouro
LEFT JOIN 
    tb_localidade ON tb_localidade.co_localidade = tb_cidadao.co_localidade_endereco
LEFT JOIN 
    tb_uf pes_uf ON pes_uf.co_uf = tb_cidadao.co_uf
LEFT JOIN 
    tb_ciap tc ON rl_evolucao_avaliacao_ciap_cid.co_ciap = tc.co_seq_ciap 
WHERE 
    tb_unidade_saude.nu_cnes NOT IN ('4565703') AND
    EXTRACT(YEAR FROM tb_atend.dt_inicio) = :ano
    AND EXTRACT(MONTH FROM tb_atend.dt_inicio) = :mes
    AND tb_proced.co_proced IS NOT NULL 
    AND tb_proced.co_proced NOT IN ('0101020104','0101030010','0307010155','0301100284',
    '0301100276','0301100241','0301100233','0301100225','0301100217',
    '0301100209','0301100195','0301050147','0301050139','0301040141',
    '0301010277','0301010269','0301010064','0301010030','0214010201',
    'ABPG042','ABPO015', 'ABPG040', 'ABPG039', 'ABPG038', 'ABPG034', 
    'ABEX022', 'ABEX013', 'ABEX012','0301100268', '0301100250', '0101040083', '0101040075')
GROUP BY 
    tb_unidade_saude.nu_cnes, tb_atend.dt_inicio, tb_prof.nu_cns, tb_cbo.co_cbo_2002, 
    tb_proced.co_proced, tb_proced.co_proced_filtro, tb_cidadao.nu_cns, tb_cidadao.no_cidadao,
    tb_cidadao.dt_nascimento, tb_cidadao.no_sexo, tb_localidade.co_ibge,
    tb_cidadao.co_raca_cor, tb_tipo_logradouro.nu_dne, tb_cidadao.ds_cep, tb_cidadao.ds_logradouro,
    tb_cidadao.ds_complemento, tb_cidadao.nu_numero, tb_cidadao.no_bairro, tb_cidadao.nu_telefone_residencial,
    tb_cidadao.nu_cpf
    UNION ALL
SELECT DISTINCT  
    tb_unidade_saude.nu_cnes as PRD_UID,
    CAST(EXTRACT(YEAR FROM tb_atend.dt_inicio) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_atend.dt_inicio) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS varchar(6)) AS PRD_CMP,
    tb_prof.nu_cns AS PRD_CNSMED,
    tb_cbo.co_cbo_2002 AS PRD_CBO,
    '001' AS PRD_FLH,
    '1' AS PRD_SEQ,
    CASE
        WHEN tb_proced.co_proced LIKE 'A%' THEN SUBSTRING(tb_proced.co_proced_filtro, 9,10)
        ELSE tb_proced.co_proced
    END AS prd_pa,
    CASE 
        WHEN tb_cidadao.nu_cns IS NOT NULL THEN tb_cidadao.nu_cns 
        ELSE '' 
    END AS PRD_CNSPAC,
    CASE 
        WHEN tb_cidadao.nu_cns IS NULL THEN tb_cidadao.nu_cpf 
        ELSE '' 
    END AS PRD_CPF_PCNTE,
    SUBSTRING(tb_cidadao.no_cidadao, 1,30) AS PRD_NMPAC,
    EXTRACT(YEAR FROM tb_cidadao.dt_nascimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_cidadao.dt_nascimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) || 
        CAST(LPAD(CAST(EXTRACT(DAY FROM tb_cidadao.dt_nascimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_DTNASC,
    CASE
        WHEN tb_cidadao.no_sexo = 'FEMININO' THEN 'F'
        ELSE 'M'
    END AS PRD_SEXO,
    CASE 
        WHEN tb_localidade.co_ibge IS NULL THEN '350390' 
        ELSE SUBSTRING(tb_localidade.co_ibge,1,6) 
    END AS PRD_IBGE,
    EXTRACT(YEAR FROM tb_atend.dt_inicio) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_atend.dt_inicio) AS varchar(2)), 2, '0') AS VARCHAR(2)) || 
        CAST(LPAD(CAST(EXTRACT(DAY FROM tb_atend.dt_inicio) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_DTATEN,
    LEFT(string_agg(cast(TB_CID10.nu_cid10 AS varchar(4)), '  '), 4) AS PRD_CID,
    CAST(LPAD(CAST(EXTRACT(year FROM age(tb_cidadao.dt_nascimento)) AS varchar(3)), 3, '0') AS VARCHAR(3)) AS PRD_IDADE,
    000001 AS PRD_QT_P,
    '01' AS PRD_CATEN,
    '' AS PRD_NAUT,
    'BPI' AS PRD_ORG,
    EXTRACT(YEAR FROM tb_atend.dt_inicio) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_atend.dt_inicio) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_MVM,
    '0' AS PRD_FLPA,
    '0' AS PRD_FLCBO,
    '0' AS PRD_FLCA,
    '0' AS PRD_FLIDA,
    '0' AS PRD_FLQT,
    '0' AS PRD_FLER,
    '0' AS PRD_FLMUN,
    '0' AS PRD_FLCID,
    CASE 
        WHEN tb_cidadao.co_raca_cor = '1' THEN '01'
        WHEN tb_cidadao.co_raca_cor = '2' THEN '02'           
        WHEN tb_cidadao.co_raca_cor = '3' THEN '04'           
        WHEN tb_cidadao.co_raca_cor = '4' THEN '03'           
        WHEN tb_cidadao.co_raca_cor = '5' THEN '05'           
        WHEN tb_cidadao.co_raca_cor = '6' THEN '03'     
        ELSE LPAD(CAST(tb_cidadao.co_raca_cor AS VARCHAR(2)), 2, '0')
    END AS PRD_RACA,
    '' AS PRD_SERVICO,
    '' AS PRD_CLASSIFICACAO,
    '' AS PRD_ETNIA,
    '010' AS PRD_NAC,
    '00' AS PRD_ADVQT,
    '' AS PRD_CNPJ,
    '' AS PRD_EQP_AREA,
    '' AS PRD_EQP_SEQ,
    CASE 
        WHEN tb_tipo_logradouro.nu_dne IS NULL THEN '081' 
        ELSE tb_tipo_logradouro.nu_dne 
    END AS PRD_LOGRAD_PCNTE,
    CASE 
        WHEN tb_cidadao.ds_cep IS NULL THEN '07400970'
        WHEN tb_cidadao.ds_cep IN ('07400000') THEN '07400970'
        ELSE tb_cidadao.ds_cep 
    END AS PRD_CEP_PCNTE,
    CASE
        WHEN tb_cidadao.ds_logradouro IS NULL THEN SUBSTRING('Avenida dos Expedicionarios',1,30)
        ELSE SUBSTRING(tb_cidadao.ds_logradouro,1,30) 
    END AS PRD_END_PCNTE,
    SUBSTRING(tb_cidadao.ds_complemento,1,10) AS PRD_COMPL_PCNTE,
    CASE
        WHEN tb_cidadao.nu_numero IS NULL THEN  SUBSTRING('S/N',1,5) 
        ELSE SUBSTRING(tb_cidadao.nu_numero,1,5) 
    END AS PRD_NUM_PCNTE,
    CASE WHEN tb_cidadao.no_bairro IS NULL THEN 'Centro' 
        ELSE SUBSTRING(tb_cidadao.no_bairro,1,30) 
    END AS PRD_BAIRRO_PCNTE,
    substring(tb_cidadao.nu_telefone_residencial, 1 , 2) AS PRD_DDTEL_PCNTE,
    substring(tb_cidadao.nu_telefone_residencial, 3 , 9) AS PRD_TEL_PCNTE,
    '' AS PRD_EMAIL_PCNTE,
    '' AS PRD_INE
FROM 
    public.tb_atend
LEFT JOIN 
    public.tb_status_atend ON tb_status_atend.co_status_atend = tb_atend.st_atend
LEFT JOIN 
    public.tb_unidade_saude ON tb_unidade_saude.co_seq_unidade_saude = tb_atend.co_unidade_saude
LEFT JOIN 
    public.tb_atend_prof ON tb_atend_prof.co_atend = tb_atend.co_seq_atend
LEFT JOIN 
    public.tb_lotacao ON tb_lotacao.co_ator_papel = tb_atend_prof.co_lotacao
LEFT JOIN 
    public.tb_prof ON tb_prof.co_seq_prof = tb_lotacao.co_prof
LEFT JOIN 
    public.tb_uf ON tb_uf.co_uf = tb_prof.co_uf_emissora_conselho_classe
LEFT JOIN 
    public.tb_conselho_classe ON tb_conselho_classe.co_conselho_classe = tb_prof.co_conselho_classe
LEFT JOIN 
    public.tb_cbo ON tb_cbo.co_cbo = tb_lotacao.co_cbo
LEFT JOIN 
    rl_evolucao_plano_ciap ON rl_evolucao_plano_ciap.co_atend_prof = tb_atend_prof.co_seq_atend_prof
INNER JOIN 
    tb_proced ON tb_proced.co_seq_proced = rl_evolucao_plano_ciap.co_proced
LEFT JOIN 
    public.rl_evolucao_avaliacao_ciap_cid ON rl_evolucao_avaliacao_ciap_cid.co_atend_prof = tb_atend_prof.co_seq_atend_prof
LEFT JOIN 
    public.tb_cid10 ON tb_cid10.co_cid10 = rl_evolucao_avaliacao_ciap_cid.co_cid10
LEFT JOIN 
    public.tb_prontuario ON tb_prontuario.co_seq_prontuario = tb_atend.co_prontuario
LEFT JOIN 
    public.tb_cidadao ON tb_cidadao.co_seq_cidadao = tb_prontuario.co_cidadao
LEFT JOIN 
    public.tb_tipo_logradouro ON tb_tipo_logradouro.co_tipo_logradouro = tb_cidadao.tp_logradouro
LEFT JOIN 
    tb_localidade ON tb_localidade.co_localidade = tb_cidadao.co_localidade_endereco
LEFT JOIN 
    tb_uf pes_uf ON pes_uf.co_uf = tb_cidadao.co_uf
LEFT JOIN 
    tb_ciap tc ON rl_evolucao_avaliacao_ciap_cid.co_ciap = tc.co_seq_ciap 
WHERE 
    tb_unidade_saude.nu_cnes NOT IN ('4565703') AND
    EXTRACT(YEAR FROM tb_atend.dt_inicio) = :ano
    AND EXTRACT(MONTH FROM tb_atend.dt_inicio) = :mes  
    AND tb_proced.co_proced IS NOT NULL 
    AND tb_proced.co_proced NOT IN ('0101020104','0101030010','0307010155','0301100284',
    '0301100276','0301100241','0301100233','0301100225','0301100217',
    '0301100209','0301100195','0301050147','0301050139','0301040141',
    '0301010277','0301010269','0301010064','0301010030','0214010201',
    'ABPG042','ABPO015', 'ABPG040', 'ABPG039', 'ABPG038', 'ABPG034', 
    'ABEX022', 'ABEX013', 'ABEX012','0301100268', '0301100250', '0101040083', '0101040075')
GROUP BY 
    tb_unidade_saude.nu_cnes, tb_atend.dt_inicio, tb_prof.nu_cns, tb_cbo.co_cbo_2002, 
    tb_proced.co_proced, tb_proced.co_proced_filtro, tb_cidadao.nu_cns, tb_cidadao.no_cidadao,
    tb_cidadao.dt_nascimento, tb_cidadao.no_sexo, tb_localidade.co_ibge,
    tb_cidadao.co_raca_cor, tb_tipo_logradouro.nu_dne, tb_cidadao.ds_cep, tb_cidadao.ds_logradouro,
    tb_cidadao.ds_complemento, tb_cidadao.nu_numero, tb_cidadao.no_bairro, tb_cidadao.nu_telefone_residencial,
    tb_cidadao.nu_cpf
UNION ALL
SELECT DISTINCT  
    tb_dim_unidade_saude.nu_cnes as PRD_UID,
    CAST(EXTRACT(YEAR FROM tb_fat_proced_atend.dt_inicial_atendimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_fat_proced_atend.dt_inicial_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS varchar(6)) AS PRD_CMP,
    tb_dim_profissional.nu_cns AS PRD_CNSMED,
    tb_dim_cbo.nu_cbo AS PRD_CBO,
    '001' AS PRD_FLH,
    '1' AS PRD_SEQ,    
    CASE 
        WHEN tb_dim_procedimento.co_proced IN ('ABEX004') THEN '0211020036'    
        WHEN tb_dim_procedimento.co_proced IN ('ABPG001') THEN '0309050022'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG002') THEN '0101040059'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG003') THEN '0301100047'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG004') THEN '0303080019'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG005') THEN '0401020177'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG006') THEN '0301100063'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG007') THEN '0301100276'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG008') THEN '0401010031'
        WHEN tb_dim_procedimento.co_proced IN ('ABEX004') THEN '0211020036'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG010') THEN '0201020033'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG012') THEN '0401010074'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG013') THEN '0211060100'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG014') THEN '0303090030'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG015') THEN '0404010300'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG016') THEN '0401010112'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG017') THEN '0404010270'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG018') THEN '0301100152'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG019') THEN '0401010066'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG020') THEN '0211060275' 
        WHEN tb_dim_procedimento.co_proced IN ('ABPG021') THEN '0404010342'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG022') THEN '0214010066'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG024') THEN '0214010058'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG025') THEN '0214010090'
        WHEN tb_dim_procedimento.co_proced IN ('ABEX026') THEN '0202010473'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG026') THEN '0214010074'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG027') THEN '0301100217'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG028') THEN '0301100209'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG029') THEN '0301100195'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG030') THEN '0301100101'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG031') THEN '0301100233'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG032') THEN '0301100241'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG033') THEN '0301100039'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG040') THEN '0214010155'
        WHEN tb_dim_procedimento.co_proced IN ('ABPG041') THEN '0301100225'
        ELSE tb_dim_procedimento.co_proced 
    END AS prd_pa,
    CASE 
        WHEN tb_cidadao.nu_cns IS NOT NULL THEN tb_cidadao.nu_cns 
        ELSE '' 
    END AS PRD_CNSPAC,
    CASE 
        WHEN tb_cidadao.nu_cns IS NULL THEN tb_cidadao.nu_cpf 
        ELSE '' 
    END AS PRD_CPF_PCNTE,
    SUBSTRING(tb_cidadao.no_cidadao, 1,30) AS PRD_NMPAC,
    EXTRACT(YEAR FROM tb_cidadao.dt_nascimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_cidadao.dt_nascimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) || 
        CAST(LPAD(CAST(EXTRACT(DAY FROM tb_cidadao.dt_nascimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_DTNASC,
    CASE
        WHEN tb_cidadao.no_sexo = 'FEMININO' THEN 'F'
        ELSE 'M'
    END AS PRD_SEXO,
    CASE 
        WHEN tb_dim_municipio.co_ibge IS NULL THEN '350390' 
        ELSE SUBSTRING(tb_dim_municipio.co_ibge,1,6) 
    END AS PRD_IBGE,
    EXTRACT(YEAR FROM tb_fat_proced_atend.dt_inicial_atendimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_fat_proced_atend.dt_inicial_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) || 
        CAST(LPAD(CAST(EXTRACT(DAY FROM tb_fat_proced_atend.dt_inicial_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_DTATEN,
    '0' AS PRD_CID,
    CAST(LPAD(CAST(EXTRACT(year FROM age(tb_cidadao.dt_nascimento)) AS varchar(3)), 3, '0') AS VARCHAR(3)) AS PRD_IDADE,
    000001 AS PRD_QT_P,
    '01' AS PRD_CATEN,
    '' AS PRD_NAUT,
    'BPI' AS PRD_ORG,
    EXTRACT(YEAR FROM tb_fat_proced_atend.dt_inicial_atendimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM tb_fat_proced_atend.dt_inicial_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_MVM,
    '0' AS PRD_FLPA,
    '0' AS PRD_FLCBO,
    '0' AS PRD_FLCA,
    '0' AS PRD_FLIDA,
    '0' AS PRD_FLQT,
    '0' AS PRD_FLER,
    '0' AS PRD_FLMUN,
    '0' AS PRD_FLCID,
    CASE 
        WHEN tb_cidadao.co_raca_cor = '1' THEN '01'
        WHEN tb_cidadao.co_raca_cor = '2' THEN '02'           
        WHEN tb_cidadao.co_raca_cor = '3' THEN '04'           
        WHEN tb_cidadao.co_raca_cor = '4' THEN '03'           
        WHEN tb_cidadao.co_raca_cor = '5' THEN '05'           
        WHEN tb_cidadao.co_raca_cor = '6' THEN '03'     
        ELSE LPAD(CAST(tb_cidadao.co_raca_cor AS VARCHAR(2)), 2, '0')
    END AS PRD_RACA,
    '' AS PRD_SERVICO,
    '' AS PRD_CLASSIFICACAO,
    '' AS PRD_ETNIA,
    '010' AS PRD_NAC,
    '00' AS PRD_ADVQT,
    '' AS PRD_CNPJ,
    '' AS PRD_EQP_AREA,
    '' AS PRD_EQP_SEQ,
    '081' AS PRD_LOGRAD_PCNTE,
    CASE 
        WHEN tb_cidadao.ds_cep IS NULL THEN '07400970'
        WHEN tb_cidadao.ds_cep IN ('07400000') THEN '07400970'
        ELSE tb_cidadao.ds_cep 
    END AS PRD_CEP_PCNTE,
    CASE
        WHEN tb_cidadao.ds_logradouro IS NULL THEN SUBSTRING('Avenida dos Expedicionarios',1,30)
        ELSE SUBSTRING(tb_cidadao.ds_logradouro,1,30) 
    END AS PRD_END_PCNTE,
    SUBSTRING(tb_cidadao.ds_complemento,1,10) AS PRD_COMPL_PCNTE,
    CASE
        WHEN tb_cidadao.nu_numero IS NULL THEN  SUBSTRING('S/N',1,5) 
        ELSE SUBSTRING(tb_cidadao.nu_numero,1,5) 
    END AS PRD_NUM_PCNTE,
    CASE WHEN tb_cidadao.no_bairro IS NULL THEN 'Centro' 
        ELSE SUBSTRING(tb_cidadao.no_bairro,1,30) 
    END AS PRD_BAIRRO_PCNTE,
    substring(tb_cidadao.nu_telefone_residencial, 1 , 2) AS PRD_DDTEL_PCNTE,
    substring(tb_cidadao.nu_telefone_residencial, 3 , 9) AS PRD_TEL_PCNTE,
    '' AS PRD_EMAIL_PCNTE,
    '' AS PRD_INE     
FROM 
    tb_fat_proced_atend
RIGHT JOIN 
    tb_fat_proced_atend_proced tb_fat_proced_atend_proced ON tb_fat_proced_atend_proced.co_fat_procedimento = tb_fat_proced_atend.co_fat_procedimento 
    AND tb_fat_proced_atend_proced.nu_atendimento = tb_fat_proced_atend.nu_atendimento
LEFT JOIN 
    tb_dim_procedimento p1 ON p1.co_seq_dim_procedimento = tb_fat_proced_atend_proced.co_dim_procedimento
LEFT JOIN 
    tb_dim_procedimento p2 ON p2.co_seq_dim_procedimento = p2.co_seq_dim_proced_ref_ab
INNER JOIN 
    tb_fat_cidadao_pec ON tb_fat_cidadao_pec.co_seq_fat_cidadao_pec = tb_fat_proced_atend.co_fat_cidadao_pec
LEFT JOIN 
    tb_prontuario ON tb_prontuario.co_cidadao = tb_fat_cidadao_pec.co_cidadao
LEFT JOIN 
    tb_cidadao ON tb_cidadao.co_seq_cidadao = tb_prontuario.co_cidadao  
LEFT JOIN 
    tb_fat_cidadao_pec AS Fatcid ON Fatcid.nu_cns = tb_cidadao.nu_cns
LEFT JOIN 
    tb_cidadao_grupo ON tb_cidadao_grupo.co_cidadao = tb_fat_cidadao_pec.co_cidadao  
INNER JOIN 
    tb_dim_procedimento ON tb_dim_procedimento.co_seq_dim_procedimento = tb_fat_proced_atend_proced.co_dim_procedimento
INNER JOIN 
    tb_dim_profissional ON tb_dim_profissional.co_seq_dim_profissional = tb_fat_proced_atend_proced.co_dim_profissional
INNER JOIN 
    tb_dim_tipo_origem ON tb_dim_tipo_origem.co_seq_dim_tipo_origem = tb_fat_proced_atend_proced.co_dim_cds_tipo_origem
INNER JOIN 
    tb_dim_local_atendimento ON tb_dim_local_atendimento.co_seq_dim_local_atendimento = tb_fat_proced_atend_proced.co_dim_local_atendimento
INNER JOIN 
    tb_dim_cbo ON tb_dim_cbo.co_seq_dim_cbo = tb_fat_proced_atend_proced.co_dim_cbo
INNER JOIN 
    tb_dim_unidade_saude ON tb_dim_unidade_saude.co_seq_dim_unidade_saude = tb_fat_proced_atend_proced.co_dim_unidade_saude
INNER JOIN 
    tb_dim_sexo ON tb_dim_sexo.co_seq_dim_sexo = tb_fat_proced_atend_proced.co_dim_sexo
INNER JOIN 
    tb_dim_tipo_ficha ON tb_dim_tipo_ficha.co_seq_dim_tipo_ficha = tb_fat_proced_atend_proced.co_dim_tipo_ficha
INNER JOIN 
    tb_dim_equipe ON tb_dim_equipe.co_seq_dim_equipe = tb_fat_proced_atend_proced.co_dim_equipe
INNER JOIN 
    tb_dim_faixa_etaria ON tb_dim_faixa_etaria.co_seq_dim_faixa_etaria = tb_fat_proced_atend.co_dim_faixa_etaria
INNER JOIN 
    tb_dim_tempo ON tb_dim_tempo.co_seq_dim_tempo = tb_fat_proced_atend_proced.co_dim_tempo
INNER JOIN 
    tb_dim_municipio ON tb_dim_municipio.co_seq_dim_municipio = tb_fat_proced_atend_proced.co_dim_municipio
INNER JOIN 
    tb_dim_turno ON tb_dim_turno.co_seq_dim_turno = tb_fat_proced_atend_proced.co_dim_turno
INNER JOIN 
    tb_dim_tipo_origem_dado_transp ON tb_dim_tipo_origem_dado_transp.co_seq_dim_tp_orgm_dado_transp = tb_fat_proced_atend_proced.co_dim_tipo_origem_dado_transp 
WHERE 
    tb_dim_unidade_saude.nu_cnes NOT IN ('4565703') AND
    EXTRACT(YEAR FROM tb_fat_proced_atend.dt_inicial_atendimento) = :ano
    AND EXTRACT(MONTH FROM tb_fat_proced_atend.dt_inicial_atendimento) = :mes
    AND tb_dim_procedimento.co_proced IS NOT NULL 
    AND tb_dim_procedimento.co_proced NOT IN ('0101020104','0101030010','0307010155','0301100284',
    '0301100276','0301100241','0301100233','0301100225','0301100217',
    '0301100209','0301100195','0301050147','0301050139','0301040141',
    '0301010277','0301010269','0301010064','0301010030','0214010201',
    'ABPG042','ABPO015', 'ABPG040', 'ABPG039', 'ABPG038', 'ABPG034', 
    'ABEX022', 'ABEX013', 'ABEX012','0301100268', '0301100250', '0101040083', '0101040075')
    AND tb_dim_tipo_ficha.nu_identificador = '7'
UNION ALL
SELECT DISTINCT
    unidade.nu_cnes AS PRD_UID,
    CAST(EXTRACT(YEAR FROM atend.dt_final_atendimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM atend.dt_final_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS VARCHAR(6)) AS PRD_CMP,
    Prof.nu_cns AS PRD_CNSMED,
    cbo.nu_cbo AS PRD_CBO,
    '001' AS PRD_FLH,
    '1' AS PRD_SEQ,
    UNNEST(REGEXP_MATCHES(fat_proced_atend.ds_filtro_procedimento, '[^\|]+', 'g')) AS PRD_PA,
    CASE 
        WHEN cid.nu_cns IS NOT NULL THEN cid.nu_cns 
        ELSE '' 
    END AS PRD_CNSPAC,
    CASE 
        WHEN cid.nu_cns IS NULL THEN cid.nu_cpf 
        ELSE '' 
    END AS PRD_CPF_PCNTE,
    SUBSTRING(cid.no_cidadao, 1,30) AS PRD_NMPAC,
    EXTRACT(YEAR FROM cid.dt_nascimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM cid.dt_nascimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) || 
        CAST(LPAD(CAST(EXTRACT(DAY FROM cid.dt_nascimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_DTNASC,
    CASE
        WHEN cid.no_sexo = 'FEMININO' THEN 'F'
        ELSE 'M'
    END AS PRD_SEXO,
    CASE 
        WHEN tb_localidade.co_ibge IS NULL THEN '350390' 
        ELSE SUBSTRING(tb_localidade.co_ibge,1,6) 
    END AS PRD_IBGE,
    EXTRACT(YEAR FROM atend.dt_inicial_atendimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM atend.dt_inicial_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) || 
        CAST(LPAD(CAST(EXTRACT(DAY FROM atend.dt_inicial_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_DTATEN,
    CASE 
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302070036%' THEN 'T951'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302010025%' THEN 'N319'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302070010%' THEN 'T302'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302060057%' THEN 'S141'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302060030%' THEN 'G838'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302060014%' THEN 'I694'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302050027%' THEN 'M255'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302050019%' THEN 'T932'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302040056%' THEN 'I988'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302040030%' THEN 'Q048'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302040021%' THEN 'J998'
        WHEN fat_proced_atend.ds_filtro_procedimento LIKE '%0302060022%' THEN 'I694'       
        ELSE REPLACE(atend.ds_filtro_cids, '|', '')
    END AS PRD_CID,
    CAST(LPAD(CAST(EXTRACT(YEAR FROM age(cid.dt_nascimento)) AS varchar(3)), 3, '0') AS VARCHAR(3)) AS PRD_IDADE,
    000001 AS PRD_QT_P,
    '01' AS PRD_CATEN,
    '' AS PRD_NAUT,
    'BPI' AS PRD_ORG,
    EXTRACT(YEAR FROM atend.dt_inicial_atendimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM atend.dt_inicial_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_MVM,
    '0' AS PRD_FLPA,
    '0' AS PRD_FLCBO,
    '0' AS PRD_FLCA,
    '0' AS PRD_FLIDA,
    '0' AS PRD_FLQT,
    '0' AS PRD_FLER,
    '0' AS PRD_FLMUN,
    '0' AS PRD_FLCID,
    CASE 
        WHEN cid.co_raca_cor = '1' THEN '01'
        WHEN cid.co_raca_cor = '2' THEN '02'           
        WHEN cid.co_raca_cor = '3' THEN '04'           
        WHEN cid.co_raca_cor = '4' THEN '03'           
        WHEN cid.co_raca_cor = '5' THEN '05'           
        WHEN cid.co_raca_cor = '6' THEN '03'     
        ELSE LPAD(CAST(cid.co_raca_cor AS VARCHAR(2)), 2, '0')
    END AS PRD_RACA,
    '' AS PRD_SERVICO,
    '' AS PRD_CLASSIFICACAO,
    '' AS PRD_ETNIA,
    '010' AS PRD_NAC,
    '00' AS PRD_ADVQT,
    '' AS PRD_CNPJ,
    '' AS PRD_EQP_AREA,
    '' AS PRD_EQP_SEQ,
    CASE 
        WHEN tb_tipo_logradouro.nu_dne IS NULL THEN '081' 
        ELSE tb_tipo_logradouro.nu_dne 
    END AS PRD_LOGRAD_PCNTE,
    CASE 
        WHEN cid.ds_cep IS NULL THEN '07400970'
        WHEN cid.ds_cep IN ('07400000') THEN '07400970'
        ELSE cid.ds_cep 
    END AS PRD_CEP_PCNTE,
    CASE
        WHEN cid.ds_logradouro IS NULL THEN SUBSTRING('Avenida dos Expedicionarios',1,30)
        ELSE SUBSTRING(cid.ds_logradouro,1,30) 
    END AS PRD_END_PCNTE,
    SUBSTRING(cid.ds_complemento,1,10) AS PRD_COMPL_PCNTE,
    CASE
        WHEN cid.nu_numero IS NULL THEN  SUBSTRING('S/N',1,5) 
        ELSE SUBSTRING(cid.nu_numero,1,5) 
    END AS PRD_NUM_PCNTE,
    CASE WHEN cid.no_bairro IS NULL THEN 'Centro' 
        ELSE SUBSTRING(cid.no_bairro,1,30) 
    END AS PRD_BAIRRO_PCNTE,
    SUBSTRING(cid.nu_telefone_residencial, 1 , 2) AS PRD_DDTEL_PCNTE,
    SUBSTRING(cid.nu_telefone_residencial, 3 , 9) AS PRD_TEL_PCNTE,
    '' AS PRD_EMAIL_PCNTE,
    '' AS PRD_INE 
FROM 
    tb_fat_atendimento_individual atend
INNER JOIN 
    tb_dim_municipio tb_localidade ON atend.co_dim_municipio = tb_localidade.co_seq_dim_municipio
INNER JOIN 
    tb_dim_uf dimensaouf2_ ON tb_localidade.co_dim_uf = dimensaouf2_.co_seq_dim_uf
INNER JOIN 
    tb_dim_unidade_saude unidade ON atend.co_dim_unidade_saude_1 = unidade.co_seq_dim_unidade_saude
INNER JOIN 
    tb_dim_equipe dimensaoeq4_ ON atend.co_dim_equipe_1 = dimensaoeq4_.co_seq_dim_equipe
INNER JOIN 
    tb_dim_profissional Prof ON atend.co_dim_profissional_1 = Prof.co_seq_dim_profissional
INNER JOIN 
    tb_dim_tipo_atendimento dimtipoate6_ ON atend.co_dim_tipo_atendimento = dimtipoate6_.co_seq_dim_tipo_atendimento
INNER JOIN 
    tb_dim_sexo dimensaose7_ ON atend.co_dim_sexo = dimensaose7_.co_seq_dim_sexo
INNER JOIN 
    tb_dim_tipo_origem dimensaoti8_ ON atend.co_dim_cds_tipo_origem = dimensaoti8_.co_seq_dim_tipo_origem
INNER JOIN 
    tb_dim_turno dimensaotu9_ ON atend.co_dim_turno = dimensaotu9_.co_seq_dim_turno
INNER JOIN 
    tb_dim_cbo cbo ON atend.co_dim_cbo_1 = cbo.co_seq_dim_cbo
INNER JOIN 
    tb_fat_proced_atend fat_proced_atend ON atend.nu_uuid_ficha = fat_proced_atend.nu_uuid_ficha
INNER JOIN    
	tb_fat_cidadao_pec tfcp ON atend.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
INNER JOIN
    tb_cidadao cid ON tfcp.co_cidadao = cid.co_seq_cidadao
JOIN
    public.tb_tipo_logradouro ON tb_tipo_logradouro.co_tipo_logradouro = cid.tp_logradouro
WHERE 
TO_CHAR(TO_DATE(atend.co_dim_tempo::text, 'YYYYMMDD'), 'YYYY') = :ano
AND TO_CHAR(TO_DATE(atend.co_dim_tempo::text, 'YYYYMMDD'), 'MM') = :mes
    AND unidade.nu_cnes = '4565703'
    AND (dimtipoate6_.co_seq_dim_tipo_atendimento <> '5'AND
  fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABPO015%' 
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABPG010%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABPG012%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABPG040%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABPG039%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABPG038%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABPG034%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABEX022%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABEX013%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABEX012%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%0301100268%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%0301100250%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%0101040083%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%0101040075%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%0301010064%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABPG025%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%ABPG024%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%0301010030%'
    AND fat_proced_atend.ds_filtro_procedimento NOT LIKE '%0301010129%')
    UNION ALL 
 SELECT DISTINCT
    unidade.nu_cnes AS PRD_UID,
    CAST(EXTRACT(YEAR FROM atend.dt_final_atendimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM atend.dt_final_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS VARCHAR(6)) AS PRD_CMP,
    Prof.nu_cns AS PRD_CNSMED,
    cbo.nu_cbo AS PRD_CBO,
    '001' AS PRD_FLH,
    '1' AS PRD_SEQ,
    procedimento AS PRD_PA, -- Procedimentos já divididos em linhas
    CASE 
        WHEN cid.nu_cns IS NOT NULL THEN cid.nu_cns 
        ELSE '' 
    END AS PRD_CNSPAC,
    CASE 
        WHEN cid.nu_cns IS NULL THEN cid.nu_cpf 
        ELSE '' 
    END AS PRD_CPF_PCNTE,
    SUBSTRING(cid.no_cidadao, 1, 30) AS PRD_NMPAC,
    EXTRACT(YEAR FROM cid.dt_nascimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM cid.dt_nascimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) || 
        CAST(LPAD(CAST(EXTRACT(DAY FROM cid.dt_nascimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_DTNASC,
    CASE
        WHEN cid.no_sexo = 'FEMININO' THEN 'F'
        ELSE 'M'
    END AS PRD_SEXO,
    CASE 
        WHEN tb_localidade.co_ibge IS NULL THEN '350390' 
        ELSE SUBSTRING(tb_localidade.co_ibge, 1, 6) 
    END AS PRD_IBGE,
    EXTRACT(YEAR FROM atend.dt_inicial_atendimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM atend.dt_inicial_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) || 
        CAST(LPAD(CAST(EXTRACT(DAY FROM atend.dt_inicial_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_DTATEN,
    CASE 
        WHEN procedimento = '0302070036' THEN 'T951'
        WHEN procedimento = '0302010025' THEN 'N319'
        WHEN procedimento = '0302070010' THEN 'T302'
        WHEN procedimento = '0302060057' THEN 'S141'
        WHEN procedimento = '0302060030' THEN 'G838'
        WHEN procedimento = '0302060014' THEN 'I694'
        WHEN procedimento = '0302050027' THEN 'M255'
        WHEN procedimento = '0302050019' THEN 'T932'
        WHEN procedimento = '0302040056' THEN 'I988'
        WHEN procedimento = '0302040030' THEN 'Q048'
        WHEN procedimento = '0302040021' THEN 'J998'
        WHEN procedimento = '0302060022' THEN 'I694'       
        ELSE REPLACE(atend.ds_filtro_cids, '|', '')
    END AS PRD_CID,
    CAST(LPAD(CAST(EXTRACT(YEAR FROM age(cid.dt_nascimento)) AS varchar(3)), 3, '0') AS VARCHAR(3)) AS PRD_IDADE,
    000001 AS PRD_QT_P,
    '01' AS PRD_CATEN,
    '' AS PRD_NAUT,
    'BPI' AS PRD_ORG,
    EXTRACT(YEAR FROM atend.dt_inicial_atendimento) || 
        CAST(LPAD(CAST(EXTRACT(MONTH FROM atend.dt_inicial_atendimento) AS varchar(2)), 2, '0') AS VARCHAR(2)) AS PRD_MVM,
    '0' AS PRD_FLPA,
    '0' AS PRD_FLCBO,
    '0' AS PRD_FLCA,
    '0' AS PRD_FLIDA,
    '0' AS PRD_FLQT,
    '0' AS PRD_FLER,
    '0' AS PRD_FLMUN,
    '0' AS PRD_FLCID,
    CASE 
        WHEN cid.co_raca_cor = '1' THEN '01'
        WHEN cid.co_raca_cor = '2' THEN '02'           
        WHEN cid.co_raca_cor = '3' THEN '04'           
        WHEN cid.co_raca_cor = '4' THEN '03'           
        WHEN cid.co_raca_cor = '5' THEN '05'           
        WHEN cid.co_raca_cor = '6' THEN '03'     
        ELSE LPAD(CAST(cid.co_raca_cor AS VARCHAR(2)), 2, '0')
    END AS PRD_RACA,
    '' AS PRD_SERVICO,
    '' AS PRD_CLASSIFICACAO,
    '' AS PRD_ETNIA,
    '010' AS PRD_NAC,
    '00' AS PRD_ADVQT,
    '' AS PRD_CNPJ,
    '' AS PRD_EQP_AREA,
    '' AS PRD_EQP_SEQ,
    CASE 
        WHEN tb_tipo_logradouro.nu_dne IS NULL THEN '081' 
        ELSE tb_tipo_logradouro.nu_dne 
    END AS PRD_LOGRAD_PCNTE,
    CASE 
        WHEN cid.ds_cep IS NULL THEN '07400970'
        WHEN cid.ds_cep IN ('07400000') THEN '07400970'
        ELSE cid.ds_cep 
    END AS PRD_CEP_PCNTE,
    CASE
        WHEN cid.ds_logradouro IS NULL THEN SUBSTRING('Avenida dos Expedicionarios', 1, 30)
        ELSE SUBSTRING(cid.ds_logradouro, 1, 30) 
    END AS PRD_END_PCNTE,
    SUBSTRING(cid.ds_complemento, 1, 10) AS PRD_COMPL_PCNTE,
    CASE
        WHEN cid.nu_numero IS NULL THEN  SUBSTRING('S/N', 1, 5) 
        ELSE SUBSTRING(cid.nu_numero, 1, 5) 
    END AS PRD_NUM_PCNTE,
    CASE WHEN cid.no_bairro IS NULL THEN 'Centro' 
        ELSE SUBSTRING(cid.no_bairro, 1, 30) 
    END AS PRD_BAIRRO_PCNTE,
    SUBSTRING(cid.nu_telefone_residencial, 1 , 2) AS PRD_DDTEL_PCNTE,
    SUBSTRING(cid.nu_telefone_residencial, 3 , 9) AS PRD_TEL_PCNTE,
    '' AS PRD_EMAIL_PCNTE,
    '' AS PRD_INE 
    FROM 
        tb_fat_atendimento_individual atend
    INNER JOIN 
        tb_dim_municipio tb_localidade ON atend.co_dim_municipio = tb_localidade.co_seq_dim_municipio
    INNER JOIN 
        tb_dim_uf dimensaouf2_ ON tb_localidade.co_dim_uf = dimensaouf2_.co_seq_dim_uf
    INNER JOIN 
        tb_dim_unidade_saude unidade ON atend.co_dim_unidade_saude_1 = unidade.co_seq_dim_unidade_saude
    INNER JOIN 
        tb_dim_equipe dimensaoeq4_ ON atend.co_dim_equipe_1 = dimensaoeq4_.co_seq_dim_equipe
    INNER JOIN 
        tb_dim_profissional Prof ON atend.co_dim_profissional_1 = Prof.co_seq_dim_profissional
    INNER JOIN 
        tb_dim_tipo_atendimento dimtipoate6_ ON atend.co_dim_tipo_atendimento = dimtipoate6_.co_seq_dim_tipo_atendimento
    INNER JOIN 
        tb_dim_sexo dimensaose7_ ON atend.co_dim_sexo = dimensaose7_.co_seq_dim_sexo
    INNER JOIN 
        tb_dim_tipo_origem dimensaoti8_ ON atend.co_dim_cds_tipo_origem = dimensaoti8_.co_seq_dim_tipo_origem
    INNER JOIN 
        tb_dim_turno dimensaotu9_ ON atend.co_dim_turno = dimensaotu9_.co_seq_dim_turno
    INNER JOIN 
        tb_dim_cbo cbo ON atend.co_dim_cbo_1 = cbo.co_seq_dim_cbo
    INNER JOIN 
        tb_fat_proced_atend fat_proced_atend ON atend.nu_uuid_ficha = fat_proced_atend.nu_uuid_ficha
    INNER JOIN    
        tb_fat_cidadao_pec tfcp ON atend.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
    INNER JOIN
        tb_cidadao cid ON tfcp.co_cidadao = cid.co_seq_cidadao
    JOIN
        public.tb_tipo_logradouro ON tb_tipo_logradouro.co_tipo_logradouro = cid.tp_logradouro,
    REGEXP_SPLIT_TO_TABLE(fat_proced_atend.ds_filtro_procedimento, '\|') AS procedimento -- Divide a coluna em linhas
    WHERE 
        unidade.nu_cnes = '9001808' 
        AND cbo.nu_cbo LIKE '225310'
        AND TO_CHAR(TO_DATE(atend.co_dim_tempo::text, 'YYYYMMDD'), 'YYYY') = '2024'
        AND TO_CHAR(TO_DATE(atend.co_dim_tempo::text, 'YYYYMMDD'), 'MM') = '11'
        AND procedimento IN ('0302070036', '0302010025', '0302070010', '0302060057', '0302060030', '0302060014', '0302050027', '0302050019', '0302040056', '0302040030', '0302040021', '0302060022')
    ORDER BY 9,13'''}
        
    elif tipo =='visitas':
        queries ={'tb_visitas': '''select 
case when t0.nu_cpf_cidadao is null then null else 'X' end as nu_cpf_cidadao,
case when t0.nu_cns is null then null else 'X' end as nu_cns,
co_dim_tipo_ficha, co_dim_municipio, co_dim_profissional, co_dim_tipo_imovel, nu_micro_area, nu_peso, nu_altura, t0.dt_nascimento, co_dim_faixa_etaria, st_visita_compartilhada, st_mot_vis_cad_att, st_mot_vis_visita_periodica, st_mot_vis_busca_ativa, st_mot_vis_acompanhamento, st_mot_vis_egresso_internacao, st_mot_vis_ctrl_ambnte_vetor, st_mot_vis_convte_atvidd_cltva, st_mot_vis_orintacao_prevncao, st_mot_vis_outros, st_busca_ativa_consulta, st_busca_ativa_exame, st_busca_ativa_vacina, st_busca_ativa_bolsa_familia, st_acomp_gestante, st_acomp_puerpera, st_acomp_recem_nascido, st_acomp_crianca, st_acomp_pessoa_desnutricao, st_acomp_pessoa_reabil_deficie, st_acomp_pessoa_hipertensao, st_acomp_pessoa_diabetes, st_acomp_pessoa_asma, st_acomp_pessoa_dpoc_enfisema, st_acomp_pessoa_cancer, st_acomp_pessoa_doenca_cronica, st_acomp_pessoa_hanseniase, st_acomp_pessoa_tuberculose, st_acomp_sintomaticos_respirat, st_acomp_tabagista, st_acomp_domiciliados_acamados, st_acomp_condi_vulnerab_social, st_acomp_condi_bolsa_familia, st_acomp_saude_mental, st_acomp_usuario_alcool, st_acomp_usuario_outras_drogra, st_ctrl_amb_vet_acao_educativa, st_ctrl_amb_vet_imovel_foco, st_ctrl_amb_vet_acao_mecanica, st_ctrl_amb_vet_tratamnt_focal, co_dim_desfecho_visita, co_dim_tipo_origem_dado_transp, co_dim_cds_tipo_origem, co_fat_cidadao_pec, co_dim_tipo_glicemia, nu_medicao_pressao_arterial, nu_medicao_temperatura, nu_medicao_glicemia,
case when nu_latitude between -33.75 and 5.25 then nu_latitude else null end as nu_latitude,
case when nu_longitude between -74.00 and -34.80 then nu_longitude else null end as nu_longitude,
case
when (case when nu_latitude between -33.75 and 5.25 then 1 else 0 end) = 1
and (case when nu_longitude between -74.00 and -34.80 then 1 else 0 end) = 1 then 1 else 0 end as com_localizacao,
case 
when (case when nu_latitude between -33.75 and 5.25 then 1 else 0 end) = 0 and 
(case when nu_longitude between -74.00 and -34.80 then 1 else 0 end) = 0 then 1 else 0 end as sem_localizacao,
case when t12.co_seq_dim_tipo_origem = '2' or t12.co_seq_dim_tipo_origem = '3' then 1 else 0 end as registro_cds,
case when t12.co_seq_dim_tipo_origem = '6' then 1 else 0 end as registro_app_territorio,
ds_dia_semana, nu_dia, nu_mes, nu_ano,
case when ds_dia_semana ilike any (array['%s%bado%', '%domin%']) then 1 else 0 end as sab_dom,
t13.ds_turno,
t7.ds_sexo,
t7.sg_sexo,
initcap(t3.no_equipe) as no_equipe,
t3.nu_ine,
t14.nu_cnes,
initcap(t14.no_unidade_saude) as no_unidade_saude,
initcap(t18.no_profissional) as no_profissional,
t6.co_seq_dim_profissional,
CASE st_visita_compartilhada
	when 1 THEN 'SIM'
	ELSE 'NÃO' END AS st_visita_compartilhada_mcaf,
case when nu_altura is null then 0 else 1 end as "CONTAGEM_ALTURA",
case when nu_peso is null then 0 else 1 end as "CONTAGEM_PESO",
case when nu_medicao_temperatura is null then 0 else 1 end as "CONTAGEM_TEMPERATURA",
case when nu_medicao_pressao_arterial is null then 0 else 1 end as "CONTAGEM_PRESSÃO_ARTERIAL",
case when nu_medicao_glicemia is null then 0 else 1 end as "CONTAGEM_GLICEMIA",
CASE WHEN t0.NU_CNS IS NULL OR '0' THEN 0 ELSE 1 END AS "SEM_NU_CNS_MCAF",
CASE WHEN t0.NU_CPF_CIDADAO IS NULL OR '0' THEN 0 ELSE 1 END AS "SEM_NU_CPF_MCAF",
CASE
WHEN t1.no_cbo ilike any (array['%AGENTE COMUNIT%RIO DE SA%DE%']) then 'ACS'
WHEN t1.no_cbo ilike any (array['%AGENTE DE COMBATE %S ENDEMIAS%']) then 'ACE' END AS "CBO_MCAF",
CASE WHEN t0.st_acomp_recem_nascido = '0' AND t0.st_acomp_crianca = '0' THEN '0' ELSE '1' END AS "soma_recem_nascido+crianca",
dt_registro as "DT_VISITA_MCAF",
to_char(t19.dt_ficha, 'hh24:mi:ss') as hr_min_seg,
case when t20.ds_anotacao is null then 'NÃO' else 'SIM' end as anotacao,
to_char(dt_registro, 'dd/mm/yyyy') as "DT_VISITA_MAP",
to_char(t0.dt_nascimento, 'DD/MM/YYYY') as "DT_NASCIMENTO_MCAF",
extract(year from age(current_date,t0.dt_nascimento)) as "IDADE_ATUAL",
extract(year from age(t8.dt_registro, t0.dt_nascimento)) as "IDADE_NA_VISITA",
case t12.co_seq_dim_tipo_origem
	when '1' then 'NÃO INFORMADO'
	when '2' then 'E-SUS CDS OFFLINE'
	when '3' then 'E-SUS CDS ONLINE'
	when '4' then 'PEC'
	when '5' then 'EXTERNO'
	when '6' then 'E-SUS TERRITORIO'
	when '7' then 'E-SUS ATIVIDADE COLETIVA'
	when '8' then 'E-SUS VACINAÇÃO'
	end as "origem_visita",
t10.ds_tipo_imovel
from tb_fat_visita_domiciliar t0 
left join tb_dim_cbo t1 on t1.co_seq_dim_cbo = t0.co_dim_cbo
left join tb_dim_desfecho_visita t2 on t2.co_seq_dim_desfecho_visita = t0.co_dim_desfecho_visita
left join tb_dim_equipe t3 on t3.co_seq_dim_equipe = t0.co_dim_equipe
left join tb_dim_profissional t6 on t6.co_seq_dim_profissional = t0.co_dim_profissional
left join tb_dim_sexo t7 on t7.co_seq_dim_sexo = t0.co_dim_sexo
left join tb_dim_tempo t8 on t8.co_seq_dim_tempo = t0.co_dim_tempo
left join tb_dim_tipo_ficha t9 on t9.co_seq_dim_tipo_ficha = t0.co_dim_tipo_ficha
left join tb_dim_tipo_imovel t10 on t10.co_seq_dim_tipo_imovel = t0.co_dim_tipo_imovel
left join tb_dim_tipo_glicemia t11 on t11.co_seq_dim_tipo_glicemia = t0.co_dim_tipo_glicemia
left join tb_dim_tipo_origem t12 on t12.co_seq_dim_tipo_origem = t0.co_dim_cds_tipo_origem
left join tb_dim_turno t13 on t13.co_seq_dim_turno = t0.co_dim_turno
left join tb_dim_unidade_saude t14 on t14.co_seq_dim_unidade_saude = t0.co_dim_unidade_saude
left join tb_dim_tipo_origem_dado_transp t15 on t15.co_seq_dim_tp_orgm_dado_transp = t0.co_dim_tipo_origem_dado_transp
--left join tb_fat_cidadao_pec t16 on t16.co_seq_fat_cidadao_pec = t0.co_fat_cidadao_pec
left join tb_prof_historico_cns t17 on t17.nu_cns = t6.nu_cns
left join tb_prof t18 on t18.co_seq_prof = t17.co_prof
left Join tb_cds_ficha_visita_domiciliar t19 on t19.co_unico_ficha = t0.nu_uuid_ficha
left join tb_visita_domiciliar_acs t20 on t20.co_unico_visita_domiciliar = t0.nu_uuid_ficha
where
--t1.no_cbo ilike any (array['%AGENTE DE COMBATE %S ENDEMIAS%', '%AGENTE COMUNIT%RIO DE SA%DE%'])
t1.nu_cbo like any (array['515105', '515140', '5151F1'])
--OPÇÃO 1 - EXTRAIR VISITAS ENTRE PERÍODOS (INICIO/FIM)
--and dt_registro between '2022-01-01' and '3000-12-31'
--OPÇÃO 2 - EXTRAIR VISITAS DOS ÚLTIMOS 12 MESES
--and dt_registro date_trunc('month', current_date) - interval '47 months'
--OPÇÃO 3 - EXTRAIR VISITAS APENAS DO ANO ATUAL
and date_part('year', dt_registro) = date_part('year', current_date)'''} 
    
    elif tipo =='iaf':
        queries ={'tb_iaf': '''select 
t3.nu_ano, 
t3.nu_mes,
t2.nu_cnes AS "Cnes",     
t2.no_unidade_saude AS "Nome da Unidade",     
initcap(t6.no_profissional) AS "Profissional",     
initcap(t7.no_cbo) AS "Cbo",     
SUM(t1.nu_participantes) AS "Total de Participantes",     
SUM(t1.nu_participantes_registrados) AS "Total Participantes Registrados",     
COUNT(*) AS "Total de Atividades" FROM tb_fat_atividade_coletiva t1 
LEFT JOIN tb_dim_unidade_saude t2 ON t1.co_dim_unidade_saude = t2.co_seq_dim_unidade_saude 
LEFT JOIN tb_dim_tempo t3 ON t1.co_dim_tempo = t3.co_seq_dim_tempo 
LEFT JOIN tb_unidade_saude t4 ON t2.nu_cnes = t4.nu_cnes 
LEFT JOIN tb_dim_profissional t6 ON t1.co_dim_profissional = t6.co_seq_dim_profissional 
LEFT JOIN tb_dim_cbo t7 ON t1.co_dim_cbo = t7.co_seq_dim_cbo WHERE t1.ds_filtro_pratica_em_saude ILIKE '%|11|%' AND     t2.nu_cnes IN (SELECT nu_cnes FROM tb_unidade_saude         
LEFT JOIN tb_tipo_unidade_saude ON tp_unidade_saude = co_seq_tipo_unidade_saude         
WHERE st_ativo = 1 AND co_tipo_unidade_cnes IN (1,2,32)) GROUP BY 1,2,3,4,5,6'''}
    
    elif tipo =='pse':
        queries ={'tb_pse': '''SELECT    
inep_sql.nu_ano as "Ano",
inep_sql.nu_mes as "Mes",
tinep.nu_identificador AS Inep,     
tinep.no_estabelecimento AS "Nome da Escola",     
SUM(CASE WHEN '|' || inep_sql.filter_pratica_em_saude || '|' LIKE '%|24|%' THEN inep_sql.total_activites ELSE 0 END) AS "Verificação da Situação Vacinal",     
SUM(CASE WHEN '|' || inep_sql.filter_pratica_em_saude || '|' LIKE '%|2|%' THEN inep_sql.total_activites ELSE 0 END) AS "Aplicação de Fluor",     
SUM(CASE WHEN '|' || inep_sql.filter_pratica_em_saude || '|' LIKE '%|22|%' THEN inep_sql.total_activites ELSE 0 END) AS "Saude Auditiva",     
SUM(CASE WHEN '|' || inep_sql.filter_pratica_em_saude || '|' LIKE '%|3|%' THEN inep_sql.total_activites ELSE 0 END) AS "Saude Ocular",     
SUM(CASE WHEN '|' || inep_sql.filter_pratica_em_saude || '|' LIKE '%|11|%' THEN inep_sql.total_activites ELSE 0 END) AS "Praticas Corporais Atividade Fisica",     
SUM(CASE WHEN '|' || inep_sql.filter_pratica_em_saude || '|' LIKE '%|20|%' THEN inep_sql.total_activites ELSE 0 END) AS "Antropometria",     
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|16|%' THEN inep_sql.total_activites ELSE 0 END) AS "Saude Mental",     
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|17|%' THEN inep_sql.total_activites ELSE 0 END) AS "Saude Sexual e Reprodutiva",     
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|15|%' THEN inep_sql.total_activites ELSE 0 END) AS "Saude Bucal",     
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|5|%' THEN inep_sql.total_activites ELSE 0 END) AS "Cidadania e Direitos Humanos",     
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|19|%' THEN inep_sql.total_activites ELSE 0 END) AS "Agravos e Doenças Negligenciadas",     
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|13|%' THEN inep_sql.total_activites ELSE 0 END) AS "Prevenção da Violencia",     
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|7|%' THEN inep_sql.total_activites ELSE 0 END) AS "Prevenção ao Uso de Alcool",     
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|29|%' THEN inep_sql.total_activites ELSE 0 END) AS "Ações de Combate a Dengue",     
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|14|%' THEN inep_sql.total_activites ELSE 0 END) AS "Saude Ambiental",     
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|1|%' THEN inep_sql.total_activites ELSE 0 END) AS "Alimentação Saudavel",     
SUM(inep_sql.total_activites) AS "Total de Atividades", 
CASE WHEN SUM(total_activites) >= 1 THEN 'SIM' ELSE 'NÃO' END AS "Indicador 1",
CASE WHEN         (SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|1|%' THEN 1 ELSE 0 END) >= 1 OR          
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|13|%' THEN 1 ELSE 0 END) >= 1 OR          
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|16|%' THEN 1 ELSE 0 END) >= 1 OR          
SUM(CASE WHEN '|' || inep_sql.filter_tema_para_saude || '|' LIKE '%|17|%' THEN 1 ELSE 0 END) >= 1) OR         
(SUM(CASE WHEN '|' || inep_sql.filter_pratica_em_saude || '|' LIKE '%|11|%' THEN 1 ELSE 0 END) >= 1)     
THEN 'SIM' ELSE 'NÃO' END AS "Indicador 2"
FROM     tb_dim_inep AS tinep LEFT JOIN      (SELECT  t3.nu_ano,t3.nu_mes,t4.nu_identificador AS inep,         
t4.no_estabelecimento AS "Nome da Escola",         t1.ds_filtro_pratica_em_saude AS "filter_pratica_em_saude",         
t1.ds_filtro_tema_para_saude AS filter_tema_para_saude,         COUNT(*) AS total_activites     
FROM         tb_fat_atividade_coletiva t1     
LEFT JOIN tb_dim_unidade_saude t2 ON t1.co_dim_unidade_saude = t2.co_seq_dim_unidade_saude     
LEFT JOIN tb_dim_tempo t3 ON t1.co_dim_tempo = t3.co_seq_dim_tempo     
LEFT JOIN tb_dim_inep t4 ON t1.co_dim_inep = t4.co_seq_dim_inep     
LEFT JOIN tb_dim_procedimento t5 ON t1.co_dim_procedimento = t5.co_seq_dim_procedimento     
WHERE  (t1.st_pse_educacao = 1 OR t1.st_pse_saude = 1) 
AND         (t1.ds_filtro_pratica_em_saude LIKE ANY (ARRAY['%|24|%','%|2|%','%|22|%','%|3|%','%|11|%','%|20|%']) 
OR           t1.ds_filtro_tema_para_saude LIKE ANY (ARRAY['%|16|%','%|17|%','%|15|%','%|5|%','%|19|%','%|13|%','%|7|%','%|29|%','%|14|%','%|1|%'])
OR          t5.co_proced = '0101010095')         AND t4.st_registro_valido = 1 
GROUP BY      t3.nu_ano,t3.nu_mes,t4.nu_identificador,     t4.no_estabelecimento,     t1.ds_filtro_pratica_em_saude,     t1.ds_filtro_tema_para_saude ) AS inep_sql 
ON inep_sql.inep = tinep.nu_identificador WHERE     tinep.st_registro_valido = 1 AND tinep.nu_identificador <> '-'   
AND tinep.nu_identificador IN ('35004124','35075279','35122877','35167538','35229337','35233031','35281414','35281426','35286023','35349550','35357522','35437372','35448000','35457619','35472682','35480770','35480782')
GROUP BY   inep_sql.nu_ano, inep_sql.nu_mes,  tinep.nu_identificador,     tinep.no_estabelecimento'''}

    elif tipo =='pse_prof':
        queries ={'tb_pse_prof': '''SELECT 
        t3.nu_ano as "Ano" ,
        t3.nu_mes as "Mes",                  
        t4.nu_identificador AS "Inep",   
        initcap(t4.no_estabelecimento) AS "Nome da Escola",   
        CASE WHEN t1.st_pse_educacao = 1 THEN 'PSE EDUCAÇÃO' WHEN t1.st_pse_saude = 1 THEN 'PSE SAÚDE' ELSE NULL END AS "PSE",   
        t6.nu_cns as "Cns",   
        t6.no_profissional AS "Profissional",
        t7.nu_cbo as "Cbo", 
        initcap(t7.no_cbo) as "Nome Cbo",
        t8.nu_ine as "Ine",   
        initcap(t8.no_equipe) as "Nome da Equipe",   
        t9.nu_cnes as "Cnes",   
        t9.no_unidade_saude AS "Nome da Unidade",   
        SUM(t1.nu_participantes) AS "Total de Participantes", 
        SUM(t1.nu_participantes_registrados) AS "Total de Participantes Registrados",   
        COUNT(*) AS "Total de Atividades"
        FROM   tb_fat_atividade_coletiva t1   
        LEFT JOIN tb_dim_unidade_saude t2 ON t1.co_dim_unidade_saude = t2.co_seq_dim_unidade_saude   
        LEFT JOIN tb_dim_tempo t3 ON t1.co_dim_tempo = t3.co_seq_dim_tempo   
        LEFT JOIN tb_dim_inep t4 ON t1.co_dim_inep = t4.co_seq_dim_inep   
        LEFT JOIN tb_dim_procedimento t5 ON t1.co_dim_procedimento = t5.co_seq_dim_procedimento   
        LEFT JOIN tb_dim_profissional t6 ON t1.co_dim_profissional = t6.co_seq_dim_profissional   
        LEFT JOIN tb_dim_cbo t7 ON t1.co_dim_cbo = t7.co_seq_dim_cbo   
        LEFT JOIN tb_dim_equipe t8 ON t1.co_dim_equipe = t8.co_seq_dim_equipe   
        LEFT JOIN tb_dim_unidade_saude t9 ON t1.co_dim_unidade_saude = t9.co_seq_dim_unidade_saude   
        LEFT JOIN tb_dim_procedimento t10 ON t1.co_dim_procedimento = t10.co_seq_dim_procedimento   
        LEFT JOIN tb_fat_atvdd_coletiva_ext t11 ON t11.co_fat_atividade_coletiva = t1.co_seq_fat_atividade_coletiva 
        WHERE  (t1.st_pse_educacao = 1 OR t1.st_pse_saude = 1) AND   (t1.ds_filtro_pratica_em_saude 
        LIKE ANY (ARRAY['%|24|%','%|2|%','%|22|%','%|3|%','%|11|%','%|20|%'])     OR     t1.ds_filtro_tema_para_saude 
        LIKE ANY (ARRAY['%|16|%','%|17|%','%|15|%','%|5|%','%|19|%','%|13|%','%|7|%','%|29|%','%|14|%','%|1|%'])     OR     t5.co_proced = '0101010095') AND   t4.st_registro_valido = 1  
        AND t4.nu_identificador IN ('35004124','35075279','35122877','35167538','35229337','35233031','35281414','35281426','35286023','35349550','35357522','35437372','35448000','35457619','35472682','35480770','35480782')
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,13'''}

    if queries is None:
        raise ValueError(f"Tipo de consulta desconhecido: {tipo}")

    step_size = 100 / len(queries)
    for query_name, query in queries.items():
        execute_query(query_name, text(query), external_engine, local_engine, step_size, tipo, 
                      params = {'ano': ano, 'mes': mes} if tipo in ['bpa'] else None,
                      update_progress=update_progress)

    progress[tipo] = 100
    if update_progress:
        update_progress(100)  # Atualiza o progresso para 100% ao final