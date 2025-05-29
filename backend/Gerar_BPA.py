# backend/Gerar_BPA.py
from socketio_config import socketio  # For processar_bpa and update_progress if kept
from Conexões import get_local_engine # Still used by load_db_config, format_field, etc.
# from Conexões import log_message # Moved to db_operations or replaced by local logger
from sqlalchemy import text # Still used by criar_arquivo_bpa
# import json # Moved to file_utils
import logging
# import requests # Moved to cep_utils
# import time # Moved to cep_utils
# import xml.etree.ElementTree as ET # Not used by remaining functions
# from sqlalchemy import text # Already imported above
# from Conexões import get_local_engine, log_message # Already imported above
# from socketio_config import socketio # Already imported above

# Import moved CEP utility functions
from .services.bpa_service.cep_utils import (
    limpar_logradouro,
    buscar_dados_opencep, # Not directly used by remaining functions in Gerar_BPA.py
    buscar_dados_viacep, # Not directly used by remaining functions in Gerar_BPA.py
    buscar_dados_apicep, # Not directly used by remaining functions in Gerar_BPA.py
    buscar_por_logradouro_estado_cidade_rua, # Not directly used by remaining functions in Gerar_BPA.py
    buscar_dados_cep # Not directly used by remaining functions in Gerar_BPA.py (it's used by atualizar_enderecos)
)

# Import moved DB operation functions
from .services.bpa_service.db_operations import (
    # atualizar_enderecos, # Not called directly from Gerar_BPA.py anymore
    consultar_bpa_dados,
    consultar_bpa_dados_ano_mes,
    executar_procedure,
    executar_procedure_segunda,
    executar_procedure_terceira,
    executar_procedure_quarta
)

# Configuração do log - REMOVED (centralized in app.py)
# This basicConfig might conflict if app.py also calls it.
# Ideally, basicConfig is called once at the application's entry point.
# For now, keeping it as it was in Gerar_BPA.py.
# logging.basicConfig(
#     filename='logs_ceps.txt', # This will create logs_ceps.txt in the current working directory
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     encoding='utf-8'
# )
logger = logging.getLogger(__name__) # Local logger for this module, will use root config

# Import moved formatting functions
from .services.bpa_service.formatting import format_field, format_bpa_row, gerar_cabecalho_bpa

# Import moved file utility functions
from .services.bpa_service.file_utils import write_bpa_to_txt, load_db_config, carregar_config_bpa


# Functions moved to services.bpa_service.db_operations:
# - atualizar_enderecos
# - consultar_bpa_dados
# - consultar_bpa_dados_ano_mes
# - executar_procedure
# - executar_procedure_segunda
# - executar_procedure_terceira
# - executar_procedure_quarta

# Functions load_db_config, write_bpa_to_txt, carregar_config_bpa were moved to file_utils.py

def criar_arquivo_bpa():
    columns = ['PRD_UID', 'PRD_CMP', 'PRD_CNSMED', 'PRD_CBO', 'PRD_FLH', 'PRD_SEQ', 
               'PRD_PA', 'PRD_CNSPAC', 'PRD_CPF_PCNT', 'PRD_NMPAC', 'PRD_DTNASC', 
               'PRD_SEXO', 'PRD_IBGE', 'PRD_DTATEN', 'PRD_CID', 'PRD_IDADE', 
               'PRD_QT_P', 'PRD_CATEN', 'PRD_NAUT', 'PRD_ORG', 'PRD_MVM', 'PRD_FLPA', 
               'PRD_FLCBO', 'PRD_FLCA', 'PRD_FLIDA', 'PRD_FLQT', 'PRD_FLER', 
               'PRD_FLMUN', 'PRD_FLCID', 'PRD_RACA', 'PRD_SERVICO', 'PRD_CLASSIFICACAO', 
               'PRD_ETNIA', 'PRD_NAC', 'PRD_ADVQT', 'PRD_CNPJ', 'PRD_EQP_AREA', 
               'PRD_EQP_SEQ', 'PRD_LOGRAD_PCNTE', 'PRD_CEP_PCNTE', 'PRD_END_PCNTE', 
               'PRD_COMPL_PCNTE', 'PRD_NUM_PCNTE', 'PRD_BAIRRO_PCNTE', 'PRD_DDTEL_PCNTE', 
               'PRD_TEL_PCNTE', 'PRD_EMAIL_PCNTE', 'PRD_INE']

    # Configuração da engine
    engine = get_local_engine()

    try:
        with engine.connect() as connection:
            # Inicia uma transação explícita
            transaction = connection.begin()

            try:
                log_message("Executando Procedure 1")
                executar_procedure(connection)

                log_message("Executando Procedure 2")
                executar_procedure_segunda(connection)

                log_message("Executando Procedure 3")
                max_folha_terceira = executar_procedure_terceira(connection)
                log_message(f"Max Folha Terceira: {max_folha_terceira}")

                log_message("Executando Procedure 4")
                max_folha_quarta = executar_procedure_quarta(connection)
                log_message(f"Max Folha Quarta: {max_folha_quarta}")
                
                # Calcula o maior valor de folha entre as duas procedures
                max_folha = max_folha_terceira + max_folha_quarta
                log_message(f"Max Folha Calculada: {max_folha}")

                transaction.commit()

            except Exception as e:
                log_message(f"Erro ao executar as procedures: {e}")
                if transaction.is_active:
                    transaction.rollback()
                raise

        # Consulta final ao banco de dados após as procedures
        query = text("""SELECT * FROM tb_bpa order by prd_org, prd_uid, prd_pa, prd_cbo, prd_cmp, prd_flh, prd_seq, prd_qt_p""")
        with engine.connect() as connection:
            result = connection.execute(query)
            results = result.fetchall()

        if results:
            # Exibe os nomes das colunas e a primeira linha de resultados para verificação
            log_message(f"Colunas: {columns}")
            log_message(f"Primeira linha de resultados: {results[0]}")
        if not results:
            log_message("Nenhuma linha foi retornada.")
            return None
        
        # Calcula o campo de controle
# Corrige o cálculo para verificar se os valores são numéricos e não vazios
        soma_total = sum(
            int(row[6]) + int(row[16])
            for row in results
            if row[6] is not None and row[6] != '' and row[16] is not None and row[16] != '')
        resto_divisao = soma_total % 1111
        campo_controle = 1111 + resto_divisao
        log_message(f"Campo de Controle Calculado: {campo_controle}")

        # Calcula o campo de controle
# Corrige o cálculo para verificar se os valores são numéricos e não vazios
        soma_total = sum(
            int(row[6]) + int(row[16]) # Assuming PRD_PA (index 6) and PRD_QT_P (index 16)
            for row in results
            if row[6] is not None and str(row[6]).strip().isdigit() and \
               row[16] is not None and str(row[16]).strip().isdigit()
        )
        resto_divisao = soma_total % 1111
        campo_controle = 1111 + resto_divisao
        logger.info(f"Campo de Controle Calculado: {campo_controle}")

        # Carregar config para seqX valores
        seq_config_values = carregar_config_bpa()

        # Cabeçalho BPA
        ano_mes = consultar_bpa_dados_ano_mes()
        total_registros = len(results) + 1
        # Use imported gerar_cabecalho_bpa and pass seq_config_values
        cabecalho = gerar_cabecalho_bpa(ano_mes, total_registros, max_folha, campo_controle, seq_config_values) + "\n"

        # Corpo BPA
        formatted_results = []
        for row in results:
            row_dict = dict(zip(columns, row))
            formatted_line = format_bpa_row(row_dict).strip() + "\n"  # Garante que cada linha tem uma quebra de linha no final
            formatted_results.append(formatted_line)

        # Monta o conteúdo final com quebras de linha corretas
        bpa_content = cabecalho + ''.join(formatted_results)

        # Salva o conteúdo em um arquivo
        filepath = f'bpa_{ano_mes}.txt'
        with open(filepath, 'w') as file:
            file.write(bpa_content)

        log_message(f"BPA gerado em {filepath}")
        return filepath

    except Exception as e:
        log_message(f"Erro ao gerar BPA: {e}")
        raise

def executar_procedure(connection):
    # Defina os IDs dos PA que você deseja incluir no WHERE
    pa_ids = [
        '0414020243', '0301100012', '0414020146', '0102010498', 
        '0307030032', '0401010066', '0101010036', '0101010010', 
        '0404020674', '0404020577', '0101020074', '0307030040', 
        '0301060037', '0307020010', '0301060118', '0301060100',
        '0414020383', '0414020405', '0307020070', '0301060029', 
        '0307010015', '0414020120', '0101020090', '0414020138', 
        '0301060096', '0101040024', '0102010056', '0201010020',
        '0201010470', '0211070041', '0211070203', '0211070211',
        '0301040079', '0301100039', '0401010023', '0307010023',
        '0301010153', '0414020278', '0205020100', '0309050049',
        '0205020186', '0102010293', '0301080399', '0202030776',
        '0307020118', '0401010031', '0309050022', '0307040151',
        '0102010510', '0211080055', '0102010072', '0102010218',
        '0301080259', '0301080267', '0102010242', '0414020073',
        '0102010323', '0301040036', '0101020040', '0102010226',
        '0101020015', '0101020023', '0102010528', '0101020082',
        '0102010307', '0102010064', '0101020066', '0101020058',
        '0404020615', '0307040135', '0401010074', '0414020170',
        '0211020036', '0211020052', '0301100101', '0301080160',
        '0301100152', '0301100179', '0202010473', '0201020041',
        '0211060275', '0307010040', '0101020031', '0102010340',
        '0102010501', '0214010015', '0414020359', '0307030059',
        '0307010031', '0202060446', '0204010071'
    ]

    # Primeiro, selecione e agrupe os dados
    query = text(f"""
    SELECT s_prd.prd_uid,
           s_prd.prd_pa,
           s_prd.prd_cbo,
           s_prd.prd_cmp,
           SUM(s_prd.prd_qt_p) as prd_qt_p
    FROM tb_bpa as s_prd
    WHERE s_prd.prd_org = 'BPI'
      AND s_prd.prd_pa IN :pa_ids
    GROUP BY s_prd.prd_uid, s_prd.prd_pa, s_prd.prd_cbo, s_prd.prd_cmp
    """)

    results = connection.execute(query, {'pa_ids': tuple(pa_ids)})

    for row in results:
        # Inserir os dados agrupados
        insert_query = text("""
        INSERT INTO tb_bpa (PRD_UID, PRD_CMP, PRD_CNSMED, PRD_CBO, PRD_FLH, PRD_SEQ, PRD_PA, PRD_CNSPAC,
                           PRD_SEXO, PRD_IBGE, PRD_DTATEN, PRD_CID, PRD_IDADE, PRD_QT_P, PRD_CATEN,
                           PRD_NAUT, PRD_ORG)
        VALUES (:prd_uid, :prd_cmp, '', :prd_cbo, '001', '01', :prd_pa, '', '', '', '', '', '000',
                :prd_qt_p, '', '', 'BPA')""")
        connection.execute(insert_query, {
            'prd_uid': row.prd_uid,
            'prd_cmp': row.prd_cmp,
            'prd_cbo': row.prd_cbo,
            'prd_pa': row.prd_pa,
            'prd_qt_p': row.prd_qt_p
        })

    # Executar o DELETE com os IDs corretos fora do loop
    delete_query = text("""
        DELETE FROM tb_bpa
        WHERE prd_org = 'BPI'
          AND (prd_pa LIKE '%ABPG%' 
          OR prd_pa LIKE '%ABEX%' 
          OR prd_pa IN :pa_ids
          OR prd_pa IN (
              '0101020104', '0101030010', '0214010201', '0301010269', '0309010063', 
              '0301040141', '0301050139', '0301050147', '0301010277', '0309010047',
              '0301100195', '0301100209', '0301100217', '0301100225', '0301100233',
              '0301100241', '0301100276', '0301100284', '0307010155', '0214010082'
          ))
    """)
    connection.execute(delete_query, {'pa_ids': tuple(pa_ids)})

    log_message("Primeira Procedure Concluída")

    # Primeiro update
    update_uid_query_1 = text("""
    UPDATE tb_bpa
    SET prd_cid = 'G804'
    WHERE prd_uid = '0491381'
    """)
    connection.execute(update_uid_query_1)
    log_message("Update 1 (CID para UID 0491381) executado")
    
    # Segundo update
    update_uid_query_2 = text("""
    UPDATE tb_bpa
    SET prd_cep_pcnte = '07400959',
        prd_end_pcnte = 'dos Expedicionários',
        prd_num_pcnte = '290',
        prd_bairro_pcnte = 'Jardim Rincão',
        prd_ibge = '350390'
    WHERE prd_cep_pcnte = '07400000' or 
        prd_ibge <> '350390'
    """)
    connection.execute(update_uid_query_2)
    log_message("Update 2 (CEP 07400000) executado")


from sqlalchemy.sql import text
import time  # opcional, para pausas visuais se quiser ver no log

def executar_procedure_segunda(connection):
    # IDs dos PA a serem incluídos no WHERE
    pa_ids = [
        '0301010110', '0301010030', '0301010056',
        '0301010064', '0301010137'
    ]

    # 1. Seleção e agrupamento dos dados
    query = text("""
    SELECT s_prd.prd_uid,
           s_prd.prd_pa,
           s_prd.prd_cbo,
           s_prd.prd_cmp,
           s_prd.prd_idade,
           SUM(s_prd.prd_qt_p) as prd_qt_p
    FROM tb_bpa as s_prd
    WHERE s_prd.prd_org = 'BPI'
      AND s_prd.prd_pa IN :pa_ids
    GROUP BY s_prd.prd_uid, s_prd.prd_pa, s_prd.prd_cbo, s_prd.prd_cmp, s_prd.prd_idade
    """)
    results = connection.execute(query, {'pa_ids': tuple(pa_ids)})

    # 2. Inserção dos dados agrupados + exclusão dos originais processados
    for row in results:
        insert_query = text("""
        INSERT INTO tb_bpa (PRD_UID, PRD_CMP, PRD_CNSMED, PRD_CBO, PRD_FLH, PRD_SEQ, PRD_PA, PRD_CNSPAC,
                           PRD_SEXO, PRD_IBGE, PRD_DTATEN, PRD_CID, PRD_IDADE, PRD_QT_P, PRD_CATEN,
                           PRD_NAUT, PRD_ORG)
        VALUES (:prd_uid, :prd_cmp, '', :prd_cbo, '001', '01', :prd_pa, '', '', '', '', '', :prd_idade,
                :prd_qt_p, '', '', 'BPA')
        """)
        connection.execute(insert_query, {
            'prd_uid': row.prd_uid,
            'prd_cmp': row.prd_cmp,
            'prd_cbo': row.prd_cbo,
            'prd_pa': row.prd_pa,
            'prd_idade': row.prd_idade,
            'prd_qt_p': row.prd_qt_p
        })

        delete_query = text("""
        DELETE FROM tb_bpa
        WHERE prd_org = 'BPI'
          AND prd_pa = :prd_pa
        """)
        connection.execute(delete_query, {'prd_pa': row.prd_pa})

    log_message("Inserções e exclusões concluídas.")

    # 3. Updates sequenciais com logs


    # Segundo update
    update_uid_query_2 = text("""
    UPDATE tb_bpa
    SET prd_uid = CASE
        WHEN prd_uid = '0000001' THEN '6896847'
        WHEN prd_uid = '0491381' THEN '6430163'                      
        ELSE prd_uid
    END
    WHERE prd_uid IN ('0000001', '0491381')
    """)
    connection.execute(update_uid_query_2)
    log_message("Update 2 (substituição de UIDs) executado")

    # 4. Atualização prd_cid individual por PA
    update_por_pa = {
        '0302070036': 'T951',
        '0302010025': 'N319',
        '0302070010': 'T302',
        '0302060057': 'S141',
        '0302060030': 'G838',
        '0302060014': 'I694',
        '0302050027': 'M255',
        '0302050019': 'T932',
        '0302040056': 'I988',
        '0302040030': 'Q048',
        '0302040021': 'J998',
        '0302060022': 'I694',
        '0201010046': 'K629'
    }

    for pa, cid in update_por_pa.items():
        individual_update = text("""
            UPDATE tb_bpa
            SET prd_cid = :cid
            WHERE prd_pa = :pa
        """)
        connection.execute(individual_update, {'cid': cid, 'pa': pa})
        log_message(f"Update CID executado para PA {pa} com CID {cid}")
        # time.sleep(0.5)  # opcional: delay visual
    
    log_message("Segunda Procedure Concluída")

def executar_procedure_terceira(connection):

    fol_novo = 0
    seq_novo = 0
    cnes = ''
    max_folha_terceira = 0  # Capturar o maior número de folha

    # Consulta para selecionar os dados
    query_select = text("""
    SELECT s_prd.prd_uid,
           s_prd.prd_pa,
           s_prd.prd_cbo,
           s_prd.prd_flh,
           s_prd.prd_seq,
           s_prd.prd_idade,
           s_prd.prd_qt_p,
           s_prd.prd_org
    FROM tb_bpa as s_prd
    WHERE s_prd.prd_org = 'BPA'
    ORDER BY s_prd.prd_uid, s_prd.prd_pa, s_prd.prd_cbo, s_prd.prd_flh,
           s_prd.prd_seq
    """)

    results = connection.execute(query_select)

    for row in results:
        prd_uid = row.prd_uid
        prd_pa = row.prd_pa
        prd_cbo = row.prd_cbo
        prd_flh = row.prd_flh
        prd_seq = row.prd_seq
        prd_idade = row.prd_idade
        prd_qt_p = row.prd_qt_p
        prd_org = row.prd_org

        # Atualiza a sequência e a folha conforme a lógica descrita
        seq_novo += 1
        if prd_uid != cnes:
            cnes = prd_uid
            seq_novo = 1
            fol_novo = 1
            #if prd_uid == '6896847':
                #fol_novo = 100

        if seq_novo > 20:
            seq_novo = 1
            fol_novo += 1


        prd_flh_novo = f"{fol_novo:03}"
        prd_seq_novo = f"{seq_novo:02}"


        # Atualizar o registro na tabela
        update_query = text("""
        UPDATE tb_bpa
        SET prd_flh = :prd_flh_novo, prd_seq = :prd_seq_novo
        WHERE prd_uid = :prd_uid
          AND prd_pa = :prd_pa
          AND prd_cbo = :prd_cbo
          AND prd_flh = :prd_flh
          AND prd_seq = :prd_seq
          AND prd_idade = :prd_idade
          AND prd_qt_p = :prd_qt_p
          AND prd_org = :prd_org
        """)

        connection.execute(update_query, {
            'prd_flh_novo': prd_flh_novo,
            'prd_seq_novo': prd_seq_novo,
            'prd_uid': prd_uid,
            'prd_pa': prd_pa,
            'prd_cbo': prd_cbo,
            'prd_flh': prd_flh,
            'prd_seq': prd_seq,
            'prd_idade': prd_idade,
            'prd_qt_p': prd_qt_p,
            'prd_org': prd_org
        })

        # Atualizar max_folha_terceira
        if fol_novo > max_folha_terceira:
            max_folha_terceira = fol_novo
    log_message(f"Terceira Procedure Concluída")
    return max_folha_terceira

def executar_procedure_quarta(connection):
    fol_novo = 1  # Inicializa a folha na primeira
    seq_novo = 0  # Inicializa a sequência
    cnes_atual = None  # Para acompanhar mudanças de CNES
    max_folha_quarta = 0  # Acompanhar a maior folha usada

    # Variável para armazenar o último registro para comparação
    ultimo_registro = None

    # Consulta para selecionar os dados
    query_select = text("""
    SELECT
         s_prd.prd_uid,
         s_prd.prd_pa,
         s_prd.prd_cnsmed,
         s_prd.prd_cbo,
         s_prd.prd_flh,
         s_prd.prd_seq,
         s_prd.prd_idade,
         s_prd.prd_qt_p,
         s_prd.prd_org,
         s_prd.prd_nmpac,
         s_prd.prd_dtnasc,
         s_prd.prd_dtaten,
         s_prd.prd_cnspac,
         COALESCE(s_prd.prd_cid, '') AS prd_cid  -- Tratar CID como vazio se for nulo
    FROM tb_bpa as s_prd
    WHERE s_prd.prd_org = 'BPI'
    ORDER BY s_prd.prd_uid, s_prd.prd_cnsmed, s_prd.prd_cbo, s_prd.prd_flh, s_prd.prd_seq
    """)

    results = connection.execute(query_select)

    for row in results:
        # Desempacotando os valores
        prd_uid = row.prd_uid
        prd_pa = row.prd_pa
        prd_cnsmed = row.prd_cnsmed
        prd_cbo = row.prd_cbo
        prd_flh = row.prd_flh
        prd_seq = row.prd_seq
        prd_idade = row.prd_idade
        prd_qt_p = row.prd_qt_p
        prd_org = row.prd_org
        prd_nmpac = row.prd_nmpac
        prd_dtnasc = row.prd_dtnasc
        prd_dtaten = row.prd_dtaten
        prd_cnspac = row.prd_cnspac
        prd_cid = row.prd_cid  # CID pode ser vazio

        # Resetar contadores ao mudar de CNES
        if prd_uid != cnes_atual:
            cnes_atual = prd_uid
            fol_novo = 1
            seq_novo = 1  # Sempre iniciar com sequência 1 quando mudar de CNES
        else:
            # Criação de uma chave única para identificar cada registro
            # Incluindo o CID tratado (mesmo vazio)
            registro_atual = (prd_uid, prd_pa, prd_cnsmed, prd_nmpac, prd_dtaten, prd_cid)

            # Incrementa a sequência se o registro atual for diferente do último
            if registro_atual != ultimo_registro:
                seq_novo += 1
                ultimo_registro = registro_atual

        # Se a sequência for maior que 20, inicia uma nova folha
        if seq_novo > 20:
            seq_novo = 1
            fol_novo += 1

        # Formatar os valores para salvar no banco
        prd_flh_novo = f"{fol_novo:03}"  # Formatar folha com 3 dígitos
        prd_seq_novo = f"{seq_novo:02}"  # Formatar sequência com 2 dígitos

        # Atualizar o registro na tabela
        update_query = text("""
        UPDATE tb_bpa
        SET prd_flh = :prd_flh_novo,
            prd_seq = :prd_seq_novo
        WHERE prd_uid = :prd_uid
          AND prd_pa = :prd_pa
          AND prd_cnsmed = :prd_cnsmed
          AND prd_cbo = :prd_cbo
          AND prd_flh = :prd_flh
          AND prd_seq = :prd_seq
          AND prd_idade = :prd_idade
          AND prd_qt_p = :prd_qt_p
          AND prd_org = :prd_org
          AND prd_nmpac = :prd_nmpac
          AND prd_dtnasc = :prd_dtnasc
          AND prd_dtaten = :prd_dtaten
          AND prd_cnspac = :prd_cnspac
          AND COALESCE(prd_cid, '') = :prd_cid  -- Adicionando verificação CID
        """)

        connection.execute(update_query, {
            'prd_flh_novo': prd_flh_novo,
            'prd_seq_novo': prd_seq_novo,
            'prd_uid': prd_uid,
            'prd_pa': prd_pa,
            'prd_cnsmed': prd_cnsmed,
            'prd_cbo': prd_cbo,
            'prd_flh': prd_flh,
            'prd_seq': prd_seq,
            'prd_idade': prd_idade,
            'prd_qt_p': prd_qt_p,
            'prd_org': prd_org,
            'prd_nmpac': prd_nmpac,
            'prd_dtnasc': prd_dtnasc,
            'prd_dtaten': prd_dtaten,
            'prd_cnspac': prd_cnspac,
            'prd_cid': prd_cid  # Comparar diretamente
        })

        # Atualizar a maior folha para relatório final
        if fol_novo > max_folha_quarta:
            max_folha_quarta = fol_novo

    log_message(f"Max Folha Quarta: {max_folha_quarta}")
    log_message(f"Quarta Procedure Concluída")
    return max_folha_quarta

def processar_bpa():
    for progress in range(0, 101, 10):  # Incrementa de 10 em 10
        socketio.emit('progress_update', {'tipo': 'bpa', 'percentual': progress})
        time.sleep(1)  # Simulação de trabalho real

def update_progress(progress):
    # Emite o evento de progresso para o frontend via Socket.IO
    socketio.emit('progress_update', {'tipo': 'bpa', 'percentual': progress})

