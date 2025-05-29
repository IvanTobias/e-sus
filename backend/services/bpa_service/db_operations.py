import logging
from sqlalchemy import text

# Adjust import paths for Conexões based on the new location
# db_operations.py is in backend/services/bpa_service/
# Conexões.py is in backend/
from ...Conexões import get_local_engine, log_message

# Import from cep_utils in the same package
from .cep_utils import buscar_dados_cep

# Configure logging for this module
logger = logging.getLogger(__name__)
# Assuming basicConfig is called at app startup.

def atualizar_enderecos(connection, socketio_emitter): # socketio_emitter added
    ceps = connection.execute(text("SELECT DISTINCT prd_cep_pcnte FROM tb_bpa group by 1")).fetchall()

    total = len(ceps)
    atualizados, substituidos, falhas = 0, 0, 0

    for index, row in enumerate(ceps, 1):
        cep_original = row[0]
        # buscar_dados_cep is imported from .cep_utils
        info = buscar_dados_cep(cep_original, connection=connection) 

        if info:
            novo_cep = info.get('cep', cep_original).replace('-', '')
            if len(novo_cep) != 8:
                novo_cep = cep_original

            update_result = connection.execute(text("""
                UPDATE tb_bpa
                SET prd_end_pcnte = :logradouro,
                    prd_bairro_pcnte = :bairro,
                    prd_cep_pcnte = :novo_cep,
                    prd_ibge = :ibge
                WHERE prd_cep_pcnte = :cep_antigo
            """), {
                'logradouro': info.get('logradouro', ''),
                'bairro': info.get('bairro', ''),
                'cep_antigo': cep_original,
                'novo_cep': novo_cep,
                'ibge': info.get('ibge', '')
            })

            if update_result.rowcount > 0:
                if novo_cep != cep_original:
                    substituidos += 1
                    msg = f"✅ CEP {cep_original} atualizado para {novo_cep}: {info.get('logradouro')}, {info.get('bairro')} (IBGE: {info.get('ibge', '')})"
                else:
                    atualizados += 1
                    msg = f"✅ CEP {cep_original} atualizado: {info.get('logradouro')}, {info.get('bairro')} (IBGE: {info.get('ibge', '')})"
                logger.info(msg) # Changed from print and logging.info
            else:
                falhas += 1
                msg = f"⚠️ Nenhuma linha atualizada para CEP {cep_original}: talvez não encontrado no tb_bpa."
                logger.warning(msg) # Changed from print and logging.warning

        progresso = (index / total) * 100
        logger.info(f"📊 Progresso CEP Update: {index}/{total} ({progresso:.2f}%)")
        if socketio_emitter: # Check if an emitter was passed
            socketio_emitter.emit('progress_update', {'tipo': 'cep', 'percentual': int(progresso)})

    resumo = f"🔚 Total: {total} | Atualizados: {atualizados} | Substituídos: {substituidos} | Falhas: {falhas}"
    logger.info(resumo)
    if socketio_emitter: # Check if an emitter was passed
        socketio_emitter.emit('progress_update', {'tipo': 'cep', 'percentual': 100})

    # Commit is handled by the caller if it's part of a larger transaction,
    # or if connection is engine-level, it might autocommit depending on dialect.
    # Original code had connection.commit() here, which might be needed if connection
    # is not managed by a higher-level transaction context.
    # For now, removing explicit commit to allow caller to manage transaction.
    # try:
    #     connection.commit()
    # except Exception as e:
    #     logger.error(f"⚠️ Não foi possível dar commit automaticamente em atualizar_enderecos: {e}")


def consultar_bpa_dados():
    query = text("""Select * FROM tb_bpa""")
    engine = get_local_engine() # get_local_engine imported from ...Conexões
    with engine.connect() as connection:
        result = connection.execute(query)
        results = result.fetchall()
    if results:
        return results
    else:
        # log_message is imported from ...Conexões
        # However, using local logger is more consistent for service modules.
        logger.info("A consulta (consultar_bpa_dados) não retornou resultados.") 
    return []

def consultar_bpa_dados_ano_mes():
    query = text("""SELECT prd_cmp FROM tb_bpa LIMIT 1""")
    engine = get_local_engine()
    with engine.connect() as connection:
        result = connection.execute(query)
        results = result.fetchone()
    if results:
        ano_mes = results[0]
        logger.info(f"ano_mes encontrado: {ano_mes}")
        return ano_mes
    else:
        logger.info("A consulta (consultar_bpa_dados_ano_mes) não retornou resultados.")
        return None

def executar_procedure(connection):
    pa_ids = [
        '0414020243', '0301100012', '0414020146', '0102010498', 
        '0307030032', '0401010066', '0101010036', '0101010010', 
        # ... (rest of pa_ids as in original) ...
        '0307010031', '0202060446', '0204010071'
    ]
    # Ensure full list of pa_ids is here
    pa_ids = [
        '0414020243', '0301100012', '0414020146', '0102010498', '0307030032', '0401010066', '0101010036', '0101010010', 
        '0404020674', '0404020577', '0101020074', '0307030040', '0301060037', '0307020010', '0301060118', '0301060100',
        '0414020383', '0414020405', '0307020070', '0301060029', '0307010015', '0414020120', '0101020090', '0414020138', 
        '0301060096', '0101040024', '0102010056', '0201010020', '0201010470', '0211070041', '0211070203', '0211070211',
        '0301040079', '0301100039', '0401010023', '0307010023', '0301010153', '0414020278', '0205020100', '0309050049',
        '0205020186', '0102010293', '0301080399', '0202030776', '0307020118', '0401010031', '0309050022', '0307040151',
        '0102010510', '0211080055', '0102010072', '0102010218', '0301080259', '0301080267', '0102010242', '0414020073',
        '0102010323', '0301040036', '0101020040', '0102010226', '0101020015', '0101020023', '0102010528', '0101020082',
        '0102010307', '0102010064', '0101020066', '0101020058', '0404020615', '0307040135', '0401010074', '0414020170',
        '0211020036', '0211020052', '0301100101', '0301080160', '0301100152', '0301100179', '0202010473', '0201020041',
        '0211060275', '0307010040', '0101020031', '0102010340', '0102010501', '0214010015', '0414020359', '0307030059',
        '0307010031', '0202060446', '0204010071'
    ]

    query = text("""
    SELECT s_prd.prd_uid, s_prd.prd_pa, s_prd.prd_cbo, s_prd.prd_cmp, SUM(s_prd.prd_qt_p) as prd_qt_p
    FROM tb_bpa as s_prd
    WHERE s_prd.prd_org = 'BPI' AND s_prd.prd_pa IN :pa_ids
    GROUP BY s_prd.prd_uid, s_prd.prd_pa, s_prd.prd_cbo, s_prd.prd_cmp
    """)
    results = connection.execute(query, {'pa_ids': tuple(pa_ids)})
    for row in results:
        insert_query = text("""
        INSERT INTO tb_bpa (PRD_UID, PRD_CMP, PRD_CNSMED, PRD_CBO, PRD_FLH, PRD_SEQ, PRD_PA, PRD_CNSPAC,
                           PRD_SEXO, PRD_IBGE, PRD_DTATEN, PRD_CID, PRD_IDADE, PRD_QT_P, PRD_CATEN,
                           PRD_NAUT, PRD_ORG)
        VALUES (:prd_uid, :prd_cmp, '', :prd_cbo, '001', '01', :prd_pa, '', '', '', '', '', '000',
                :prd_qt_p, '', '', 'BPA')""")
        connection.execute(insert_query, dict(row._mapping, prd_qt_p=row.prd_qt_p)) # Use ._mapping for dict conversion

    delete_query = text("""
        DELETE FROM tb_bpa
        WHERE prd_org = 'BPI'
          AND (prd_pa LIKE '%ABPG%' OR prd_pa LIKE '%ABEX%' OR prd_pa IN :pa_ids OR prd_pa IN (
              '0101020104', '0101030010', '0214010201', '0301010269', '0309010063', 
              '0301040141', '0301050139', '0301050147', '0301010277', '0309010047',
              '0301100195', '0301100209', '0301100217', '0301100225', '0301100233',
              '0301100241', '0301100276', '0301100284', '0307010155', '0214010082'
          ))
    """)
    connection.execute(delete_query, {'pa_ids': tuple(pa_ids)})
    logger.info("Primeira Procedure Concluída (db_operations)")
    update_uid_query_1 = text("UPDATE tb_bpa SET prd_cid = 'G804' WHERE prd_uid = '0491381'")
    connection.execute(update_uid_query_1)
    logger.info("Update 1 (CID para UID 0491381) executado (db_operations)")
    update_uid_query_2 = text("""
    UPDATE tb_bpa SET prd_cep_pcnte = '07400959', prd_end_pcnte = 'dos Expedicionários',
        prd_num_pcnte = '290', prd_bairro_pcnte = 'Jardim Rincão', prd_ibge = '350390'
    WHERE prd_cep_pcnte = '07400000' or prd_ibge <> '350390'
    """)
    connection.execute(update_uid_query_2)
    logger.info("Update 2 (CEP 07400000) executado (db_operations)")

def executar_procedure_segunda(connection):
    pa_ids = ['0301010110', '0301010030', '0301010056', '0301010064', '0301010137']
    query = text("""
    SELECT s_prd.prd_uid, s_prd.prd_pa, s_prd.prd_cbo, s_prd.prd_cmp, s_prd.prd_idade, SUM(s_prd.prd_qt_p) as prd_qt_p
    FROM tb_bpa as s_prd
    WHERE s_prd.prd_org = 'BPI' AND s_prd.prd_pa IN :pa_ids
    GROUP BY s_prd.prd_uid, s_prd.prd_pa, s_prd.prd_cbo, s_prd.prd_cmp, s_prd.prd_idade
    """)
    results = connection.execute(query, {'pa_ids': tuple(pa_ids)})
    for row in results:
        insert_query = text("""
        INSERT INTO tb_bpa (PRD_UID, PRD_CMP, PRD_CNSMED, PRD_CBO, PRD_FLH, PRD_SEQ, PRD_PA, PRD_CNSPAC,
                           PRD_SEXO, PRD_IBGE, PRD_DTATEN, PRD_CID, PRD_IDADE, PRD_QT_P, PRD_CATEN,
                           PRD_NAUT, PRD_ORG)
        VALUES (:prd_uid, :prd_cmp, '', :prd_cbo, '001', '01', :prd_pa, '', '', '', '', '', :prd_idade,
                :prd_qt_p, '', '', 'BPA')
        """)
        connection.execute(insert_query, dict(row._mapping, prd_qt_p=row.prd_qt_p))
        delete_query = text("DELETE FROM tb_bpa WHERE prd_org = 'BPI' AND prd_pa = :prd_pa")
        connection.execute(delete_query, {'prd_pa': row.prd_pa})
    logger.info("Inserções e exclusões da segunda procedure concluídas (db_operations).")
    update_uid_query_2 = text("""
    UPDATE tb_bpa SET prd_uid = CASE WHEN prd_uid = '0000001' THEN '6896847' WHEN prd_uid = '0491381' THEN '6430163' ELSE prd_uid END
    WHERE prd_uid IN ('0000001', '0491381')
    """)
    connection.execute(update_uid_query_2)
    logger.info("Update 2 (substituição de UIDs) executado (db_operations)")
    update_por_pa = {
        '0302070036': 'T951', '0302010025': 'N319', '0302070010': 'T302', '0302060057': 'S141',
        '0302060030': 'G838', '0302060014': 'I694', '0302050027': 'M255', '0302050019': 'T932',
        '0302040056': 'I988', '0302040030': 'Q048', '0302040021': 'J998', '0302060022': 'I694',
        '0201010046': 'K629'
    }
    for pa, cid in update_por_pa.items():
        individual_update = text("UPDATE tb_bpa SET prd_cid = :cid WHERE prd_pa = :pa")
        connection.execute(individual_update, {'cid': cid, 'pa': pa})
        logger.info(f"Update CID executado para PA {pa} com CID {cid} (db_operations)")
    logger.info("Segunda Procedure Concluída (db_operations)")

def executar_procedure_terceira(connection):
    fol_novo, seq_novo, cnes, max_folha_terceira = 0, 0, '', 0
    query_select = text("""
    SELECT prd_uid, prd_pa, prd_cbo, prd_flh, prd_seq, prd_idade, prd_qt_p, prd_org
    FROM tb_bpa WHERE prd_org = 'BPA'
    ORDER BY prd_uid, prd_pa, prd_cbo, prd_flh, prd_seq
    """)
    results = connection.execute(query_select)
    for row in results:
        seq_novo += 1
        if row.prd_uid != cnes:
            cnes, seq_novo, fol_novo = row.prd_uid, 1, 1
        if seq_novo > 20: seq_novo, fol_novo = 1, fol_novo + 1
        prd_flh_novo, prd_seq_novo = f"{fol_novo:03}", f"{seq_novo:02}"
        update_query = text("""
        UPDATE tb_bpa SET prd_flh = :prd_flh_novo, prd_seq = :prd_seq_novo
        WHERE prd_uid = :prd_uid AND prd_pa = :prd_pa AND prd_cbo = :prd_cbo AND prd_flh = :prd_flh AND
              prd_seq = :prd_seq AND prd_idade = :prd_idade AND prd_qt_p = :prd_qt_p AND prd_org = :prd_org
        """)
        connection.execute(update_query, {**row._asdict(), 'prd_flh_novo': prd_flh_novo, 'prd_seq_novo': prd_seq_novo})
        if fol_novo > max_folha_terceira: max_folha_terceira = fol_novo
    logger.info(f"Terceira Procedure Concluída, Max Folha: {max_folha_terceira} (db_operations)")
    return max_folha_terceira

def executar_procedure_quarta(connection):
    fol_novo, seq_novo, cnes_atual, max_folha_quarta, ultimo_registro = 1, 0, None, 0, None
    query_select = text("""
    SELECT prd_uid, prd_pa, prd_cnsmed, prd_cbo, prd_flh, prd_seq, prd_idade, prd_qt_p, prd_org,
           prd_nmpac, prd_dtnasc, prd_dtaten, prd_cnspac, COALESCE(prd_cid, '') AS prd_cid
    FROM tb_bpa WHERE prd_org = 'BPI'
    ORDER BY prd_uid, prd_cnsmed, prd_cbo, prd_flh, prd_seq
    """)
    results = connection.execute(query_select)
    for row in results:
        if row.prd_uid != cnes_atual:
            cnes_atual, fol_novo, seq_novo = row.prd_uid, 1, 1
        else:
            registro_atual = (row.prd_uid, row.prd_pa, row.prd_cnsmed, row.prd_nmpac, row.prd_dtaten, row.prd_cid)
            if registro_atual != ultimo_registro:
                seq_novo += 1
            ultimo_registro = registro_atual
        if seq_novo > 20: seq_novo, fol_novo = 1, fol_novo + 1
        prd_flh_novo, prd_seq_novo = f"{fol_novo:03}", f"{seq_novo:02}"
        update_query = text("""
        UPDATE tb_bpa SET prd_flh = :prd_flh_novo, prd_seq = :prd_seq_novo
        WHERE prd_uid = :prd_uid AND prd_pa = :prd_pa AND prd_cnsmed = :prd_cnsmed AND
              prd_cbo = :prd_cbo AND prd_flh = :prd_flh AND prd_seq = :prd_seq AND
              prd_idade = :prd_idade AND prd_qt_p = :prd_qt_p AND prd_org = :prd_org AND
              prd_nmpac = :prd_nmpac AND prd_dtnasc = :prd_dtnasc AND prd_dtaten = :prd_dtaten AND
              prd_cnspac = :prd_cnspac AND COALESCE(prd_cid, '') = :prd_cid
        """)
        connection.execute(update_query, {**row._asdict(), 'prd_flh_novo': prd_flh_novo, 'prd_seq_novo': prd_seq_novo})
        if fol_novo > max_folha_quarta: max_folha_quarta = fol_novo
    logger.info(f"Quarta Procedure Concluída, Max Folha: {max_folha_quarta} (db_operations)")
    return max_folha_quarta
