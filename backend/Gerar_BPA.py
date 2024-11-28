# backend/Gerar_BPA.py
from socketio_config import socketio  # Importe o socketio do módulo de configuração
from Conexões import get_local_engine, log_message
from sqlalchemy import text
import json
import time

def load_db_config(config_path='config.json'):
    with open(config_path, 'r') as config_file:
        return json.load(config_file)

def consultar_bpa_dados():
    query = text("""Select * FROM tb_bpa""")
    engine = get_local_engine()
    with engine.connect() as connection:
        result = connection.execute(query)
        results = result.fetchall()
    if results:
        #print(f"Consulta retornou {len(results)} linhas.")
        for row in results:
        #print(f"Linha original: {row}")
            return results
    else:
        log_message("A consulta não retornou resultados.")
    return []

def consultar_bpa_dados_ano_mes():
    query = text("""SELECT prd_cmp FROM tb_bpa LIMIT 1""")
    engine = get_local_engine()
    with engine.connect() as connection:
        result = connection.execute(query)
        results = result.fetchone()  # Use fetchone() para obter um único resultado
    if results:
        ano_mes = results[0]  # Supondo que prd_cmp seja a primeira coluna
        print(f"ano_mes encontrado: {ano_mes}")
        return ano_mes
    else:
        print("A consulta não retornou resultados.")
        return None

def format_field(value, length, field_type='ALFA', pad=' ', prd_org='BPI', field_name=''):
    try:
        # Caso o valor seja None ou uma string vazia
        if value is None or value == '':
            value = '' if prd_org == 'BPA' else ' ' * length
        
        if field_type == 'NUM':
            # Especificamente para PRD_FLH e PRD_SEQ, sempre preencha com zeros à esquerda
            if field_name in ['PRD_FLH', 'PRD_SEQ']:
                value = str(value).zfill(length)
                            # Exceção para PRD_NUM_PCNTE: sempre preencha com espaços à esquerda
            elif field_name == 'PRD_NUM_PCNTE':
                value = str(value).rjust(length, ' ')
            else:
                # Verifica se o valor é numérico antes de tentar formatá-lo
                if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
                    if prd_org == 'BPA':
                        if field_name == 'PRD_QT_P':
                            value = str(value).zfill(length)  # Zeros à esquerda para PRD_QT_P em BPA
                        else:
                            value = str(value).ljust(length, pad)  # Espaços à direita para outros campos numéricos em BPA
                    else:  # BPI
                        value = str(value).rjust(length, pad)  # Espaços à esquerda para campos numéricos em BPI
                else:
                    # Se o valor não é numérico, preencha com espaços para evitar erro
                    value = ' ' * length
        else:  # Campos alfanuméricos
            value = str(value).ljust(length, pad)  # Espaços à direita para campos alfanuméricos em ambos

        return value[:length]  # Garante que o valor final tenha exatamente o comprimento especificado
    
    except Exception as e:
        print(f"Erro ao formatar o campo {field_name} com valor {value}: {e}")
        raise
    
def format_bpa_row(row):
    
    prd_org = row.get('PRD_ORG')
    formatted_row = ""

    if prd_org == 'BPA':
        # Formatação específica para BPA
        row_dict = {
            'PRD_IDENT': format_field('02', 2, 'NUM', prd_org=prd_org),
            'PRD_UID': format_field(row.get('PRD_UID'), 7, 'NUM', prd_org=prd_org),
            'PRD_CMP': format_field(row.get('PRD_CMP'), 6, 'NUM', prd_org=prd_org),
            'PRD_CBO': format_field(row.get('PRD_CBO'), 6, 'ALFA', prd_org=prd_org),
            'PRD_FLH': format_field(row.get('PRD_FLH'), 3, 'NUM', prd_org=prd_org),
            'PRD_SEQ': format_field(row.get('PRD_SEQ'), 2, 'NUM', prd_org=prd_org),
            'PRD_PA': format_field(row.get('PRD_PA'), 10, 'NUM', prd_org=prd_org),
            'PRD_IDADE': format_field(row.get('PRD_IDADE'), 3, 'NUM', prd_org=prd_org),
            'PRD_QT_P': format_field(row.get('PRD_QT_P'), 6, 'NUM', prd_org=prd_org, field_name='PRD_QT_P'),
            'PRD_ORG': format_field(row.get('PRD_ORG'), 3, 'ALFA', prd_org=prd_org),
        }
    
    
    # Crie o dicionário com os campos e valores
    else: 
        row_dict = {
        'PRD_IDENT': format_field('03', 2, 'NUM', prd_org=prd_org),
        'PRD_UID': format_field(row.get('PRD_UID'), 7, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_CMP': format_field(row.get('PRD_CMP'), 6, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_CNSMED': format_field(row.get('PRD_CNSMED'), 15, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_CBO': format_field(row.get('PRD_CBO'), 6, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_DTATEN': format_field(row.get('PRD_DTATEN'), 8, 'NUM', prd_org=row.get('PRD_ORG')),        
        'PRD_FLH': format_field(row.get('PRD_FLH'), 3, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_SEQ': format_field(row.get('PRD_SEQ'), 2, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_PA': format_field(row.get('PRD_PA'), 10, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_CNSPAC': format_field(row.get('PRD_CNSPAC'), 15, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_SEXO': format_field(row.get('PRD_SEXO'), 1, 'ALFA', prd_org=row.get('PRD_ORG')),        
        'PRD_IBGE': format_field(row.get('PRD_IBGE'), 6, 'NUM', prd_org=row.get('PRD_ORG')),        
        'PRD_CID': format_field(row.get('PRD_CID'), 4, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_IDADE': format_field(row.get('PRD_IDADE'), 3, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_QT_P': format_field(row.get('PRD_QT_P'), 6, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_CATEN': format_field(row.get('PRD_CATEN'), 2, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_NAUT': format_field(row.get('PRD_NAUT'), 13, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_ORG': format_field(row.get('PRD_ORG'), 3, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_NMPAC': format_field(row.get('PRD_NMPAC'), 30, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_DTNASC': format_field(row.get('PRD_DTNASC'), 8, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_RACA': format_field(row.get('PRD_RACA'), 2, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_ETNIA': format_field(row.get('PRD_ETNIA'), 4, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_NAC': format_field(row.get('PRD_NAC'), 3, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_SERVICO': format_field(row.get('PRD_SERVICO'), 3, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_CLASSIFICACAO': format_field(row.get('PRD_CLASSIFICACAO'), 3, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_EQP_SEQ': format_field(row.get('PRD_EQP_SEQ'), 8, 'NUM', prd_org=row.get('PRD_ORG'), field_name='PRD_EQP_SEQ'),
        'PRD_EQP_AREA': format_field(row.get('PRD_EQP_AREA'), 4, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_CNPJ': format_field(row.get('PRD_CNPJ'), 14, 'NUM', prd_org=row.get('PRD_ORG'), field_name='PRD_CNPJ'),
        'PRD_CEP_PCNTE': format_field(row.get('PRD_CEP_PCNTE'), 8, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_LOGRAD_PCNTE': format_field(row.get('PRD_LOGRAD_PCNTE'), 3, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_END_PCNTE': format_field(row.get('PRD_END_PCNTE'), 30, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_COMPL_PCNTE': format_field(row.get('PRD_COMPL_PCNTE'), 10, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_NUM_PCNTE': format_field(row.get('PRD_NUM_PCNTE'), 5, 'NUM', prd_org=row.get('PRD_ORG'), field_name='PRD_NUM_PCNTE'),
        'PRD_BAIRRO_PCNTE': format_field(row.get('PRD_BAIRRO_PCNTE'), 30, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_DDTEL_PCNTE': format_field(row.get('PRD_DDTEL_PCNTE'), 2, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_TEL_PCNTE': format_field(row.get('PRD_TEL_PCNTE'), 9, 'NUM', prd_org=row.get('PRD_ORG')),
        'PRD_EMAIL_PCNTE': format_field(row.get('PRD_EMAIL_PCNTE'), 40, 'ALFA', prd_org=row.get('PRD_ORG')),
        'PRD_INE': format_field(row.get('PRD_INE'), 10, 'NUM', prd_org=row.get('PRD_ORG'), field_name='PRD_INE'),
        'PRD_CPF_PCNT': format_field(row.get('PRD_CPF_PCNT'), 11, 'NUM', prd_org=row.get('PRD_ORG'), field_name='PRD_CPF_PCNT'),
        'PRD_FIM': '\r\n'  # Corresponde a CR + LF (fim da linha em ASCII)
}

    # Adicione logs para verificar a formatação de cada campo
    for field, formatted_value in row_dict.items():
        if formatted_value is None:
            formatted_value = ''  # Substitua None por string vazia
        #print(f"Campo: {field}, Original: {row.get(field)}, Formatado: {formatted_value}")

    # Concatenar todos os campos em uma única linha formatada
    formatted_row = ''.join(str(value) for value in row_dict.values())
    
    return formatted_row

def write_bpa_to_txt(results, filepath):
    with open(filepath, 'w', buffering=8192) as f:  # Buffer de 8KB
        f.writelines(results)  # Escreve todas as linhas de uma vez

def carregar_config_bpa():
    try:
        with open('bpa_config.json', 'r') as config_file:
            config_data = json.load(config_file)
            return (
                config_data.get('seq7', ""),  # Use get() para acessar os valores do dicionário
                config_data.get('seq8', ""),
                config_data.get('seq9', ""),
                config_data.get('seq10', ""),
                config_data.get('seq11', "M")  # Valor padrão "M"
            )
    except FileNotFoundError:
        return "", "", "", "", "M"
    
seq7, seq8, seq9, seq10, seq11 = carregar_config_bpa()
def gerar_cabecalho_bpa(ano_mes, num_linhas, num_folhas, campo_controle):
    # Cabeçalho fixo
    cbc_hdr = "01"  # Indicador de linha do Header
    inicio_cabecalho = "#BPA#"
    #campo_controle = '1111'

    # Cabeçalho com variáveis
    cbc_mvm = f"{ano_mes:0>6}"  # Ano e mês de Processamento AAAAMM
    cbc_lin = f"{num_linhas:0>6}"  # Número de linhas do BPA gravadas
    cbc_flh = f"{num_folhas:0>6}"  # Quantidade de folhas de BPA gravadas

    # Nome e sigla do órgão responsável
    cbc_rsp = seq7.ljust(30)
    cbc_sgl = seq8.ljust(6)

    # CGC/CPF do prestador ou órgão
    cbc_cgccpf = seq9.zfill(14)

    # Destino e indicador de órgão
    cbc_dst = seq10.ljust(40)
    cbc_dst_in = seq11  # M para Municipal, E para Estadual

    # Versão do sistema
    cbc_versao = "D04.01".ljust(10)

    # Fim do cabeçalho (remova a quebra de linha se houver)
    cbc_fim = ""

    # Monta o cabeçalho completo
    cabecalho = (
        f"{cbc_hdr}{inicio_cabecalho}{cbc_mvm}{cbc_lin}{cbc_flh}"
        f"{campo_controle:0>4}{cbc_rsp}{cbc_sgl}{cbc_cgccpf}{cbc_dst}"
        f"{cbc_dst_in}{cbc_versao}{cbc_fim}"
    )

    return cabecalho

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

        # Cabeçalho BPA
        ano_mes = consultar_bpa_dados_ano_mes()  # Função que obtém o ano/mês de processamento
        total_registros = len(results) + 1  # Quantidade de registros retornados
        cabecalho = gerar_cabecalho_bpa(ano_mes, total_registros, max_folha, campo_controle) + "\n"  # Adiciona quebra de linha ao cabeçalho

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
        '0414020243', '0301100012', '0414020146', '0102010498', '0307040151',
        '0307030032', '0401010066', '0101010036', '0101010010', '0307010031',
        '0404020674', '0404020577', '0101020074', '0307030040', '0307030059',
        '0301060037', '0307020010', '0301060118', '0301060100',
        '0414020383', '0414020405', '0307020070', '0301060029', 
        '0307010015', '0414020120', '0101020090', '0414020138', '0414020359',
        '0301060096', '0101040024', '0102010056', '0201010020',
        '0201010470', '0211070041', '0211070203', '0211070211', '0214010015',
        '0301040079', '0301100039', '0401010023', '0307010023',
        '0301010153', '0414020278', '0205020100', '0309050049',
        '0205020186', '0102010293', '0301080399', '0202030776', '0102010501',
        '0307020118', '0401010031', '0309050022',
        '0102010510', '0211080055', '0102010072', '0102010218',
        '0301080259', '0301080267', '0102010242', '0414020073', '0102010340',
        '0102010323', '0301040036', '0101020040', '0102010226', '0101020031',
        '0101020015', '0101020023', '0102010528', '0101020082', '0307010040',
        '0102010307', '0102010064', '0101020066', '0101020058',
        '0404020615', '0307040135', '0401010074', '0414020170', '0211060275',
        '0211020036', '0211020052', '0301100101', '0301080160',
        '0301100152', '0301100179', '0202010473', '0201020041', '0202060446',
        '0204010071'
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
              '0301100241', '0301100276', '0301100284', '0307010155'
          ))
    """)
    connection.execute(delete_query, {'pa_ids': tuple(pa_ids)})

    log_message("Primeira Procedure Concluída")

def executar_procedure_segunda(connection):
    # IDs dos PA a serem incluídos no WHERE
    pa_ids = [
        '0301010110', '0301010030', '0301010056',
        '0301010064', '0301010072', '0301010137'
    ]

    # 1. Seleção e agrupamento dos dados
    query = text(f"""
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

    # 2. Inserção dos dados agrupados
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

        # 3. Deletar os registros que já foram processados
        delete_query = text("""
        DELETE FROM tb_bpa
        WHERE prd_org = 'BPI'
          AND prd_pa = :prd_pa
        """)
        connection.execute(delete_query, {'prd_pa': row.prd_pa})

    # Atualizações específicas do prd_uid
    update_uid_query_1 = text("""
    UPDATE tb_bpa
    SET prd_cid = 'G804'
    WHERE prd_uid = '0491381'
    """)
    connection.execute(update_uid_query_1)

    update_uid_query_2 = text("""
    UPDATE tb_bpa
    SET prd_uid = '6896847'
    WHERE prd_uid = '0000001'
    """)
    connection.execute(update_uid_query_2)

        # Atualização do prd_cid com base nas condições de prd_pa
    update_cid_query = text("""
    UPDATE tb_bpa
    SET prd_cid = CASE
        WHEN prd_pa = '0302070036' THEN 'T951'
        WHEN prd_pa = '0302010025' THEN 'N319'
        WHEN prd_pa = '0302070010' THEN 'T302'
        WHEN prd_pa = '0302060057' THEN 'S141'
        WHEN prd_pa = '0302060030' THEN 'G838'
        WHEN prd_pa = '0302060014' THEN 'I694'
        WHEN prd_pa = '0302050027' THEN 'M255'
        WHEN prd_pa = '0302050019' THEN 'T932'
        WHEN prd_pa = '0302040056' THEN 'I988'
        WHEN prd_pa = '0302040030' THEN 'Q048'
        WHEN prd_pa = '0302040021' THEN 'J998'
        WHEN prd_pa = '0302060022' THEN 'I694'
        ELSE prd_cid
    END
    WHERE prd_pa IN (
        '0302070036', '0302010025', '0302070010', '0302060057',
        '0302060030', '0302060014', '0302050027', '0302050019',
        '0302040056', '0302040030', '0302040021', '0302060022'
    )
    """)
    connection.execute(update_cid_query)

    # Log para confirmar a conclusão
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
    ORDER BY s_prd.prd_uid, s_prd.prd_pa, s_prd.prd_cbo
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
    ORDER BY s_prd.prd_uid, s_prd.prd_cnsmed, s_prd.prd_cbo, s_prd.prd_flh, s_prd.prd_seq, prd_dtaten
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
    for progresso in range(0, 101, 10):  # Incrementa de 10 em 10
        socketio.emit('progress_update', {'type': 'bpa', 'progress': progresso})
        time.sleep(1)  # Simulação de trabalho real

def update_progress(progress):
    # Emite o evento de progresso para o frontend via Socket.IO
    socketio.emit('progress_update', {'type': 'bpa', 'progress': progress})

