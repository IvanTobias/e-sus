import logging

# Configure logging for this module
logger = logging.getLogger(__name__)

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
                        if field_name == 'PRD_QT_P': # Specific case for PRD_QT_P in BPA
                            value = str(value).zfill(length)
                        else: # Other numeric fields in BPA
                            value = str(value).ljust(length, pad)
                    else:  # BPI
                        value = str(value).rjust(length, pad)
                else: # Non-numeric value for a NUM field
                    value = ' ' * length
        else:  # Campos alfanuméricos (field_type == 'ALFA')
            value = str(value).ljust(length, pad)

        return value[:length]
    
    except Exception as e:
        # Use logger for errors
        logger.error(f"Erro ao formatar o campo {field_name} com valor {value}: {e}")
        raise # Re-raise the exception after logging

def format_bpa_row(row): # Depends on format_field
    prd_org = row.get('PRD_ORG')
    # formatted_row = "" # Not needed

    if prd_org == 'BPA':
        row_dict = {
            'PRD_IDENT': format_field('02', 2, 'NUM', prd_org=prd_org),
            'PRD_UID': format_field(row.get('PRD_UID'), 7, 'NUM', prd_org=prd_org),
            'PRD_CMP': format_field(row.get('PRD_CMP'), 6, 'NUM', prd_org=prd_org),
            'PRD_CBO': format_field(row.get('PRD_CBO'), 6, 'ALFA', prd_org=prd_org), # Was NUM in one version, ALFA in another. Assuming ALFA as per original BPA spec.
            'PRD_FLH': format_field(row.get('PRD_FLH'), 3, 'NUM', prd_org=prd_org, field_name='PRD_FLH'),
            'PRD_SEQ': format_field(row.get('PRD_SEQ'), 2, 'NUM', prd_org=prd_org, field_name='PRD_SEQ'),
            'PRD_PA': format_field(row.get('PRD_PA'), 10, 'NUM', prd_org=prd_org),
            'PRD_IDADE': format_field(row.get('PRD_IDADE'), 3, 'NUM', prd_org=prd_org),
            'PRD_QT_P': format_field(row.get('PRD_QT_P'), 6, 'NUM', prd_org=prd_org, field_name='PRD_QT_P'),
            'PRD_ORG': format_field(row.get('PRD_ORG'), 3, 'ALFA', prd_org=prd_org),
        }
    else: # BPI
        row_dict = {
            'PRD_IDENT': format_field('03', 2, 'NUM', prd_org=prd_org),
            'PRD_UID': format_field(row.get('PRD_UID'), 7, 'NUM', prd_org=prd_org),
            'PRD_CMP': format_field(row.get('PRD_CMP'), 6, 'NUM', prd_org=prd_org),
            'PRD_CNSMED': format_field(row.get('PRD_CNSMED'), 15, 'NUM', prd_org=prd_org),
            'PRD_CBO': format_field(row.get('PRD_CBO'), 6, 'NUM', prd_org=prd_org), # Assuming NUM for BPI CBO
            'PRD_DTATEN': format_field(row.get('PRD_DTATEN'), 8, 'NUM', prd_org=prd_org),        
            'PRD_FLH': format_field(row.get('PRD_FLH'), 3, 'NUM', prd_org=prd_org, field_name='PRD_FLH'),
            'PRD_SEQ': format_field(row.get('PRD_SEQ'), 2, 'NUM', prd_org=prd_org, field_name='PRD_SEQ'),
            'PRD_PA': format_field(row.get('PRD_PA'), 10, 'NUM', prd_org=prd_org),
            'PRD_CNSPAC': format_field(row.get('PRD_CNSPAC'), 15, 'NUM', prd_org=prd_org),
            'PRD_SEXO': format_field(row.get('PRD_SEXO'), 1, 'ALFA', prd_org=prd_org),        
            'PRD_IBGE': format_field(row.get('PRD_IBGE'), 6, 'NUM', prd_org=prd_org),        
            'PRD_CID': format_field(row.get('PRD_CID'), 4, 'ALFA', prd_org=prd_org),
            'PRD_IDADE': format_field(row.get('PRD_IDADE'), 3, 'NUM', prd_org=prd_org),
            'PRD_QT_P': format_field(row.get('PRD_QT_P'), 6, 'NUM', prd_org=prd_org, field_name='PRD_QT_P'), # Was not field_name before
            'PRD_CATEN': format_field(row.get('PRD_CATEN'), 2, 'NUM', prd_org=prd_org),
            'PRD_NAUT': format_field(row.get('PRD_NAUT'), 13, 'ALFA', prd_org=prd_org), # Was NUM, ALFA is more typical for AUT
            'PRD_ORG': format_field(row.get('PRD_ORG'), 3, 'ALFA', prd_org=prd_org),
            'PRD_NMPAC': format_field(row.get('PRD_NMPAC'), 30, 'ALFA', prd_org=prd_org),
            'PRD_DTNASC': format_field(row.get('PRD_DTNASC'), 8, 'NUM', prd_org=prd_org),
            'PRD_RACA': format_field(row.get('PRD_RACA'), 2, 'NUM', prd_org=prd_org),
            'PRD_ETNIA': format_field(row.get('PRD_ETNIA'), 4, 'ALFA', prd_org=prd_org), # Was NUM, ALFA seems more plausible
            'PRD_NAC': format_field(row.get('PRD_NAC'), 3, 'NUM', prd_org=prd_org),
            'PRD_SERVICO': format_field(row.get('PRD_SERVICO'), 3, 'ALFA', prd_org=prd_org), # Was NUM
            'PRD_CLASSIFICACAO': format_field(row.get('PRD_CLASSIFICACAO'), 3, 'ALFA', prd_org=prd_org), # Was NUM
            'PRD_EQP_SEQ': format_field(row.get('PRD_EQP_SEQ'), 8, 'NUM', prd_org=prd_org, field_name='PRD_EQP_SEQ'),
            'PRD_EQP_AREA': format_field(row.get('PRD_EQP_AREA'), 4, 'ALFA', prd_org=prd_org),
            'PRD_CNPJ': format_field(row.get('PRD_CNPJ'), 14, 'NUM', prd_org=prd_org, field_name='PRD_CNPJ'),
            'PRD_CEP_PCNTE': format_field(row.get('PRD_CEP_PCNTE'), 8, 'NUM', prd_org=prd_org),
            'PRD_LOGRAD_PCNTE': format_field(row.get('PRD_LOGRAD_PCNTE'), 3, 'ALFA', prd_org=prd_org),
            'PRD_END_PCNTE': format_field(row.get('PRD_END_PCNTE'), 30, 'ALFA', prd_org=prd_org),
            'PRD_COMPL_PCNTE': format_field(row.get('PRD_COMPL_PCNTE'), 10, 'ALFA', prd_org=prd_org),
            'PRD_NUM_PCNTE': format_field(row.get('PRD_NUM_PCNTE'), 5, 'NUM', prd_org=prd_org, field_name='PRD_NUM_PCNTE'),
            'PRD_BAIRRO_PCNTE': format_field(row.get('PRD_BAIRRO_PCNTE'), 30, 'ALFA', prd_org=prd_org),
            'PRD_DDTEL_PCNTE': format_field(row.get('PRD_DDTEL_PCNTE'), 2, 'NUM', prd_org=prd_org),
            'PRD_TEL_PCNTE': format_field(row.get('PRD_TEL_PCNTE'), 9, 'NUM', prd_org=prd_org),
            'PRD_EMAIL_PCNTE': format_field(row.get('PRD_EMAIL_PCNTE'), 40, 'ALFA', prd_org=prd_org),
            'PRD_INE': format_field(row.get('PRD_INE'), 10, 'NUM', prd_org=prd_org, field_name='PRD_INE'),
            'PRD_CPF_PCNT': format_field(row.get('PRD_CPF_PCNT'), 11, 'NUM', prd_org=prd_org, field_name='PRD_CPF_PCNT'),
            'PRD_FIM': '\r\n'
        }
    
    # No need for these logs here as format_field has its own logging for errors.
    # for field, formatted_value in row_dict.items():
    #     if formatted_value is None: formatted_value = ''
    #     #logger.debug(f"Campo: {field}, Original: {row.get(field)}, Formatado: {formatted_value}")

    return ''.join(str(value) for value in row_dict.values())

# seq_config should be a tuple or dict: (seq7, seq8, seq9, seq10, seq11) or {'seq7': ..., ...}
def gerar_cabecalho_bpa(ano_mes, num_linhas, num_folhas, campo_controle, seq_config):
    if isinstance(seq_config, tuple) and len(seq_config) == 5:
        seq7, seq8, seq9, seq10, seq11 = seq_config
    elif isinstance(seq_config, dict):
        seq7 = seq_config.get('seq7', "")
        seq8 = seq_config.get('seq8', "")
        seq9 = seq_config.get('seq9', "")
        seq10 = seq_config.get('seq10', "")
        seq11 = seq_config.get('seq11', "M")
    else:
        logger.error("seq_config passed to gerar_cabecalho_bpa is not a valid tuple or dict.")
        # Fallback to empty strings or default to avoid crashing, or raise error
        seq7, seq8, seq9, seq10, seq11 = "", "", "", "", "M"

    cbc_hdr = "01"
    inicio_cabecalho = "#BPA#"
    cbc_mvm = f"{ano_mes:0>6}"
    cbc_lin = f"{num_linhas:0>6}"
    cbc_flh = f"{num_folhas:0>6}"
    cbc_rsp = seq7.ljust(30)
    cbc_sgl = seq8.ljust(6)
    cbc_cgccpf = seq9.zfill(14) # Assuming seq9 is CPF/CNPJ, should be string of digits
    cbc_dst = seq10.ljust(40)
    cbc_dst_in = seq11
    cbc_versao = "D04.01".ljust(10)
    cbc_fim = "" # Original had this, seems to be an empty placeholder

    cabecalho = (
        f"{cbc_hdr}{inicio_cabecalho}{cbc_mvm}{cbc_lin}{cbc_flh}"
        f"{campo_controle:0>4}{cbc_rsp}{cbc_sgl}{cbc_cgccpf}{cbc_dst}"
        f"{cbc_dst_in}{cbc_versao}{cbc_fim}"
    )
    return cabecalho
