import pandas as pd
import os
from sqlalchemy import create_engine

CAMINHO_FIXO = r"C:\Users\ivantp\Downloads\cep_dne_extraido\Fixo"

# Definir leiaute (largura de colunas) para o arquivo de localidades (DNE_GU_LOCALIDADES.TXT)
# com base no leiaute fornecido:
# Campos relevantes: [Tipo (1), País (2), UF (2), sep (6), Chave Localidade (8), Nome Localidade (72),
# CEP (8), ... , Código IBGE (7), ...]
localidades_widths = [
    1,   # 1: Tipo de Registro (esperado "D")
    2,   # 2: Sigla do País
    2,   # 3: Sigla da UF
    6,   # 4: Separador (espaços)
    8,   # 5: Chave da Localidade no DNE
    72,  # 6: Nome Oficial da Localidade
    8,   # 7: CEP Geral da Localidade (pode estar vazio se codificada por logradouros)
    36,  # 8: Abreviatura da Localidade
    1,   # 9: Tipo da Localidade (Distrito/Município/etc.)
    1,   # 10: Situação da Localidade (codificação por logradouro)
    6,   # 11: Separador
    8,   # 12: Chave de Subordinação (se for distrito, chave do município)
    3,   # 13: Sigla da DR da ECT
    7    # 14: Código IBGE do Município
    # (Campos após código IBGE são ignorados para este contexto)
]
localidades_cols = [
    "tipo_reg", "pais", "uf", "sep1", "chave_localidade",
    "nome_localidade", "cep_localidade", "abreviatura_localidade",
    "tipo_localidade", "situacao", "sep2", "chave_municipio",
    "dr_ect", "codigo_ibge"
]

# Ler o arquivo de localidades completo (contém nome das cidades e códigos IBGE)
localidades_df = pd.read_fwf(
    "C:\\Users\\ivantp\\Downloads\\cep_dne_extraido\\Fixo\\DNE_GU_LOCALIDADES.TXT",
    widths=localidades_widths,
    names=localidades_cols,
    encoding="latin1"
)
# Filtrar apenas registros de dados (Tipo = "D") e selecionar campos chave e IBGE
localidades_df = localidades_df[localidades_df["tipo_reg"] == "D"]
ibge_map = {row["chave_localidade"]: row["codigo_ibge"] for _, row in localidades_df.iterrows()}

# Listar siglas de UF para iterar sobre arquivos de logradouros de cada estado (AC, AL, ..., SP, TO)
ufs = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
       "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

# Definir leiaute base (largura de colunas) para arquivos de logradouros
# Os campos 1 a 21 são comuns a todos os tipos de registro (D, S, N, K, Q).
log_base_widths = [
    1,   # 1: Tipo de Registro
    2,   # 2: Sigla da UF
    6,   # 3: Separador (espaços)
    8,   # 4: Chave da Localidade no DNE
    72,  # 5: Nome oficial da Localidade (cidade)
    5,   # 6: Separador
    8,   # 7: Chave do Bairro inicial no DNE
    72,  # 8: Nome do Bairro inicial do Logradouro
    5,   # 9: Separador
    8,   # 10: Chave do Bairro final no DNE
    72,  # 11: Nome do Bairro final do Logradouro
    26,  # 12: Tipo oficial do Logradouro (ex: Rua, Avenida)
    3,   # 13: Preposição do Logradouro (ex: "de", "do", se houver)
    72,  # 14: Título ou Patente do Logradouro (parte do nome, ex: "Dr", "Pres" se houver)
    6,   # 15: Separador
    8,   # 16: Chave do Logradouro no DNE
    72,  # 17: Nome oficial do Logradouro
    36,  # 18: Abreviatura do Logradouro recomendada
    36,  # 19: Informação adicional do Logradouro
    8,   # 20: CEP do Logradouro (pode estar vazio dependendo do tipo)
    1    # 21: Indicador de Grande Usuário (S/N, indica se há grande usuário no logradouro)
]
# Após a posição 21, o leiaute varia conforme o tipo de registro:
# - Tipo D: campo 22 é "Tipo do Logradouro está ativo" (1 char), e depois 104 chars de espaços.
# - Tipo S: campos 22-25 para faixa numérica e paridade, campo 26 ativo, depois 73 espaços.
# - Tipo N: campo 22 geralmente "Número do lote (ou trecho) no logradouro" (11 chars), 
#          e possivelmente campos subsequentes se complementos existirem.
# - Tipo K: campos para complemento_1 (como "Bloco", "Conjunto"), incluindo CEP se não houver Q.
# - Tipo Q: campos para complemento_2.
# Para simplificar a extração via pandas, leremos somente os campos base comuns e lidaremos 
# com campos adicionais via código (slicing manual) quando necessário.

log_base_cols = [
    "tipo_reg", "uf", "sep1", "chave_localidade",
    "nome_localidade", "sep2", "chave_bairro_ini",
    "bairro_ini", "sep3", "chave_bairro_fim", "bairro_fim",
    "tipo_logradouro", "preposicao", "titulo_logradouro",
    "sep4", "chave_logradouro", "nome_logradouro",
    "abreviatura_log", "info_adicional", "cep", "flag_gu"
]

# Preparar lista final para armazenar registros completos
registros = []

# Funções auxiliares de limpeza/normalização
def limpa_texto(campo):
    """Remove espaços em branco extras e normaliza texto."""
    return campo.strip() if campo else ""

def formata_cep(cep):
    """Normaliza o CEP para 8 dígitos (mantém zeros à esquerda)."""
    cep = cep.strip()
    return cep.zfill(8) if cep else ""

# Percorrer cada arquivo de logradouros de UF
for uf in ufs:
    file_name = f"DNE_GU_{uf}_LOGRADOUROS.TXT"
    with open(os.path.join(CAMINHO_FIXO, file_name), "r", encoding="latin-1") as f:
        current_log = None   # armazenamento do logradouro atual (dados base do tipo D)
        current_n = None     # armazenamento de dados do registro N atual (se houver)
        current_k = None     # armazenamento de dados do registro K atual (se houver)
        
        for line in f:
            # Remover quebras de linha
            line = line.rstrip("\r\n")
            if not line:
                continue  # pular linhas vazias (se existirem)
            tipo = line[0]
            # Ignorar registros de Cabeçalho (tipo "C")
            if tipo == "C":
                continue
            # Extrair campos base comuns (posição 1 a 21) usando os índices de coluna especificados
            # Observação: como as posições são baseadas em 1, ajustar para índices Python (0-based)
            uf_code           = line[1:3]
            chave_localidade  = line[9:17]
            nome_localidade   = line[17:89]
            chave_bairro_ini  = line[94:102]
            bairro_ini        = line[102:174]
            chave_bairro_fim  = line[179:187]
            bairro_fim        = line[187:259]
            tipo_logr         = line[259:285]
            prep_logr         = line[285:288]
            titulo_logr       = line[288:360]
            chave_logradouro  = line[366:374]
            nome_logradouro   = line[374:446]
            abreviatura_log   = line[446:482]
            info_adicional    = line[482:518]
            cep_field         = line[518:526]
            flag_gu           = line[526:527]
            
            # Montar partes do nome do logradouro (tipo + título + nome)
            # Ex: "Rua Doutor Bento" = tipo_logr="Rua", titulo_logr="Doutor", nome_logradouro="Bento"
            tipo_logr = limpa_texto(tipo_logr)
            prep_logr = limpa_texto(prep_logr)
            titulo_logr = limpa_texto(titulo_logr)
            nome_logradouro = limpa_texto(nome_logradouro)
            if titulo_logr:
                # Se há título/patente, incluir antes do nome do logradouro
                nome_completo_log = f"{tipo_logr} {titulo_logr} {nome_logradouro}"
            else:
                nome_completo_log = f"{tipo_logr} {nome_logradouro}"
            # Incluir preposição se existir (ex: "Rua da Conceição")
            if prep_logr:
                # Inserir preposição entre tipo e nome se aplicável
                nome_completo_log = f"{tipo_logr} {prep_logr} {titulo_logr or ''} {nome_logradouro}".replace("  ", " ")
            
            # Normalizar bairros e cidade
            bairro_ini = limpa_texto(bairro_ini)
            bairro_fim = limpa_texto(bairro_fim)
            # Se o logradouro abrange dois bairros (inicial e final diferentes), unir os nomes
            if bairro_ini and bairro_fim and bairro_ini != bairro_fim:
                bairro_nome = f"{bairro_ini} / {bairro_fim}"
            else:
                # Caso contrário, usar o bairro inicial (ou final se inicial vazio)
                bairro_nome = bairro_ini or bairro_fim
            
            cidade_nome = limpa_texto(nome_localidade)
            
            # Recuperar código IBGE do município a partir da chave da localidade
            codigo_ibge = ibge_map.get(chave_localidade, "")
            
            if tipo == "D":
                # Registro D: Dados básicos do logradouro (pode conter CEP se não houver subdivisões)
                current_log = {
                    "uf": uf_code.strip().upper(),
                    "cidade": cidade_nome,
                    "bairro": bairro_nome,
                    "logradouro": nome_completo_log.strip(),
                    "cep": formata_cep(cep_field),
                    "ibge": codigo_ibge
                }
                # Se o CEP estiver preenchido no registro D, é um logradouro com CEP único (sem S/N/K/Q)
                # Podemos adicionar diretamente
                if current_log["cep"]:
                    registros.append(current_log)
                    current_log = None  # não haverá registros complementares nesse caso
            elif tipo == "S":
                # Registro S: Seccionamento - CEP específico para faixa de numeração
                if current_log:
                    # Extrair faixa de numeração e paridade
                    num_ini = limpa_texto(line[527:538])  # Número inicial do trecho (11 chars: 528-538)
                    num_fim = limpa_texto(line[538:549])  # Número final do trecho (11 chars: 539-549)
                    paridade = line[549:550].strip()      # Paridade (P/I/A - 550)
                    cep_especifico = formata_cep(line[518:526])
                    # Formar complemento do logradouro com faixa (ex: "de 1 a 609 - lado Ímpar")
                    complemento_faixa = ""
                    if num_ini:
                        complemento_faixa = f"Numeração {num_ini}"
                        if num_fim:
                            complemento_faixa += f" até {num_fim}"
                        if paridade:
                            # Converter indicador de paridade para texto legível
                            par_dict = {"P": "lado Par", "I": "lado Ímpar", "A": "ambos os lados"}
                            complemento_faixa += f" ({par_dict.get(paridade, paridade)})"
                    # Combinar dados base do logradouro com detalhes da faixa e CEP
                    registro = {
                        "uf": current_log["uf"],
                        "cidade": current_log["cidade"],
                        "bairro": current_log["bairro"],
                        "logradouro": current_log["logradouro"] + (f" {complemento_faixa}" if complemento_faixa else ""),
                        "cep": cep_especifico,
                        "ibge": current_log["ibge"]
                    }
                    registros.append(registro)
                # (Não atualizamos current_log, pois um logradouro pode ter múltiplos S e terminam com um novo D)
            elif tipo == "N":
                # Registro N: Numeração de Lote - CEP por número de lote (especial, ex: Brasília)
                if current_log:
                    cep_especifico = formata_cep(line[518:526])
                    num_lote = limpa_texto(line[527:538])  # Número do lote ou identificação (11 chars)
                    # Montar logradouro com número de lote (ex: "Quadra 409")
                    logradouro_com_lote = current_log["logradouro"]
                    if num_lote:
                        logradouro_com_lote += f" {num_lote}"
                    if cep_especifico:
                        # Se o registro N tem CEP, significa que não haverá complementos K/Q
                        registro = {
                            "uf": current_log["uf"],
                            "cidade": current_log["cidade"],
                            "bairro": current_log["bairro"],
                            "logradouro": logradouro_com_lote,
                            "cep": cep_especifico,
                            "ibge": current_log["ibge"]
                        }
                        registros.append(registro)
                    else:
                        # Se o CEP estiver vazio, este registro N serve de base para registros K subsequentes
                        current_n = {
                            "uf": current_log["uf"],
                            "cidade": current_log["cidade"],
                            "bairro": current_log["bairro"],
                            "logradouro": logradouro_com_lote,
                            "ibge": current_log["ibge"]
                        }
            elif tipo == "K":
                # Registro K: Complemento_1 (ex: Bloco, Conjunto) subordinado a um N
                if current_n:
                    cep_especifico = formata_cep(line[518:526])
                    # Campos de complemento_1:
                    num_lote = limpa_texto(line[527:538])      # Número do lote (deve ser igual ao do N correspondente)
                    nome_comp1 = limpa_texto(line[538:574])    # Nome do Complemento_1 (36 chars, ex: "BLOCO")
                    num_comp1  = limpa_texto(line[574:585])    # Número/Letra do Complemento_1 (ex: "A", "B")
                    # Construir logradouro incluindo complemento_1 (ex: "Quadra 409 Bloco A")
                    logradouro_com_comp1 = current_n["logradouro"]
                    if nome_comp1:
                        logradouro_com_comp1 += f" {nome_comp1}"
                    if num_comp1:
                        logradouro_com_comp1 += f" {num_comp1}"
                    if cep_especifico:
                        # Se K tem CEP, não haverá Q para este complemento
                        registro = {
                            "uf": current_n["uf"],
                            "cidade": current_n["cidade"],
                            "bairro": current_n["bairro"],
                            "logradouro": logradouro_com_comp1,
                            "cep": cep_especifico,
                            "ibge": current_n["ibge"]
                        }
                        registros.append(registro)
                    else:
                        # Se CEP vazio, haverá registros Q (complemento_2) detalhando este complemento_1
                        current_k = {
                            "uf": current_n["uf"],
                            "cidade": current_n["cidade"],
                            "bairro": current_n["bairro"],
                            "logradouro": logradouro_com_comp1,
                            "ibge": current_n["ibge"]
                        }
            elif tipo == "Q":
                # Registro Q: Complemento_2 subordinado a um K (ex: subdivisão adicional)
                if current_k:
                    cep_especifico = formata_cep(line[518:526])
                    # Campos de complemento_2 (estrutura similar ao K após posição 527)
                    nome_comp2 = limpa_texto(line[538:574])  # Nome do Complemento_2 (36 chars)
                    num_comp2  = limpa_texto(line[574:585])  # Número/Letra do Complemento_2 (11 chars)
                    # Montar logradouro incluindo complemento_2 (ex: "Quadra 409 Bloco A Apt 101")
                    logradouro_com_comp2 = current_k["logradouro"]
                    if nome_comp2:
                        logradouro_com_comp2 += f" {nome_comp2}"
                    if num_comp2:
                        logradouro_com_comp2 += f" {num_comp2}"
                    # Registro Q sempre deve ter CEP preenchido (último nível de detalhe)
                    registro = {
                        "uf": current_k["uf"],
                        "cidade": current_k["cidade"],
                        "bairro": current_k["bairro"],
                        "logradouro": logradouro_com_comp2,
                        "cep": cep_especifico,
                        "ibge": current_k["ibge"]
                    }
                    registros.append(registro)
            # Fim do processamento de tipos de linha
        # Fim do loop de linhas do arquivo
# Fim do loop de UFs

# Criar DataFrame final a partir da lista de registros
df_ceps = pd.DataFrame(registros)
# Remover possíveis duplicatas absolutas
df_ceps = df_ceps.drop_duplicates()

# Normalizar campos de texto no DataFrame
df_ceps["uf"] = df_ceps["uf"].str.upper().str.strip()
df_ceps["cidade"] = df_ceps["cidade"].str.title().str.strip()    # Cidade com Maiúsculas Iniciais
df_ceps["bairro"] = df_ceps["bairro"].str.title().str.strip()    # Bairro com Maiúsculas Iniciais
df_ceps["logradouro"] = df_ceps["logradouro"].str.strip()

# Criar conexão com o banco PostgreSQL usando SQLAlchemy (ajuste a URI de conexão conforme necessário)
engine = create_engine("postgresql+psycopg2://postgres:esus@localhost:5432/esus")
# Escrever os dados na tabela 'correios_ceps'
df_ceps.to_sql("correios_ceps", engine, if_exists="replace", index=False)
print("Importação concluída. Total de registros inseridos:", len(df_ceps))
