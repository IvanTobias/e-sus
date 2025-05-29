import requests
import time
import logging
from sqlalchemy import text

# Configure logging for this module
# Using __name__ will make log messages clearly state they are from this module.
logger = logging.getLogger(__name__) 
# BasicConfig should ideally be called once at the application entry point.
# If Gerar_BPA.py or the main app.py already calls it, this might not be necessary
# or could be removed if it causes issues with log output.
# For modularity, if this service is used independently, it might need its own config.
# Let's assume for now that basicConfig in app.py or Gerar_BPA.py covers it,
# and this logger will inherit that configuration.
# If specific formatting for this module is needed and not covered by root logger:
# logger.addHandler(logging.NullHandler()) # To prevent "No handler found" if not configured by root
# For now, let's keep it simple and rely on root configuration.

def limpar_logradouro(logradouro):
    if logradouro:
        partes = logradouro.split(" ", 1)
        if len(partes) == 2:
            return partes[1]
    return logradouro

def buscar_dados_opencep(cep):
    url = f'https://opencep.com/v1/{cep}.json'
    try:
        logger.info(f"🔁 Tentando OpenCEP para o CEP: {cep}") # Changed print to logger.info
        response = requests.get(url, timeout=2)
        time.sleep(0.1) # Consider making delay configurable or removing if not strictly needed for rate limiting
        if response.status_code == 200:
            data = response.json()
            if not data.get('erro'): # OpenCEP specific error check
                return {
                    'logradouro': limpar_logradouro(data.get('logradouro', '')),
                    'bairro': data.get('bairro', ''),
                    'cep': cep, # Return the original queried CEP for consistency
                    'ibge': data.get('ibge', '')[:6] if data.get('ibge') else ''
                }
    except requests.exceptions.RequestException as e: # More specific exception handling
        logger.error(f"❌ Erro OpenCEP: {e}")
    return None

def buscar_dados_viacep(cep):
    url = f'https://viacep.com.br/ws/{cep}/json/'
    try:
        logger.info(f"🔁 Tentando ViaCEP para o CEP: {cep}") # Changed print to logger.info
        response = requests.get(url, timeout=2)
        time.sleep(0.5) # Consider configurable delay
        if response.status_code == 200:
            data = response.json()
            if not data.get('erro'): # ViaCEP specific error check
                return {
                    'logradouro': limpar_logradouro(data.get('logradouro', '')),
                    'bairro': data.get('bairro', ''),
                    'cep': data.get('cep', cep).replace('-', ''), # ViaCEP returns formatted CEP
                    'ibge': data.get('ibge', '')[:6] if data.get('ibge') else ''
                }
    except requests.exceptions.RequestException as e: # More specific exception handling
        logger.error(f"❌ Erro ViaCEP: {e}")
    return None

def buscar_dados_apicep(cep_com_traco): # Expects CEP with hyphen, e.g., "XXXXX-XXX"
    url = f'https://cdn.apicep.com/file/apicep/{cep_com_traco}.json'
    try:
        logger.info(f"🔁 Tentando API CEP para o CEP: {cep_com_traco}") # Changed print to logger.info
        response = requests.get(url, timeout=2)
        time.sleep(0.5) # Consider configurable delay
        if response.status_code == 200:
            data = response.json()
            # ApiCEP uses a 'status' field for errors, not 'erro'
            if data.get('status') == 200 and not data.get('error_message'): # Check for actual success
                return {
                    'logradouro': limpar_logradouro(data.get('address', '')),
                    'bairro': data.get('district', ''),
                    'cep': data.get('code', cep_com_traco).replace('-', ''), # ApiCEP returns formatted CEP
                    'ibge': '' # ApiCEP does not provide IBGE directly in this response format
                }
            elif data.get('status') != 200:
                 logger.warning(f"⚠️ API CEP retornou status {data.get('status')} para CEP {cep_com_traco}: {data.get('message')}")
        elif response.status_code == 404: # Specific handling for 404 from ApiCEP
            logger.warning(f"⚠️ API CEP não encontrou o CEP: {cep_com_traco} (status 404)")
        else:
            logger.warning(f"⚠️ API CEP retornou status HTTP {response.status_code} para CEP {cep_com_traco}")

    except requests.exceptions.RequestException as e: # More specific exception handling
        logger.error(f"❌ Erro API CEP: {e}")
    return None

def buscar_por_logradouro_estado_cidade_rua(uf, cidade, logradouro, bairro=None, cod_ibge_prefix=None):
    if not logradouro:
        return [] # Return empty list if logradouro is not provided

    # ViaCEP expects logradouro without type (e.g., "Paulista" not "Avenida Paulista")
    # This was not handled by limpar_logradouro, which removes only the first word if it's a type.
    # For this specific API, more advanced cleaning might be needed or use as is.
    # For now, using logradouro as passed.
    url = f'https://viacep.com.br/ws/{uf}/{cidade}/{logradouro}/json/'
    try:
        logger.info(f"🔁 Tentando busca por logradouro ViaCEP: {logradouro}, Cidade: {cidade}, UF: {uf}")
        response = requests.get(url, timeout=2) # Increased timeout slightly for this query type
        time.sleep(0.5) # Consider configurable delay
        if response.status_code == 200:
            dados = response.json()
            if isinstance(dados, list) and dados: # Ensure it's a list and not empty
                # Filter by bairro if provided
                if bairro:
                    bairro_lower = bairro.lower().strip()
                    dados_filtrados_bairro = [d for d in dados if d.get('bairro', '').lower().strip() == bairro_lower]
                    if dados_filtrados_bairro: dados = dados_filtrados_bairro # Update dados if filter applied and resulted in matches

                # Filter by IBGE prefix if provided and still multiple results or bairro not matched well
                if cod_ibge_prefix and (not bairro or len(dados) > 1) : # Apply if bairro not specific enough or not provided
                    dados_filtrados_ibge = [d for d in dados if d.get('ibge', '').startswith(cod_ibge_prefix)]
                    if dados_filtrados_ibge: dados = dados_filtrados_ibge # Update dados if filter applied

                return dados # Return potentially filtered list
            elif not dados: # Empty list from ViaCEP
                logger.info(f"ℹ️ Busca por logradouro ViaCEP não retornou resultados para: {logradouro}, {cidade}, {uf}")
                return []
            else: # Unexpected response format
                logger.warning(f"⚠️ Resposta inesperada da busca por logradouro ViaCEP: {dados}")
                return [] 
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erro na busca por logradouro ViaCEP: {e}")
    return []


# Note: The connection object must be passed to this function when called.
def buscar_dados_cep(cep, connection): # connection is a required argument
    cep_numerico = ''.join(filter(str.isdigit, str(cep)))
    if len(cep_numerico) != 8:
        logger.warning(f"CEP inválido fornecido: {cep}")
        return None

    # 1ª TENTATIVA: Buscar no banco local correios_ceps pelo CEP diretamente
    if connection:
        try:
            result = connection.execute(
                text("SELECT logradouro, bairro, cep, ibge FROM correios_ceps WHERE cep = :cep LIMIT 1"),
                {"cep": cep_numerico}
            ).fetchone()
            if result:
                logger.info(f"✅ Encontrado no banco Local (correios_ceps) direto: {result._asdict()}")
                return {
                    'logradouro': limpar_logradouro(result.logradouro),
                    'bairro': result.bairro,
                    'cep': result.cep,
                    'ibge': str(result.ibge)[:6] if result.ibge is not None else ''
                }
        except Exception as e:
            logger.error(f"❌ Erro ao buscar no banco correios_ceps pelo CEP {cep_numerico}: {e}")
    else:
        logger.warning("Conexão com banco de dados não fornecida para buscar_dados_cep.")


    # 2ª TENTATIVA: OpenCEP
    info = buscar_dados_opencep(cep_numerico)
    if info: return info

    # 3ª TENTATIVA: Lógica combinada (tb_bpa + busca por logradouro) - Requires connection
    if connection:
        try:
            result_bpa = connection.execute(
                text("SELECT prd_end_pcnte, prd_bairro_pcnte, prd_ibge FROM tb_bpa WHERE prd_cep_pcnte = :cep LIMIT 1"),
                {"cep": cep_numerico}
            ).fetchone()

            if result_bpa:
                logradouro_bpa, bairro_bpa, prd_ibge_bpa = result_bpa.prd_end_pcnte, result_bpa.prd_bairro_pcnte, result_bpa.prd_ibge
                
                if prd_ibge_bpa:
                    ibge_info_local = connection.execute(
                        text("SELECT municipio, uf FROM tb_ibge WHERE ibge LIKE :prefixo LIMIT 1"),
                        {"prefixo": f"{prd_ibge_bpa[:6]}%"} # Ensure prefix is correctly sliced
                    ).fetchone()

                    if ibge_info_local:
                        municipio_local, uf_local = ibge_info_local.municipio, ibge_info_local.uf

                        # TENTATIVA 3A: Buscar no correios_ceps por UF, cidade, logradouro
                        try:
                            # Using ILIKE for case-insensitive and unaccent for accent-insensitive search
                            # Ensure unaccent extension is enabled in PostgreSQL if used.
                            # If not, standard ILIKE or lower() can be used.
                            # For simplicity, using ILIKE here. More robust matching may be needed.
                            query_3a = text("""
                                SELECT logradouro, bairro, cep, ibge
                                FROM correios_ceps
                                WHERE logradouro ILIKE :logradouro 
                                AND ibge = :ibge 
                                AND uf = :uf 
                                LIMIT 1 
                            """) 
                            # The original query had unaccent() which is PostgreSQL specific.
                            # Using ILIKE as a more general approach if unaccent is not available/intended.
                            # For true unaccent matching, it must be on the DB side.
                            candidatos_3a = connection.execute(query_3a, {
                                "logradouro": f"%{logradouro_bpa}%", 
                                "ibge": prd_ibge_bpa[:6], # Ensure correct slicing
                                "uf": uf_local
                            }).fetchone()
                            
                            if candidatos_3a:
                                logger.info(f"✅ Encontrado no banco Local (correios_ceps) por logradouro: {candidatos_3a._asdict()}")
                                return {
                                    'logradouro': limpar_logradouro(candidatos_3a.logradouro),
                                    'bairro': candidatos_3a.bairro,
                                    'cep': candidatos_3a.cep,
                                    'ibge': str(candidatos_3a.ibge)[:6] if candidatos_3a.ibge else ''
                                }
                        except Exception as e:
                            logger.error(f"❌ Erro na busca local (correios_ceps) por logradouro: {e}")
                        
                        # TENTATIVA 3B: ViaCEP por logradouro
                        candidatos_3b = buscar_por_logradouro_estado_cidade_rua(
                            uf=uf_local, cidade=municipio_local, logradouro=logradouro_bpa,
                            bairro=bairro_bpa, cod_ibge_prefix=prd_ibge_bpa[:6] # Ensure correct slicing
                        )
                        if candidatos_3b: # This returns a list
                            escolhido = candidatos_3b[0] # Take the first result
                            novo_cep_3b = escolhido.get('cep', cep_numerico).replace('-', '')
                            if len(novo_cep_3b) != 8: novo_cep_3b = cep_numerico # Fallback if CEP is malformed
                            logger.info(f"✅ Encontrado via ViaCEP por logradouro: {escolhido}")
                            return {
                                'logradouro': limpar_logradouro(escolhido.get('logradouro', '')),
                                'bairro': escolhido.get('bairro', ''),
                                'cep': novo_cep_3b,
                                'ibge': escolhido.get('ibge', '')[:6] if escolhido.get('ibge') else ''
                            }
        except Exception as e:
            logger.error(f"❌ Erro na lógica combinada (tb_bpa + busca externa/local): {e}")

    # 4ª TENTATIVA: ViaCEP (busca normal pelo CEP)
    info_viacep = buscar_dados_viacep(cep_numerico)
    if info_viacep: return info_viacep

    # 5ª TENTATIVA: ApiCep
    # ApiCep expects CEP with hyphen
    info_apicep = buscar_dados_apicep(f"{cep_numerico[:5]}-{cep_numerico[5:]}")
    if info_apicep: return info_apicep
    
    logger.warning(f"CEP {cep_numerico} não encontrado em nenhuma fonte.")
    return None
