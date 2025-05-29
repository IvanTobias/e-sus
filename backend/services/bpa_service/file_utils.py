import json
import logging
import os # Kept for potential future use, though not strictly necessary for current functions

# Configure logging for this module
logger = logging.getLogger(__name__)
# Assuming basicConfig is called at app startup.

def write_bpa_to_txt(results, filepath):
    # Consider adding error handling for file operations
    try:
        with open(filepath, 'w', buffering=8192) as f:  # Buffer de 8KB
            f.writelines(results)  # Escreve todas as linhas de uma vez
        logger.info(f"Arquivo BPA escrito com sucesso em: {filepath}")
    except IOError as e:
        logger.error(f"Erro ao escrever arquivo BPA em {filepath}: {e}")
        raise # Re-raise the exception so the caller can handle it

def load_db_config(config_path='config.json'):
    # This function assumes 'config.json' is in the current working directory
    # from where the main application is run.
    try:
        with open(config_path, 'r') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        logger.error(f"Arquivo de configuração do banco de dados '{config_path}' não encontrado.")
        raise # Or return a default config / handle error appropriately
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar o arquivo de configuração do banco de dados '{config_path}': {e}")
        raise # Or return a default config / handle error appropriately

def carregar_config_bpa(config_path='bpa_config.json'):
    # This function assumes 'bpa_config.json' is in the current working directory.
    try:
        with open(config_path, 'r') as config_file:
            config_data = json.load(config_file)
            return (
                config_data.get('seq7', ""),
                config_data.get('seq8', ""),
                config_data.get('seq9', ""),
                config_data.get('seq10', ""),
                config_data.get('seq11', "M")  # Valor padrão "M"
            )
    except FileNotFoundError:
        logger.warning(f"Arquivo '{config_path}' não encontrado. Usando valores padrão para seq.")
        return "", "", "", "", "M"
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar o arquivo '{config_path}': {e}")
        # Fallback to defaults in case of decoding error as well
        return "", "", "", "", "M"
