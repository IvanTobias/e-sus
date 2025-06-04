import os
import json
import logging
from flask import Blueprint, request, jsonify, send_file, send_from_directory
from sqlalchemy import text # text might still be used by Gerar_BPA through corrigir_ceps

import Gerar_BPA
from Gerar_BPA import update_bpa_address_by_cep # Added new function import
from Conexoes import get_local_engine # Changed from Conexões

logger = logging.getLogger(__name__)
bpa_bp = Blueprint('bpa_bp', __name__, url_prefix='/api')

@bpa_bp.route('/gerar-bpa', methods=['POST'])
def gerar_bpa_route():
    try:
        # Gerar_BPA.criar_arquivo_bpa is expected to return a filepath
        filepath = Gerar_BPA.criar_arquivo_bpa()
        if filepath:
            return send_file(filepath, as_attachment=True)
        else:
            logger.error("Erro ao gerar BPA: criar_arquivo_bpa não retornou filepath.")
            return jsonify({"message": "Erro ao gerar BPA: Arquivo não foi criado."}), 500
    except Exception as e:
        logger.error(f"Erro ao gerar BPA: {str(e)}")
        return jsonify({"message": f"Erro ao gerar BPA: {str(e)}"}), 500

# Note: '/execute-queries/bpa' was NOT moved as it's part of the generic execute_queries in task_routes.py
# If it needs to be specialized for BPA, that's a separate refactoring task.

@bpa_bp.route('/save-bpa-config', methods=['POST'])
def save_bpa_config():
    config_data = request.json
    # File path is relative to the CWD (likely backend directory)
    with open('bpa_config.json', 'w') as config_file:
        json.dump(config_data, config_file)
    return jsonify({'status': 'Configuração salva com sucesso'})

@bpa_bp.route('/load-bpa-config', methods=['GET'])
def load_bpa_config():
    try:
        # File path is relative to the CWD
        with open('bpa_config.json', 'r') as config_file:
            config_data = json.load(config_file)
            return jsonify(config_data)
    except FileNotFoundError:
        return jsonify({'error': 'Configuração não encontrada'}), 404

@bpa_bp.route('/list-bpa-files', methods=['GET'])
def list_bpa_files():
    files = []
    try:
        # Lists files in the CWD
        for filename in os.listdir('.'):
            if filename.startswith('bpa_') and filename.endswith('.txt'):
                files.append(filename)
        return jsonify({"files": files})
    except Exception as e:
        logger.error(f"Erro ao listar arquivos BPA: {str(e)}")
        return jsonify({"error": str(e)}), 500

@bpa_bp.route('/delete-bpa-file', methods=['DELETE'])
def delete_bpa_file():
    filename = request.args.get('filename')
    if not filename or not (filename.startswith('bpa_') and filename.endswith('.txt')):
        logger.warning(f"Tentativa de deletar arquivo BPA inválido: {filename}")
        return jsonify({"message": "Nome de arquivo inválido ou não permitido."}), 400
    try:
        # File path is relative to the CWD
        file_path = os.path.join(os.getcwd(), filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            return jsonify({"message": f"Arquivo {filename} deletado com sucesso!"})
        else:
            return jsonify({"message": "Arquivo não encontrado."}), 404
    except Exception as e:
        logger.error(f"Erro ao deletar arquivo BPA {filename}: {str(e)}")
        return jsonify({"message": f"Erro ao deletar arquivo: {str(e)}"}), 500

@bpa_bp.route('/download-bpa-file', methods=['GET'])
def download_bpa_file():
    filename = request.args.get('filename')
    if not filename or not (filename.startswith('bpa_') and filename.endswith('.txt')):
        logger.warning(f"Tentativa de download de arquivo BPA inválido: {filename}")
        return jsonify({"message": "Nome de arquivo inválido ou não permitido."}), 400
    try:
        # send_from_directory expects directory as first arg. CWD is used here.
        return send_from_directory(os.getcwd(), filename, as_attachment=True)
    except Exception as e:
        logger.error(f"Erro ao baixar arquivo BPA {filename}: {str(e)}")
        return jsonify({"message": f"Erro ao baixar o arquivo: {str(e)}"}), 500

@bpa_bp.route('/corrigir-ceps', methods=['GET']) # Changed to GET as per original, though POST/PUT might be more suitable
def corrigir_ceps():
    try:
        engine = get_local_engine()
        with engine.connect() as connection:
            # Assuming Gerar_BPA.atualizar_enderecos handles its own commits/transactions
            Gerar_BPA.atualizar_enderecos(connection)
        return jsonify({"status": "ok"})
    except Exception as e:
        logger.error(f"Erro ao corrigir CEPs: {str(e)}")
        return jsonify({"error": str(e)}), 500

@bpa_bp.route('/atualizar-cep-escolhido', methods=['POST'])
def atualizar_cep_escolhido():
    data = request.json
    cep = data.get('cep')
    logradouro = data.get('logradouro')
    bairro = data.get('bairro')

    if not all([cep, logradouro, bairro]):
        return jsonify({"error": "CEP, Logradouro e Bairro são obrigatórios."}), 400

    success = Gerar_BPA.update_bpa_address_by_cep(cep, logradouro, bairro)

    if success:
        return jsonify({"status": "Atualizado com sucesso"})
    else:
        # The function in Gerar_BPA.py should do its own logging for the specific error.
        # The route can log that an attempt to update failed.
        logger.warning(f"Falha ao atualizar endereço para o CEP {cep} através da função em Gerar_BPA.")
        return jsonify({"status": "Erro ao atualizar endereço. Verifique os logs do servidor."}), 500
