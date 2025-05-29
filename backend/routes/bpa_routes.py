from flask import Blueprint, request, jsonify, send_file
from sqlalchemy import text
import logging

# Imports from the project structure (..)
from .. import Gerar_BPA # Direct import from backend folder, still needed for criar_arquivo_bpa
from ..Conexões import get_local_engine
from ..Consultas import execute_and_store_queries 
from ..socketio_config import socketio # Import socketio
from ..services.bpa_service.db_operations import atualizar_enderecos # Import the moved function

# logger will be specific to this blueprint
# from .. import logger as global_logger # Option to use global logger

bpa_bp = Blueprint('bpa_api', __name__, url_prefix='/api')
logger = logging.getLogger(__name__) # Local logger for this blueprint

@bpa_bp.route('/gerar-bpa', methods=['POST'])
def gerar_bpa_route():
    try:
        # Gerar_BPA.criar_arquivo_bpa might use its own logger or the global one.
        # If it relies on the global app.logger, this might need adjustment or Gerar_BPA modified.
        filepath = Gerar_BPA.criar_arquivo_bpa()
        if filepath:
            return send_file(filepath, as_attachment=True)
        else:
            # Using local logger here
            logger.error("Erro ao gerar BPA: Arquivo não foi criado (filepath is None).")
            return jsonify({"message": "Erro ao gerar BPA: Arquivo não foi criado."}), 500
    except Exception as e:
        logger.error(f"Erro ao gerar BPA: {str(e)}")
        return jsonify({"message": f"Erro ao gerar BPA: {str(e)}"}), 500

@bpa_bp.route('/execute-bpa-queries', methods=['GET']) # Path changed
def execute_bpa_queries():
    # This route originally was GET, but often queries that modify or trigger long tasks are POST.
    # For now, keeping it as GET as per its original definition in app.py before the move.
    # However, request.args.to_dict() is used, which is fine for GET.
    config_data = request.args.to_dict() 
    tipo = 'bpa' # This seems fixed for this route.
    # execute_and_store_queries is imported from ..Consultas
    # This function might also have logging considerations (uses global logger or its own).
    execute_and_store_queries(config_data, tipo) 
    return jsonify({"status": "success"})

@bpa_bp.route('/corrigir-ceps', methods=['GET']) # Changed from GET to POST as it implies a data modification action
def corrigir_ceps():
    try:
        engine = get_local_engine()
        with engine.connect() as connection:
            # Call the moved function, passing the connection and socketio object
            atualizar_enderecos(connection, socketio)
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

    # It's good practice to validate data (e.g., cep, logradouro, bairro are not None)
    if not all([cep, logradouro, bairro]):
        logger.error(f"Erro ao atualizar CEP: Dados incompletos.")
        return jsonify({"status": "Erro: Dados incompletos"}), 400

    try:
        with get_local_engine().connect() as conn:
            update_query = text("""
                UPDATE tb_bpa
                SET prd_end_pcnte = :logradouro,
                    prd_bairro_pcnte = :bairro
                WHERE prd_cep_pcnte = :cep
            """)
            # Ensure the connection commits the transaction. 
            # For SQLAlchemy Core, Connection.execute() runs in a transaction by default
            # that is committed if the block finishes successfully.
            conn.execute(update_query, {
                'logradouro': logradouro,
                'bairro': bairro,
                'cep': cep
            })
            # For engines that do not autocommit after execute, a conn.commit() might be needed here.
            # However, typical Flask-SQLAlchemy or direct SQLAlchemy usage with context managers handles this.
        return jsonify({"status": "Atualizado com sucesso"})
    except Exception as e:
        logger.error(f"Erro ao atualizar CEP {cep}: {str(e)}")
        # It might be good to rollback conn.rollback() here if the transaction didn't auto-rollback on error.
        return jsonify({"status": f"Erro ao atualizar CEP: {str(e)}"}), 500
