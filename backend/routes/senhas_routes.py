# /home/ubuntu/esus_project/backend/routes/senhas_routes.py
from flask import Blueprint, request, jsonify
from sqlalchemy.orm import sessionmaker
from sqlalchemy import desc, func, update
from datetime import datetime
import sys
import os

# Adjust path to import modules from the backend directory
# Assuming this file is in /home/ubuntu/esus_project/backend/routes/
backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, backend_dir)

from models.senha import Senha, StatusSenha, Base
from Conexões import get_local_engine # Use local engine as confirmed
from socketio_config import socketio # Import socketio for real-time updates
from contextlib import contextmanager
import logging

senhas_bp = Blueprint("senhas_bp", __name__)

engine = get_local_engine()
Session = sessionmaker(bind=engine)

# Ensure table is created (will only create if not exists)
# This might fail in sandbox due to auth issues, but necessary for local execution
try:
    Base.metadata.create_all(engine)
    logging.info("Table 'senhas' checked/created.")
except Exception as e:
    logging.warning(f"Could not create/check 'senhas' table automatically: {e}. Ensure it exists in the local DB.")

@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Session rollback due to error: {e}")
        raise
    finally:
        session.close()

# --- Helper Function to get next number ---
def get_next_senha_number(session, tipo_prefix):
    """Calculates the next sequential number for a given prefix (P, G, N)."""
    today = datetime.utcnow().date()
    last_senha = session.query(Senha).filter(
        Senha.numero_senha.like(f"{tipo_prefix}%"),
        func.date(Senha.dt_geracao) == today
    ).order_by(desc(Senha.id)).first()

    if last_senha:
        try:
            # Extract number part after the prefix
            last_num_str = last_senha.numero_senha[len(tipo_prefix):]
            last_num = int(last_num_str)
            next_num = last_num + 1
        except (ValueError, IndexError):
            logging.warning(f"Could not parse number from last senha: {last_senha.numero_senha}. Resetting to 1.")
            next_num = 1 # Fallback if parsing fails
    else:
        next_num = 1

    return f"{tipo_prefix}{next_num:03d}" # Format as P001, N002, etc.

# --- API Routes ---

@senhas_bp.route("/api/senhas", methods=["POST"])
def gerar_senha():
    """Gera uma nova senha."""
    data = request.json
    tipo_atendimento = data.get("tipo_atendimento", "Normal") # Default to Normal
    paciente_nome = data.get("paciente_nome")
    paciente_cpf = data.get("paciente_cpf")
    paciente_cns = data.get("paciente_cns")
    unidade_id = data.get("unidade_id", 1) # Default or get from context/config

    # Determine prefix (P=Prioritário, N=Normal - adjust as needed)
    # Example: Check if tipo_atendimento indicates priority
    is_prioritario = any(keyword in tipo_atendimento.lower() for keyword in ["idoso", "gestante", "pcd", "prioritário", "preferencial"])
    prefix = "P" if is_prioritario else "N"

    try:
        with session_scope() as session:
            numero_senha = get_next_senha_number(session, prefix)
            nova_senha = Senha(
                numero_senha=numero_senha,
                tipo_atendimento=tipo_atendimento,
                paciente_nome=paciente_nome,
                paciente_cpf=paciente_cpf,
                paciente_cns=paciente_cns,
                unidade_id=unidade_id,
                status=StatusSenha.AGUARDANDO
            )
            session.add(nova_senha)
            session.flush() # Get the ID before commit
            senha_dict = nova_senha.to_dict()

        # Emit update to waiting list via Socket.IO
        socketio.emit("nova_senha_fila", senha_dict, namespace="/senhas")
        logging.info(f"Nova senha gerada e emitida: {numero_senha}")
        return jsonify(senha_dict), 201

    except Exception as e:
        logging.error(f"Erro ao gerar senha: {e}")
        return jsonify({"error": "Erro interno ao gerar senha"}), 500

@senhas_bp.route("/api/senhas/chamar", methods=["POST"])
def chamar_proxima_senha():
    """Chama a próxima senha da fila (prioritária ou normal)."""
    data = request.json
    guiche = data.get("guiche")
    profissional_id = data.get("profissional_id") # Optional: associate with professional
    unidade_id = data.get("unidade_id", 1) # Filter by unit if needed

    if not guiche:
        return jsonify({"error": "Guichê/Sala de chamada é obrigatório"}), 400

    try:
        with session_scope() as session:
            # Prioritize calling 'P' tickets first
            proxima_senha = session.query(Senha).filter(
                Senha.status == StatusSenha.AGUARDANDO,
                Senha.unidade_id == unidade_id,
                Senha.numero_senha.like("P%") # Prioritário
            ).order_by(Senha.dt_geracao).first()

            # If no priority tickets, call normal 'N' tickets
            if not proxima_senha:
                proxima_senha = session.query(Senha).filter(
                    Senha.status == StatusSenha.AGUARDANDO,
                    Senha.unidade_id == unidade_id,
                    Senha.numero_senha.like("N%") # Normal
                ).order_by(Senha.dt_geracao).first()

            if proxima_senha:
                proxima_senha.status = StatusSenha.CHAMADO
                proxima_senha.dt_chamada = datetime.utcnow()
                proxima_senha.guiche_chamada = guiche
                proxima_senha.profissional_id = profissional_id
                session.add(proxima_senha)
                session.flush()
                senha_chamada_dict = proxima_senha.to_dict()

                # Emit update to TV panel and waiting list
                socketio.emit("senha_chamada", senha_chamada_dict, namespace="/senhas")
                socketio.emit("atualizacao_fila", namespace="/senhas") # Signal to refresh waiting list
                logging.info(f"Senha chamada: {proxima_senha.numero_senha} para {guiche}")
                return jsonify(senha_chamada_dict), 200
            else:
                logging.info("Nenhuma senha aguardando para chamar.")
                return jsonify({"message": "Nenhuma senha aguardando"}), 404

    except Exception as e:
        logging.error(f"Erro ao chamar senha: {e}")
        return jsonify({"error": "Erro interno ao chamar senha"}), 500

@senhas_bp.route("/api/senhas/<int:senha_id>/status", methods=["PUT"])
def atualizar_status_senha(senha_id):
    """Atualiza o status de uma senha (Ex: Iniciar Atendimento, Finalizar, Cancelar)."""
    data = request.json
    novo_status_str = data.get("status")

    if not novo_status_str:
        return jsonify({"error": "Novo status é obrigatório"}), 400

    try:
        novo_status = StatusSenha(novo_status_str) # Validate status
    except ValueError:
        valid_statuses = [s.value for s in StatusSenha]
        return jsonify({"error": f"Status inválido. Válidos: {valid_statuses}"}), 400

    try:
        with session_scope() as session:
            senha = session.query(Senha).filter(Senha.id == senha_id).first()

            if not senha:
                return jsonify({"error": "Senha não encontrada"}), 404

            update_data = {"status": novo_status}
            current_time = datetime.utcnow()

            if novo_status == StatusSenha.EM_ATENDIMENTO and not senha.dt_inicio_atendimento:
                update_data["dt_inicio_atendimento"] = current_time
            elif novo_status == StatusSenha.FINALIZADO:
                update_data["dt_fim_atendimento"] = current_time
                if not senha.dt_inicio_atendimento: # If jumped straight to finished
                    update_data["dt_inicio_atendimento"] = senha.dt_chamada or current_time
            elif novo_status == StatusSenha.CANCELADO:
                 # Optionally add logic for cancellation reason
                 pass
            elif novo_status == StatusSenha.AGUARDANDO: # Re-queue maybe?
                 update_data["dt_chamada"] = None
                 update_data["guiche_chamada"] = None
                 update_data["dt_inicio_atendimento"] = None
                 update_data["dt_fim_atendimento"] = None
                 update_data["profissional_id"] = None

            # Use SQLAlchemy update for efficiency
            session.execute(
                update(Senha).where(Senha.id == senha_id).values(**update_data)
            )
            session.flush()

            # Fetch updated senha to return and emit
            senha_atualizada = session.query(Senha).filter(Senha.id == senha_id).one()
            senha_dict = senha_atualizada.to_dict()

        # Emit update to relevant parties (TV panel, waiting list, maybe professional dashboard)
        socketio.emit("status_senha_atualizado", senha_dict, namespace="/senhas")
        socketio.emit("atualizacao_fila", namespace="/senhas") # Signal to refresh waiting list
        logging.info(f"Status da senha {senha_id} atualizado para {novo_status.value}")
        return jsonify(senha_dict), 200

    except Exception as e:
        logging.error(f"Erro ao atualizar status da senha {senha_id}: {e}")
        return jsonify({"error": "Erro interno ao atualizar status"}), 500

@senhas_bp.route("/api/senhas/fila", methods=["GET"])
def obter_fila_espera():
    """Retorna a lista de senhas aguardando e chamadas recentemente."""
    unidade_id = request.args.get("unidade_id", default=1, type=int)
    limit = request.args.get("limit", default=20, type=int)

    try:
        with session_scope() as session:
            # Get waiting tickets
            senhas_aguardando = session.query(Senha).filter(
                Senha.status == StatusSenha.AGUARDANDO,
                Senha.unidade_id == unidade_id
            ).order_by(Senha.numero_senha.like("P%").desc(), Senha.dt_geracao).limit(limit).all()

            # Get recently called/in-progress tickets (e.g., last 5)
            senhas_chamadas_recentes = session.query(Senha).filter(
                Senha.status.in_([StatusSenha.CHAMADO, StatusSenha.EM_ATENDIMENTO]),
                Senha.unidade_id == unidade_id
            ).order_by(desc(Senha.dt_chamada)).limit(5).all()

            fila = {
                "aguardando": [s.to_dict() for s in senhas_aguardando],
                "ultimas_chamadas": [s.to_dict() for s in senhas_chamadas_recentes]
            }
            return jsonify(fila), 200

    except Exception as e:
        logging.error(f"Erro ao obter fila de espera: {e}")
        return jsonify({"error": "Erro interno ao obter fila"}), 500

@senhas_bp.route("/api/senhas/painel-tv", methods=["GET"])
def obter_painel_tv():
    """Retorna os dados para o painel de TV (últimas senhas chamadas)."""
    unidade_id = request.args.get("unidade_id", default=1, type=int)
    limit = request.args.get("limit", default=5, type=int) # Number of tickets to show on TV

    try:
        with session_scope() as session:
            # Get last 'limit' called or in-progress tickets for the TV display
            senhas_painel = session.query(Senha).filter(
                Senha.status.in_([StatusSenha.CHAMADO, StatusSenha.EM_ATENDIMENTO]),
                Senha.unidade_id == unidade_id
            ).order_by(desc(Senha.dt_chamada)).limit(limit).all()

            painel_data = [s.to_dict() for s in senhas_painel]
            return jsonify(painel_data), 200

    except Exception as e:
        logging.error(f"Erro ao obter dados do painel de TV: {e}")
        return jsonify({"error": "Erro interno ao obter dados do painel"}), 500

# --- Socket.IO Events (Optional: Direct event handlers if needed) ---
@socketio.on("connect", namespace="/senhas")
def handle_connect():
    logging.info("Client connected to /senhas namespace")

@socketio.on("disconnect", namespace="/senhas")
def handle_disconnect():
    logging.info("Client disconnected from /senhas namespace")

# Add more specific event handlers if direct client-server communication is needed
# beyond broadcasting updates after API calls.

