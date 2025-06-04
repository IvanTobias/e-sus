import os
import traceback
import logging
import pandas as pd

# Assuming these global/shared objects are accessible as direct imports
# If they are part of the app context or a specific module, adjust accordingly.
try:
    from Common import task_status, update_task_status
except ImportError:
    # Fallback or placeholder if Common cannot be directly imported
    # This might happen if services.py is not in the same Python package context as Common.py
    # For now, we'll assume direct import works, but this is a potential refactoring point.
    print("Warning: Could not import from Common. task_status and update_task_status might not work.")
    task_status = {} # Placeholder
    def update_task_status(task_type, status): pass # Placeholder

try:
    from Conexoes import get_local_engine
except ImportError:
    print("Warning: Could not import get_local_engine from Conexoes.")
    def get_local_engine(): return None # Placeholder

try:
    from Consultas import progress, progress_lock, update_progress_safely, log_message
except ImportError:
    print("Warning: Could not import from Consultas. Progress tracking and logging might not work.")
    progress = {} # Placeholder
    progress_lock = None # Placeholder, real lock needed if concurrent access
    if progress_lock is None: # Basic threading lock as a fallback
        import threading
        progress_lock = threading.Lock()

    def update_progress_safely(task_type, value, error=None): pass # Placeholder
    def log_message(msg): print(msg) # Placeholder

try:
    from socketio_config import emit_start_task, emit_progress, emit_end_task
except ImportError:
    print("Warning: Could not import from socketio_config. SocketIO emissions will not work.")
    def emit_start_task(task_type, sid=None): pass # Placeholder
    def emit_progress(task_type, percent, sid=None, error=None): pass # Placeholder
    def emit_end_task(task_type, sid=None): pass # Placeholder


logger = logging.getLogger(__name__)
# Basic logging config if this module is run standalone or before app logging is set up.
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# Helper function for export tasks (moved from task_routes.py)
def export_task(tipo, query, filename, sid):
    """
    Handles the actual logic of exporting data to an Excel file and emitting progress.

    Args:
        tipo (str): The type of export (e.g., 'cadastro', 'bpa').
        query (str): The SQL query to execute for fetching data.
        filename (str): The name of the file to save the Excel data.
        sid (str, optional): The session ID of the client requesting the export, for targeted SocketIO messages.
    """
    try:
        logger.info(f"[EXPORT-{tipo.upper()}] Iniciando exportação para {tipo} para sid={sid}")
        emit_start_task(tipo, sid)
        update_task_status(tipo, "running")

        with progress_lock: # progress_lock imported from Consultas
            progress[tipo] = 0 # progress imported from Consultas
        update_progress_safely(tipo, 0) # from Consultas

        logger.info(f"[EXPORT-{tipo.upper()}] Enviando progresso inicial (0%) para sid={sid}")
        emit_progress(tipo, 0, sid)

        update_progress_safely(tipo, 25)
        logger.info(f"[EXPORT-{tipo.upper()}] Enviando progresso (25%) para sid={sid}")
        emit_progress(tipo, 25, sid)

        engine = get_local_engine() # from Conexoes
        if engine is None:
            raise ConnectionError("Falha ao obter a engine do banco de dados local.")

        logger.info(f"[EXPORT-{tipo.upper()}] Conectado ao banco de dados, executando query: {query}")
        df = pd.read_sql(query, engine)
        logger.info(f"[EXPORT-{tipo.upper()}] Query executada, {len(df)} linhas retornadas")

        update_progress_safely(tipo, 50)
        logger.info(f"[EXPORT-{tipo.upper()}] Enviando progresso (50%) para sid={sid}")
        emit_progress(tipo, 50, sid)

        # Assuming os.getcwd() gives a writable directory accessible by the application
        # In a containerized/production environment, this path should be carefully managed.
        # For example, use a dedicated '/app/exports' or similar volume-mounted directory.
        export_dir = os.path.join(os.getcwd(), "exports") # Example: save in an 'exports' subdirectory
        if not os.path.exists(export_dir):
            os.makedirs(export_dir, exist_ok=True)
        filepath = os.path.join(export_dir, filename)

        logger.info(f"[EXPORT-{tipo.upper()}] Salvando arquivo em {filepath}")
        df.to_excel(filepath, index=False)
        logger.info(f"[EXPORT-{tipo.upper()}] Arquivo salvo com sucesso em {filepath}")

        update_progress_safely(tipo, 100)
        logger.info(f"[EXPORT-{tipo.upper()}] Enviando progresso final (100%) para sid={sid}")
        emit_progress(tipo, 100, sid)

        update_task_status(tipo, "completed")
        log_message(f"Exportação de {tipo} concluída com sucesso. Arquivo: {filepath}") # from Consultas
    except Exception as e:
        error_message = traceback.format_exc()
        log_message(f"Erro na exportação de {tipo}: {error_message}") # from Consultas
        update_task_status(tipo, "failed")
        update_progress_safely(tipo, 0, error=str(e)) # from Consultas
        logger.error(f"[EXPORT-{tipo.upper()}] Erro durante exportação: {str(e)}\n{error_message}")
        emit_progress(tipo, 0, sid, error=str(e))
    finally:
        emit_end_task(tipo, sid)
        logger.info(f"[EXPORT-{tipo.upper()}] Finalizando tarefa para sid={sid}")

# Potentially other service functions could be added here in the future.
