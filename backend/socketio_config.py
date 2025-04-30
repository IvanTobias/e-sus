# backend/socketio_config.py

from flask_socketio import SocketIO
from init import app  # Importa o app já inicializado de init.py

# Configuração do SocketIO
socketio = SocketIO(app, cors_allowed_origins="*")  # Apenas instancie o objeto uma vez

# Funções auxiliares para emissão de eventos
def emit_start_task(task_name):
    socketio.emit('start-task', {'task': task_name})

def emit_progress(tipo, percentual):
    socketio.emit('progress_update', {'tipo': tipo, 'percentual': percentual})

def emit_end_task(task_name):
    socketio.emit('end-task', {'task': task_name})
