# backend/socketio_config.py

from flask_socketio import SocketIO
from init import app  # Importa o app já inicializado de init.py

# Configuração do SocketIO
socketio = SocketIO(app, cors_allowed_origins="*")  # Apenas instancie o objeto uma vez
