from flask_socketio import SocketIO
from flask import request
from init import app  # Importa o app já inicializado de init.py
import logging

# Configuração de logging para depuração
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do SocketIO
socketio = SocketIO(
    app,
    async_mode='eventlet',  # Usar eventlet para suporte a WebSocket
    cors_allowed_origins="http://localhost:3000",  # Especificar a origem do frontend
    path='/socket.io',  # Compatível com o frontend (socket.js)
    logger=True,  # Ativar logs para depuração
    engineio_logger=True  # Ativar logs do engine.io para depuração
)

# Dicionário para rastrear o SID do cliente que iniciou cada tarefa
task_clients = {}

# Funções auxiliares para emissão de eventos
def emit_start_task(tipo, sid=None):
    log_message = f"[SOCKET] Emitindo start_task para '{tipo}'"
    if sid:
        log_message += f" para SID: {sid}"
        socketio.emit('start_task', tipo, to=sid)
    else:
        log_message += " (global)"
        socketio.emit('start_task', tipo)
    logger.info(log_message)

def emit_progress(tipo, percentual, sid=None, error=None):
    msg = {'tipo': tipo, 'percentual': percentual}
    log_message = f"[SOCKET] Emitindo progress_update para '{tipo}': {percentual}%"
    if error:
        msg['error'] = error
        log_message += f", erro: {error}"
    if sid:
        log_message += f" para SID: {sid}"
        socketio.emit('progress_update', msg, to=sid)
    else:
        log_message += " (global)"
        socketio.emit('progress_update', msg)
    logger.info(log_message)

def emit_end_task(tipo, sid=None):
    log_message = f"[SOCKET] Emitindo end_task para '{tipo}'"
    if sid:
        log_message += f" para SID: {sid}"
        socketio.emit('end_task', tipo, to=sid)
    else:
        log_message += " (global)"
        socketio.emit('end_task', tipo)
    logger.info(log_message)

# Monitorar conexões WebSocket
@socketio.on('connect', namespace='/')
def handle_connect():
    sid = getattr(request, 'sid', None)
    if sid:
        logger.info(f'[SOCKET] Cliente conectado: sid={sid}')
    else:
        logger.warning('[SOCKET] Cliente conectado, mas SID não encontrado.')

@socketio.on('disconnect', namespace='/')
def handle_disconnect():
    sid = getattr(request, 'sid', None)
    if sid:
        # Remover o SID do cliente das tarefas associadas
        for task_name in list(task_clients.keys()):
            if task_clients.get(task_name) == sid:
                del task_clients[task_name]
        logger.info(f'[SOCKET] Cliente desconectado: sid={sid}')
    else:
        logger.warning('[SOCKET] Cliente desconectado, mas SID não encontrado.')

@socketio.on('start_export', namespace='/')
def handle_start_export(data):
    sid = getattr(request, 'sid', None)
    if not sid:
        logger.error('[SOCKET] Erro: SID não encontrado no evento start_export.')
        return

    task_name = data.get('task')
    if not task_name:
        logger.error(f'[SOCKET] Erro: Nome da tarefa não fornecido no evento start_export para sid={sid}.')
        return

    task_clients[task_name] = sid
    logger.info(f"[SOCKET] Exportação iniciada por sid={sid}: task={task_name}")

