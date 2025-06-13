from flask_socketio import SocketIO, disconnect
from flask import request
from init import app
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

socketio = SocketIO(
    app,
    async_mode='eventlet',
    cors_allowed_origins="http://localhost:3000",
    path='/socket.io',
    logger=False,
    engineio_logger=False
)

task_clients = {}

def emit_start_task(tipo, sid=None):
    log_message = f"[SOCKET] Emitindo start_task para '{tipo}'"
    if sid:
        if not socketio.server.manager.is_connected(sid, '/'):
            logger.warning(f"[SOCKET] Cliente {sid} desconectado — cancelando start_task")
            return
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
        if not socketio.server.manager.is_connected(sid, '/'):
            logger.warning(f"[SOCKET] Cliente {sid} desconectado — cancelando progress_update")
            return
        log_message += f" para SID: {sid}"
        socketio.emit('progress_update', msg, to=sid)
    else:
        log_message += " (global)"
        socketio.emit('progress_update', msg)

    logger.info(log_message)

def emit_end_task(tipo, sid=None, download_url=None):
    payload = {'tipo': tipo}
    if download_url:
        payload['download_url'] = download_url

    log_message = f"[SOCKET] Emitindo end_task para '{tipo}'"
    if sid:
        if not socketio.server.manager.is_connected(sid, '/'):
            logger.warning(f"[SOCKET] Cliente {sid} desconectado — cancelando end_task")
            return
        log_message += f" para SID: {sid}"
        socketio.emit('end_task', payload, to=sid)
    else:
        log_message += " (global)"
        socketio.emit('end_task', payload)

    logger.info(log_message)


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
    if not task_name or task_name not in ['cadastro', 'bpa', 'visitas']:  # coloque os tipos válidos
        logger.warning(f"[SOCKET] Task inválida recebida de sid={sid}: {task_name}")
        socketio.emit('error_message', {'error': 'Tarefa inválida ou não especificada.'}, to=sid)

        # Desconecta o cliente de forma limpa
        disconnect(sid=sid, namespace='/')
        return

    task_clients[task_name] = sid
    logger.info(f"[SOCKET] Exportação iniciada por sid={sid}: task={task_name}")
