"""
Módulo de rota backend para integração Fiocruz.

Este módulo implementa uma rota Flask para acionar a integração Fiocruz
a partir do frontend, com suporte a Socket.IO para reportar progresso em tempo real.
"""

from flask import Blueprint, request, jsonify
import threading
import time
import os
# Importa a função correta e renomeia para evitar conflito com o log_message local
from fiocruz_integration import run_integration_process, log_message as fiocruz_log_message

# Tente importar o SocketIO, mas continue mesmo se não estiver disponível
try:
    from socketio_config import socketio
    socketio_available = True
except ImportError:
    socketio_available = False
    print("AVISO: flask_socketio não está instalado. O progresso não será reportado em tempo real.")
    print("Para instalar: pip install flask-socketio")

# Referência global para o objeto SocketIO (será definido na função init_socketio)
socketio = None

# Criar Blueprint para as rotas da integração Fiocruz
fiocruz_blueprint = Blueprint('fiocruz', __name__, url_prefix='/api/v1/fiocruz')

def init_socketio(socketio_instance):
    """
    Inicializa a referência global para o objeto SocketIO.
    Deve ser chamado após a criação do objeto SocketIO na aplicação principal.
    
    Args:
        socketio_instance: Instância do SocketIO criada na aplicação principal
    """
    global socketio
    socketio = socketio_instance

def emit_progress(tipo, progress, message=None, error=None):
    """
    Emite evento de progresso via Socket.IO se disponível.
    
    Args:
        tipo: Tipo de integração
        progress: Valor do progresso (0-100)
        message: Mensagem opcional
        error: Erro opcional
    """
    if socketio_available and socketio:
        data = {
            'tipo': tipo,
            'progress': progress
        }
        if message:
            data['message'] = message
        if error:
            data['error'] = error

        socketio.emit('fiocruz_integration_progress', data)


def custom_log_message(message):
    """
    Função personalizada para log que também emite eventos Socket.IO.
    
    Args:
        message: Mensagem de log
    """
    fiocruz_log_message(message)  # Chama a função de log original do fiocruz_integration
    emit_progress('fiocruz_dimensoes', None, message=message)  # Emitir via Socket.IO

@fiocruz_blueprint.route('/execute-integration', methods=['POST'])
def execute_integration():
    """
    Rota para executar a integração Fiocruz.
    
    Parâmetros esperados no corpo da requisição:
    - exportParquet (bool): Se True, exporta os resultados para arquivos Parquet
    - parquetDir (str): Diretório para salvar os arquivos Parquet
    
    Returns:
        JSON com status da operação
    """
    try:
        # Obter parâmetros da requisição
        data = request.json
        export_parquet = data.get('exportParquet', True)
        parquet_dir = data.get('parquetDir', './input')
        
        # Garantir que o diretório exista
        if export_parquet and parquet_dir:
            os.makedirs(parquet_dir, exist_ok=True)
        
        # Carregar configuração do banco externo do arquivo config.json
        from Conexões import load_config
        config_data = load_config()
        
        # Iniciar a integração em uma thread separada para não bloquear a resposta HTTP
        def run_integration():
            try:
                # Importa as funções necessárias de Consultas e Common
                from Common import update_task_status, update_last_import
                from Consultas import emit_start_task, emit_end_task, update_progress_safely

                tipo = data.get("tipo", "pessoas")  # <-- aqui está a correção principal

                tipos_validos = ["pessoas", "hipertensao", "diabetes", "idoso", "crianca"]
                if tipo not in tipos_validos:
                    raise ValueError(f"Tipo inválido: {tipo}")

                emit_start_task('fiocruz_dimensoes')
                update_task_status('fiocruz_dimensoes', 'running')
                update_progress_safely('fiocruz_dimensoes', 0)

                # Executar a integração usando a função execute_fiocruz_integration
                result = run_integration_process(
                    config_data,
                    tipo=tipo,
                    emit_progress=emit_progress,
                    export_parquet=export_parquet,
                    parquet_dir=parquet_dir
                )
                
                # Emitir evento final
                if result:
                    update_progress_safely('fiocruz_dimensoes', 100)
                    update_task_status('fiocruz_dimensoes', 'completed')
                    update_last_import('fiocruz_dimensoes')
                    emit_progress('fiocruz_dimensoes', 100, message="Integração concluída com sucesso!")
                else:
                    update_task_status('fiocruz_dimensoes', 'failed')
                    emit_progress('fiocruz_dimensoes', 0, error="Falha na integração. Verifique os logs para mais detalhes.")
            
            except Exception as e:
                from traceback import format_exc
                update_task_status('fiocruz_dimensoes', 'failed')
                emit_progress('fiocruz_dimensoes', 0, error=f"Erro durante a integração: {str(e)}")
                fiocruz_log_message(f"Erro na integração Fiocruz: {format_exc()}")  # Usa a função de log original
            finally:
                from Consultas import emit_end_task
                emit_end_task('fiocruz_dimensoes')
                print("Tarefa de integração Fiocruz finalizada.")
        
        # Iniciar thread
        integration_thread = threading.Thread(target=run_integration)
        integration_thread.daemon = True
        integration_thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Integração iniciada com sucesso. O progresso será reportado via Socket.IO.'
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Função para registrar o blueprint na aplicação principal
def register_fiocruz_routes(app, socketio_instance=None):
    """
    Registra as rotas da integração Fiocruz na aplicação principal.
    
    Args:
        app: Aplicação Flask
        socketio_instance: Instância do SocketIO (opcional)
    """
    app.register_blueprint(fiocruz_blueprint)
    
    if socketio_instance:
        init_socketio(socketio_instance)
        print("Socket.IO configurado para integração Fiocruz.")
