//fronted/src/components/useExtractData.js//
import { useCallback, useEffect } from 'react';
import io from 'socket.io-client';
import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || `http://${window.location.hostname}:5000`;

const useExtractData = (dispatch) => {
  // Função para iniciar a exportação
  const extractData = useCallback((type) => {
    dispatch({ type: 'SET_EXTRACTING', payload: { type, value: true } });
    dispatch({ type: 'SET_ERROR', payload: { type, message: null } }); // Limpa erros anteriores
    dispatch({ type: 'SET_PROGRESS', payload: { type, value: 0 } }); // Inicializa progresso

    const validTypes = ['cadastro', 'domiciliofcd', 'bpa', 'visitas', 'atendimentos', 'iaf', 'pse', 'pse_prof'];

    if (!validTypes.includes(type)) {
      console.error(`Tipo inválido para exportação: ${type}`);
      dispatch({
        type: 'SET_ERROR',
        payload: { type, message: `Tipo de exportação inválido: ${type}` },
      });
      dispatch({ type: 'SET_EXTRACTING', payload: { type, value: false } });
      return;
    }

    // Inicia a exportação chamando a rota genérica
    axios({
      url: `${API_BASE_URL}/export/${type}`,
      method: 'GET',
    })
      .then((response) => {
        if (response.data.status !== 'started') {
          throw new Error(response.data.message || 'Falha ao iniciar a exportação.');
        }
        console.log(`[EXPORT-${type.toUpperCase()}] Exportação iniciada com sucesso.`);
      })
      .catch((error) => {
        console.error(`Erro ao iniciar exportação de ${type}: ${error.message}`);
        dispatch({
          type: 'SET_ERROR',
          payload: { type, message: `Erro ao iniciar exportação: ${error.message}` },
        });
        dispatch({ type: 'SET_EXTRACTING', payload: { type, value: false } });
        dispatch({ type: 'SET_PROGRESS', payload: { type, value: 0 } });
      });
  }, [dispatch]);

  // Configura o Socket.IO para monitorar progresso e baixar o arquivo
  useEffect(() => {
    const socket = io(API_BASE_URL);

    socket.on('progress_update', ({ tipo, progress, error }) => {
      console.log(`[SOCKET] Progresso atualizado para ${tipo}: ${progress}, erro: ${error}`);
      dispatch({ type: 'SET_PROGRESS', payload: { type: tipo, value: progress } });

      if (error) {
        dispatch({
          type: 'SET_ERROR',
          payload: { type: tipo, message: error },
        });
        dispatch({ type: 'SET_EXTRACTING', payload: { type: tipo, value: false } });
      }
    });

    socket.on('start_task', (tipo) => {
      console.log(`[SOCKET] Tarefa iniciada para ${tipo}`);
      dispatch({ type: 'SET_EXTRACTING', payload: { type: tipo, value: true } });
      dispatch({ type: 'SET_PROGRESS', payload: { type: tipo, value: 0 } });
    });

    socket.on('end_task', async (tipo) => {
      console.log(`[SOCKET] Tarefa finalizada para ${tipo}`);
      dispatch({ type: 'SET_EXTRACTING', payload: { type: tipo, value: false } });

      try {
        const response = await fetch(`${API_BASE_URL}/download-exported-file/${tipo}`);
        if (!response.ok) {
          throw new Error('Falha ao baixar o arquivo.');
        }
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', `${tipo}_export.xlsx`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
      } catch (error) {
        console.error(`[EXPORT-${tipo.toUpperCase()}] Erro ao baixar o arquivo:`, error);
        dispatch({
          type: 'SET_ERROR',
          payload: { type: tipo, message: `Erro ao baixar o arquivo: ${error.message}` },
        });
      }
    });

    return () => {
      socket.disconnect();
    };
  }, [dispatch]);

  return extractData;
};

export default useExtractData;