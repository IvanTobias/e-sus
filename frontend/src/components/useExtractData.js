import { useCallback } from 'react';
import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || `http://${window.location.hostname}:5000`;

const useExtractData = (dispatch, socket) => {
  const extractData = useCallback((type) => {
    dispatch({ type: 'SET_EXTRACTING', payload: { type, value: true } });
    dispatch({ type: 'SET_ERROR', payload: { type, message: null } });
    dispatch({ type: 'SET_PROGRESS', payload: { type, value: 0 } });

    const validTypes = ['cadastro', 'domiciliofcd', 'bpa', 'visitas', 'atendimentos', 'iaf', 'pse', 'pse_prof'];

    if (!validTypes.includes(type)) {
      dispatch({ type: 'SET_ERROR', payload: { type, message: `Tipo de exportação inválido: ${type}` } });
      dispatch({ type: 'SET_EXTRACTING', payload: { type, value: false } });
      return;
    }

    socket.emit('start_export', { task: type });

    axios.get(`${API_BASE_URL}/export/${type}`)
      .then((response) => {
        if (response.data.status !== 'started') {
          throw new Error(response.data.message || 'Falha ao iniciar a exportação.');
        }
      })
      .catch((error) => {
        dispatch({ type: 'SET_ERROR', payload: { type, message: `Erro ao iniciar exportação: ${error.message}` } });
        dispatch({ type: 'SET_EXTRACTING', payload: { type, value: false } });
        dispatch({ type: 'SET_PROGRESS', payload: { type, value: 0 } });
      });
  }, [dispatch, socket]); // 'socket' como dependência

  return extractData;
};

export default useExtractData;