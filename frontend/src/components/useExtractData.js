// frontend/src/components/useExtractData.js
import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || `http://${window.location.hostname}:5000`;

const useExtractData = (dispatch, socket) => {
  return (tipo) => {
    dispatch({ type: 'SET_EXTRAINDO', payload: { type: tipo, value: true } });
    dispatch({ type: 'SET_PROGRESSO', payload: { type: tipo, value: 0 } });
    dispatch({ type: 'SET_MENSAGEM_ERRO', payload: { type: tipo, message: '' } });

    axios
      .get(`${API_BASE_URL}/export/${tipo}`)
      .then((response) => {
        if (response.data.status !== 'started') {
          throw new Error(response.data.message || 'Erro ao iniciar exportação');
        }

        console.log(`[EXTRACT-${tipo}] Exportação iniciada, aguardando conclusão via WebSocket...`);
        // O socket.on('end_task') cuidará do download automático
      })
      .catch((error) => {
        console.error(`[EXTRACT-${tipo}] Erro ao iniciar exportação:`, error);
        dispatch({ type: 'SET_EXTRAINDO', payload: { type: tipo, value: false } });
        dispatch({
          type: 'SET_MENSAGEM_ERRO',
          payload: { type: tipo, message: `Erro ao iniciar extração: ${error.message}` },
        });
      });
  };
};

export default useExtractData;
