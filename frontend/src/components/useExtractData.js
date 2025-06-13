import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || `http://${window.location.hostname}:5000`;

const useExtractData = (dispatch, socket) => {
  return (tipo) => {
    dispatch({ type: 'SET_EXTRAINDO', payload: { type: tipo, value: true } });
    dispatch({ type: 'SET_PROGRESSO', payload: { type: tipo, value: 0 } });
    dispatch({ type: 'SET_MENSAGEM_ERRO', payload: { type: tipo, message: '' } });

    socket.on('end_task', (data) => {
      const tipoRecebido = data.tipo;
      console.log(`[EXTRACT-${tipoRecebido}] Tarefa finalizada:`, data);

      dispatch({ type: 'SET_EXTRAINDO', payload: { type: tipoRecebido, value: false } });

      if (data && typeof data.download_url === 'string') {
        const fullURL = `${API_BASE_URL}${data.download_url}`;
        window.open(fullURL, '_blank');
      }

      setTimeout(() => {
        socket.close();
      }, 500);
    });



    axios
      .get(`${API_BASE_URL}/export/${tipo}`)
      .then((response) => {
        if (response.data.status !== 'started') {
          throw new Error(response.data.message || 'Erro ao iniciar exportação');
        }

        console.log(`[EXTRACT-${tipo}] Exportação iniciada, aguardando conclusão via WebSocket...`);
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
