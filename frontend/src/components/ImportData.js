import React, { useReducer, useEffect, useRef, useState, useCallback } from 'react';
import axios from 'axios';
import Switch from 'react-switch';
import socket from '../components/socket';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || `http://${window.location.hostname}:5000`;

axios.defaults.headers.common['Content-Type'] = 'application/json; charset=utf-8';



// Função auxiliar para chamadas de API
const apiCall = async (url, method = 'GET', data = {}) => {
  try {
    const response = await axios({ url: `${API_BASE_URL}${url}`, method, data });
    return response.data;
  } catch (error) {
    console.error(`Erro ao fazer a requisição para ${url}: ${error.message}`);
    throw error;
  }
};

const getInitialDateValues = () => {
  const currentDate = new Date();
  const currentYear = currentDate.getFullYear();
  let currentMonth = currentDate.getMonth();
  if (currentMonth === 0) {
    currentMonth = 12;
    return { ano: currentYear - 1, competencia: `${currentMonth}`.padStart(2, '0') };
  }
  return { ano: currentYear, competencia: `${currentMonth}`.padStart(2, '0') };
};

const initialDateValues = getInitialDateValues();
const initialState = {
  ano: initialDateValues.ano,
  competencia: initialDateValues.competencia,
  progress: { cadastro: 0, domiciliofcd: 0, bpa: 0, visitas: 0, iaf: 0, pse: 0, pse_prof: 0 },
  errorMessage: { cadastro: '', domiciliofcd: '', bpa: '', visitas: '', iaf: '', pse: '', pse_prof: '' },
  isButtonDisabled: { cadastro: false, domiciliofcd: false, bpa: false, visitas: false, iaf: false, pse: false, pse_prof: false },
  isExtracting: { cadastro: false, domiciliofcd: false, bpa: false, visitas: false, iaf: false, pse: false, pse_prof: false },
  isRunning: { cadastro: false, domiciliofcd: false, bpa: false, visitas: false, iaf: false, pse: false, pse_prof: false },
  isFileAvailable: { cadastro: false, domiciliofcd: false, bpa: false, visitas: false, iaf: false, pse: false, pse_prof: false },
  lastImport: { cadastro: '', domiciliofcd: '', bpa: '', visitas: '', iaf: '', pse: '', pse_prof: '' },
};

const reducer = (state, action) => {
  switch (action.type) {
    case 'SET_PROGRESS':
      return { ...state, progress: { ...state.progress, [action.payload.type]: action.payload.value } };
    case 'SET_ERROR':
      return { ...state, errorMessage: { ...state.errorMessage, [action.payload.type]: action.payload.message } };
    case 'SET_BUTTON_DISABLED':
      return { ...state, isButtonDisabled: { ...state.isButtonDisabled, [action.payload.type]: action.payload.value } };
    case 'SET_RUNNING':
      return { ...state, isRunning: { ...state.isRunning, [action.payload.type]: action.payload.value } };
    case 'SET_EXTRACTING':
      return { ...state, isExtracting: { ...state.isExtracting, [action.payload.type]: action.payload.value } };
    case 'SET_FILE_AVAILABLE':
      return { ...state, isFileAvailable: { ...state.isFileAvailable, [action.payload.type]: action.payload.value } };
    case 'SET_LAST_IMPORT':
      return { ...state, lastImport: { ...state.lastImport, [action.payload.type]: action.payload.value } };
    case 'SET_COMPETENCIA':
      return { ...state, competencia: action.payload };
    case 'SET_ANO':
      return { ...state, ano: action.payload };
    default:
      return state;
  }
};

const useWebSocketProgress = (dispatch) => {
  useEffect(() => {
    if (!API_BASE_URL) {
      console.error('API_BASE_URL não definida. Verifique suas variáveis de ambiente.');
      return;
    }

    socket.on('start-task', ({ task }) => {
      console.log(`[SOCKET] start-task: ${task}`);
      dispatch({ type: 'SET_BUTTON_DISABLED', payload: { type: task, value: true } });
      dispatch({ type: 'SET_PROGRESS', payload: { type: task, value: 0 } });
    });

    socket.on('progress_update', ({ tipo, percentual }) => {
      console.log(`[SOCKET] progress_update: ${tipo} - ${percentual}%`);
      dispatch({ type: 'SET_PROGRESS', payload: { type: tipo, value: percentual } });
    
      if (percentual === 100) {
        dispatch({ type: 'SET_BUTTON_DISABLED', payload: { type: tipo, value: false } });
        dispatch({ type: 'SET_RUNNING', payload: { type: tipo, value: false } });
        dispatch({ type: 'SET_FILE_AVAILABLE', payload: { type: tipo, value: true } });
      }
    });
    

    socket.on('end-task', ({ task }) => {
      console.log(`[SOCKET] end-task: ${task}`);
      dispatch({ type: 'SET_BUTTON_DISABLED', payload: { type: task, value: false } });
      dispatch({ type: 'SET_RUNNING', payload: { type: task, value: false }});
      dispatch({ type: 'SET_PROGRESS', payload: { type: task, value: 100 } });
    });

    socket.on('connect_error', (error) => {
      console.error('Erro de conexão com o WebSocket:', error);
    });

    socket.on('disconnect', () => {
      console.log('Desconectado. Tentando reconectar...');
    });

    socket.on('reconnect_attempt', (attempt) => {
      console.log(`Tentativa de reconexão #${attempt}`);
    });

    socket.on('reconnect_failed', () => {
      console.error('Falha ao reconectar após múltiplas tentativas.');
    });

    return () => {
      socket.off('start-task');
      socket.off('progress_update');
      socket.off('end-task');
    };
  }, [dispatch]);
};


// Componente reutilizável para seções de dados
const DataSection = ({ type, title, state, importData, extractData }) => (
  <div>
    <h3>{title}
      {state.lastImport[type] && <small> (Última importação: {state.lastImport[type]})</small>}
    </h3>
    <div className="flex-container">
      <button
        className={`btn btn-primary ${state.isRunning[type] ? 'disabled' : ''}`}
        onClick={() => !state.isRunning[type] && importData(type)}
        disabled={state.isRunning[type]}
      >
        {state.isRunning[type] ? 'Processando...' : 'Importar'}
      </button>
      <button
        className={`btn btn-success ${state.isExtracting[type] || state.isRunning[type] || !state.isFileAvailable[type] ? 'disabled' : ''}`}
        onClick={() => extractData(type)}
        disabled={state.isExtracting[type] || state.isRunning[type]}
      >
        Extrair
      </button>
      <div className="progressContainer" style={{ display: state.isRunning[type] ? 'flex' : 'none', flex: 1 }}>
      <div className="progress">
          <div
            className="progressBar"
            role="progressbar"
            style={{ width: `${state.progress[type] || 0}%` }}
            aria-valuenow={state.progress[type] || 0}
            aria-valuemin="0"
            aria-valuemax="100"
            aria-live="polite"
          >
            <span>{state.progress[type] || 0}%</span>
          </div>
        </div>
      </div>
    </div>
    <p className="errorMessage" style={{ display: state.errorMessage[type] ? 'block' : 'none' }}>
      {state.errorMessage[type]}
    </p>
  </div>
);

function ImportData() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const [isAutoUpdateOn, setIsAutoUpdateOn] = useState(false);
  const [autoUpdateTime, setAutoUpdateTime] = useState('00:00');
  const nonProgressCount = useRef({ cadastro: 0, domiciliofcd: 0, bpa: 0, visitas: 0, iaf: 0, pse: 0, pse_prof: 0 });

  useWebSocketProgress(dispatch); // Usando o hook customizado para WebSocket

  // Carregar a configuração de autoatualização
  useEffect(() => {
    const loadAutoUpdateConfig = async () => {
      try {
        const config = await apiCall('/api/get-import-config');
        setIsAutoUpdateOn(config.isAutoUpdateOn);
        setAutoUpdateTime(config.autoUpdateTime);
      } catch (error) {
        console.error('Erro ao carregar a configuração de autoatualização:', error);
      }
    };

    loadAutoUpdateConfig();
  }, []);

  // Função para salvar a configuração de autoatualização no backend
  const saveAutoUpdateConfig = useCallback(() => {
    apiCall('/api/save-auto-update-config', 'POST', {
      isAutoUpdateOn,
      autoUpdateTime,
    })
      .then(() => alert('Configuração salva com sucesso!'))
      .catch((error) => alert('Erro ao salvar a configuração.'));
  }, [isAutoUpdateOn, autoUpdateTime]);

  // Verificar disponibilidade dos arquivos
  useEffect(() => {
    const checkFileAvailability = async (type) => {
      try {
        const response = await apiCall(`/check-file/${type}`);
        dispatch({ type: 'SET_FILE_AVAILABLE', payload: { type, value: response.available } });
      } catch (error) {
        console.error(`Erro ao verificar disponibilidade do arquivo para ${type}:`, error);
      }
    };

    const syncTypes = ['cadastro', 'domiciliofcd', 'bpa', 'visitas', 'iaf', 'pse', 'pse_prof'];
    syncTypes.forEach(type => checkFileAvailability(type));
  }, []);

  // Buscar dados de última importação
  useEffect(() => {
    const fetchLastImportData = async () => {
      try {
        const config = await apiCall('/configimport');
        
        dispatch({ type: 'SET_LAST_IMPORT', payload: { type: 'cadastro', value: config.cadastro || 'N/A' } });
        dispatch({ type: 'SET_LAST_IMPORT', payload: { type: 'domiciliofcd', value: config.domiciliofcd || 'N/A' } });
        dispatch({ type: 'SET_LAST_IMPORT', payload: { type: 'bpa', value: config.bpa || 'N/A' } });
        dispatch({ type: 'SET_LAST_IMPORT', payload: { type: 'visitas', value: config.visitas || 'N/A' } });
        dispatch({ type: 'SET_LAST_IMPORT', payload: { type: 'iaf', value: config.iaf || 'N/A' } });
        dispatch({ type: 'SET_LAST_IMPORT', payload: { type: 'pse', value: config.pse || 'N/A' } });
        dispatch({ type: 'SET_LAST_IMPORT', payload: { type: 'pse_prof', value: config.pse_prof || 'N/A' } });
  
        const isFileAvailable = localStorage.getItem('isFileAvailable') === 'true';
        dispatch({ type: 'SET_FILE_AVAILABLE', payload: { type: 'bpa', value: isFileAvailable } });
  
      } catch (error) {
        console.error('Erro ao buscar dados de última importação:', error.message);
      }
    };
  
    fetchLastImportData();
  }, []);

  // Função para iniciar a importação de dados
  const importData = useCallback((type) => {
    let requestData = {};
    let url = `/execute-queries/${type}`;

    if (type === 'bpa') {
      requestData = { ano: String(state.ano), mes: state.competencia };
    }

    dispatch({ type: 'SET_PROGRESS', payload: { type, value: 0 } });
    dispatch({ type: 'SET_BUTTON_DISABLED', payload: { type, value: true } });
    dispatch({ type: 'SET_RUNNING', payload: { type, value: true } });
    dispatch({ type: 'SET_FILE_AVAILABLE', payload: { type: 'bpa', value: false } });
    localStorage.setItem('isFileAvailable', 'false');
    nonProgressCount.current[type] = 0;

    apiCall(url, 'POST', requestData)
      .then(() => {
        dispatch({ type: 'SET_ERROR', payload: { type, message: '' } });
        dispatch({ type: 'SET_RUNNING', payload: { type, value: true } });

        if (type === 'bpa') {
          localStorage.setItem('isButtonLocked', 'false');
        }
      })
      .catch((error) => {
        dispatch({
          type: 'SET_ERROR',
          payload: { type, message: `Erro ao importar ${type}: ${error.message}` },
        });
        dispatch({ type: 'SET_BUTTON_DISABLED', payload: { type, value: false } });
        dispatch({ type: 'SET_RUNNING', payload: { type, value: false } });
      });
  }, [state.ano, state.competencia]);

  // Função para extração de dados
  const extractData = useCallback((type) => {
    dispatch({ type: 'SET_EXTRACTING', payload: { type, value: true } });

    let endpoint = '';
    if (type === 'cadastro') endpoint = '/export-xls';
    else if (type === 'domiciliofcd') endpoint = '/export-xls2';
    else if (type === 'bpa') endpoint = '/export-bpa';
    else if (type === 'visitas') endpoint = '/export_visitas';
    else if (type === 'iaf') endpoint = '/export_iaf';  // Endpoint para IAF
    else if (type === 'pse') endpoint = '/export_pse';  // Endpoint para PSE
    else if (type === 'pse_prof') endpoint = '/export_pse_prof';  // Endpoint para PSE Prof
  
    axios({
      url: endpoint,
      method: 'GET',
      responseType: 'blob',
    })
      .then((response) => {
        const url = window.URL.createObjectURL(new Blob([response.data]));
        const link = document.createElement('a');
        link.href = url;

        const contentDisposition = response.headers['content-disposition'];
        const filename = contentDisposition
          ? decodeURIComponent(contentDisposition.split('filename=')[1].replace(/"/g, ''))
          : `${type}_export.xlsx`;

        link.setAttribute('download', filename);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      })
      .catch((error) => {
        console.error(`Erro ao extrair ${type}: ${error.message}`);
        dispatch({
          type: 'SET_ERROR',
          payload: { type, message: `Erro ao extrair ${type}: ${error.message}` },
        });
      })
      .finally(() => {
        dispatch({ type: 'SET_EXTRACTING', payload: { type, value: false } });
      });
  }, []);

  const handleAutoUpdateToggle = useCallback(() => {
    setIsAutoUpdateOn(prevState => !prevState);
    saveAutoUpdateConfig(); // Salvar automaticamente quando o switch é alternado
  }, [isAutoUpdateOn, autoUpdateTime]);

  return (
    <div className="config-container">
      <form>
        <h1>Importar Dados</h1>

        {/* Seções de Dados */}
        <DataSection type="cadastro" title="Cadastros Individuais (FCI)" state={state} importData={importData} extractData={extractData} />
        <DataSection type="domiciliofcd" title="Cadastros Domiciliares" state={state} importData={importData} extractData={extractData} />
        <DataSection type="visitas" title="Visitas" state={state} importData={importData} extractData={extractData} />
        <DataSection type="iaf" title="IAF" state={state} importData={importData} extractData={extractData} />
        <DataSection type="pse" title="PSE" state={state} importData={importData} extractData={extractData} />
        <DataSection type="pse_prof" title="PSE Profissionais" state={state} importData={importData} extractData={extractData} />
        <DataSection type="bpa" title="BPA" state={state} importData={importData} extractData={extractData} />
        
        <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
          <label htmlFor="competencia">Mês:</label>
          <select
            id="competencia"
            name="competencia"
            value={state.competencia}
            onChange={(e) => dispatch({ type: 'SET_COMPETENCIA', payload: e.target.value })}
          >
            {['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
              .map((mes, index) => <option value={String(index + 1).padStart(2, '0')} key={index}>{mes}</option>)}
          </select>

          <label htmlFor="ano">Ano:</label>
          <input
            type="number"
            id="ano"
            name="ano"
            min="2000"
            max="2100"
            value={state.ano}
            onChange={(e) => dispatch({ type: 'SET_ANO', payload: e.target.value })}
          />
        </div>

        {/* Seção de Atualização Automática */}
        <div>
          <h3>Atualização Automática</h3>
          <Switch
            onChange={handleAutoUpdateToggle}
            checked={isAutoUpdateOn}
            offColor="#ccc"
            onColor="#007bff"
            uncheckedIcon={false}
            checkedIcon={false}
          />
          {isAutoUpdateOn && (
            <div>
              <label>Horário para Importação Automática (HH:MM):</label>
              <input
                type="time"
                value={autoUpdateTime}
                onChange={(e) => setAutoUpdateTime(e.target.value)}
              />
            </div>
          )}
          <div>
            <button type="button" onClick={saveAutoUpdateConfig}>
              Salvar Configuração
            </button>
          </div>
        </div>
      </form>
    </div>
  );
}

export default ImportData;
