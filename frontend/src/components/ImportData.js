import React, { useReducer, useEffect, useRef, useState } from 'react';
import axios from 'axios';
import { io } from 'socket.io-client';
import Switch from 'react-switch'; // Switch para autoatualização

// Configuração global do axios para usar UTF-8
axios.defaults.headers.common['Content-Type'] = 'application/json; charset=utf-8';

// Função para calcular o ano atual e o mês anterior
const getInitialDateValues = () => {
  const currentDate = new Date();
  const currentYear = currentDate.getFullYear();
  let currentMonth = currentDate.getMonth(); // Mês atual (0 - Janeiro, 11 - Dezembro)

  if (currentMonth === 0) {
    currentMonth = 12;
    return { ano: currentYear - 1, competencia: `${currentMonth}`.padStart(2, '0') };
  }

  return { ano: currentYear, competencia: `${currentMonth}`.padStart(2, '0') };
};

// Estado inicial para o reducer
const initialDateValues = getInitialDateValues();
const initialState = {
  ano: initialDateValues.ano,
  competencia: initialDateValues.competencia,
  progress: { cadastro: 0, domiciliofcd: 0, bpa: 0, visitas: 0 },
  errorMessage: { cadastro: '', domiciliofcd: '', bpa: '', visitas: '' },
  isButtonDisabled: { cadastro: false, domiciliofcd: false, bpa: false, visitas: false },
  isExtracting: { cadastro: false, domiciliofcd: false, bpa: false, visitas: false },
  isRunning: { cadastro: false, domiciliofcd: false, bpa: false, visitas: false },
  isFileAvailable: { cadastro: false, domiciliofcd: false, bpa: false, visitas: false },
  lastImport: { cadastro: '', domiciliofcd: '', bpa: '', visitas: '' }, // Adiciona lastImport ao estado inicial
};

// Funções de ação para o reducer
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
    case 'SET_LAST_IMPORT': // Adiciona um case para definir lastImport
      return { ...state, lastImport: { ...state.lastImport, [action.payload.type]: action.payload.value } };
    case 'SET_COMPETENCIA':
      return { ...state, competencia: action.payload };
    case 'SET_ANO':
      return { ...state, ano: action.payload };
    default:
      return state;
  }
};

function ImportData() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const [isAutoUpdateOn, setIsAutoUpdateOn] = useState(false);
  const [autoUpdateTime, setAutoUpdateTime] = useState('00:00');
  const nonProgressCount = useRef({ cadastro: 0, domiciliofcd: 0, bpa: 0 });

  // Configuração do WebSocket
  useEffect(() => {
    const socket = io('http://127.0.0.1:5000');

    socket.on('progress_update', (data) => {
      dispatch({ type: 'SET_PROGRESS', payload: { type: data.type, value: data.progress } });

      if (data.progress === 100 || data.error) {
        dispatch({ type: 'SET_RUNNING', payload: { type: data.type, value: false } });
        dispatch({ type: 'SET_BUTTON_DISABLED', payload: { type: data.type, value: false } });

        if (data.type === 'bpa') {
          dispatch({ type: 'SET_FILE_AVAILABLE', payload: { type: 'bpa', value: true } });
          localStorage.setItem('isFileAvailable', 'true');
        }
      }
    });

    socket.on('connect_error', (error) => {
      console.error('Erro de conexão com o WebSocket:', error);
    });

    return () => {
      socket.disconnect();
    };
  }, []);

  // Carregar a configuração de autoatualização
  useEffect(() => {
    const loadAutoUpdateConfig = async () => {
      try {
        const response = await axios.get('http://127.0.0.1:5000/api/get-import-config');
        const config = response.data;
        setIsAutoUpdateOn(config.isAutoUpdateOn);
        setAutoUpdateTime(config.autoUpdateTime);
      } catch (error) {
        console.error('Erro ao carregar a configuração de autoatualização:', error);
      }
    };

    loadAutoUpdateConfig();
  }, []); // O array vazio [] garante que isso só rode quando o componente for montado

  // Função para salvar a configuração de autoatualização no backend
  const saveAutoUpdateConfig = () => {
    axios
      .post('http://127.0.0.1:5000/api/save-auto-update-config', {
        isAutoUpdateOn,
        autoUpdateTime,
      })
      .then((response) => alert(response.data.status))
      .catch((error) => console.error('Erro ao salvar configuração de autoatualização:', error));
  };


  useEffect(() => {
    const checkFileAvailability = async (type) => {
      try {
        const response = await axios.get(`http://127.0.0.1:5000/check-file/${type}`);
        dispatch({ type: 'SET_FILE_AVAILABLE', payload: { type, value: response.data.available } });
      } catch (error) {
        console.error(`Erro ao verificar disponibilidade do arquivo para ${type}:`, error);
      }
    };
  
    const syncTypes = ['cadastro', 'domiciliofcd', 'bpa', 'visitas'];
    syncTypes.forEach(type => checkFileAvailability(type));
  }, []);
  
  // Verifica o estado inicial dos botões e lastImport consultando o backend
  useEffect(() => {
    const checkInitialState = async () => {
      const syncTypes = ['cadastro', 'domiciliofcd', 'bpa', 'visitas'];
      for (const type of syncTypes) {
        try {
          const response = await axios.get(`http://127.0.0.1:5000/progress/${type}`);
          const progressValue = response.data.progress;

          if (progressValue > 0 && progressValue < 100) {
            dispatch({ type: 'SET_RUNNING', payload: { type, value: true } });
            dispatch({ type: 'SET_BUTTON_DISABLED', payload: { type, value: true } });
          }

          // Define o lastImport com a data recebida
          dispatch({ type: 'SET_LAST_IMPORT', payload: { type, value: response.data.lastImport || 'N/A' } });

        } catch (error) {
          console.error(`Erro ao verificar progresso e lastImport para ${type}: ${error.message}`);
        }
      }

      // Verifica se o arquivo BPA está disponível para download
      const isFileAvailable = localStorage.getItem('isFileAvailable') === 'true';
      dispatch({ type: 'SET_FILE_AVAILABLE', payload: { type: 'bpa', value: isFileAvailable } });
    };

    checkInitialState();
  }, []);

  // Função para iniciar a importação de dados no frontend
  const importData = (type) => {
    let requestData = {};
    let url = `http://127.0.0.1:5000/execute-queries/${type}`;

    if (type === 'bpa') {
      requestData = { ano: String(state.ano), mes: state.competencia };
    }

    dispatch({ type: 'SET_PROGRESS', payload: { type, value: 0 } });
    dispatch({ type: 'SET_BUTTON_DISABLED', payload: { type, value: true } });
    dispatch({ type: 'SET_RUNNING', payload: { type, value: true } });
    dispatch({ type: 'SET_FILE_AVAILABLE', payload: { type: 'bpa', value: false } });
    localStorage.setItem('isFileAvailable', 'false');
    nonProgressCount.current[type] = 0;

    axios
      .post(url, requestData)
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
  };

  // Função para extração de dados
  const extractData = (type) => {
    dispatch({ type: 'SET_EXTRACTING', payload: { type, value: true } });

    let endpoint = '';
    if (type === 'cadastro') endpoint = 'http://127.0.0.1:5000/export-xls';
    else if (type === 'domiciliofcd') endpoint = 'http://127.0.0.1:5000/export-xls2';
    else if (type === 'bpa') endpoint = 'http://127.0.0.1:5000/export-bpa';
    else if (type === 'visitas') endpoint = 'http://127.0.0.1:5000/export_visitas';

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
  };


  return (
    <div className="config-container">
      <h1>Importar Dados</h1>
      <form>
  
        {/* Seção para Cadastros Individuais */}
        <div>
          <h3>Cadastros Individuais (FCI) 
            {state.lastImport.cadastro && <small> (Última importação: {state.lastImport.cadastro})</small>}
          </h3>
          <div className="flex-container">
            <button
              id="importButton"
              className={`btn btn-primary ${state.isRunning.cadastro ? 'disabled' : ''}`}
              onClick={() => !state.isRunning.cadastro && importData('cadastro')}
              disabled={state.isRunning.cadastro}
            >
              {state.isRunning.cadastro ? 'Processando...' : 'Importar'}
            </button>
            <button
              id="exportButton"
              className={`btn btn-success ${state.isExtracting.cadastro || state.isRunning.cadastro || !state.isFileAvailable.cadastro ? 'disabled' : ''}`}
              onClick={() => extractData('cadastro')}
              disabled={state.isExtracting?.cadastro || state.isRunning?.cadastro}
            >
              Extrair
            </button>
            <div className="progressContainer" style={{ display: (state.progress?.cadastro || 0) > 0 ? 'flex' : 'none', flex: 1 }}>
              <div className="progress">
                <div
                  className="progressBar"
                  role="progressbar"
                  style={{ width: `${state.progress?.cadastro || 0}%` }}
                  aria-valuenow={state.progress?.cadastro || 0}
                  aria-valuemin="0"
                  aria-valuemax="100"
                  aria-live="polite"
                >
                  <span>{state.progress?.cadastro || 0}%</span>
                </div>
              </div>
            </div>
          </div>
          <p className="errorMessage" style={{ display: state.errorMessage?.cadastro ? 'block' : 'none' }}>
            {state.errorMessage?.cadastro || ''}
          </p>
        </div>
  
        {/* Seção para Cadastros Domiciliares */}
        <div>
          <h3>Cadastros Domiciliares  
            {state.lastImport.domiciliofcd && <small> (Última importação: {state.lastImport.domiciliofcd})</small>}
          </h3>
          <div className="flex-container">
            <button
              id="importdomiciliofcdButton"
              className={`btn btn-primary ${state.isRunning.domiciliofcd ? 'disabled' : ''}`}
              onClick={() => !state.isRunning.domiciliofcd && importData('domiciliofcd')}
              disabled={state.isRunning.domiciliofcd}
            >
              {state.isRunning.domiciliofcd ? 'Processando...' : 'Importar'}
            </button>
            <button
              id="exportdomiciliofcdButton"
              className={`btn btn-success ${state.isExtracting.domiciliofcd || state.isRunning.domiciliofcd || !state.isFileAvailable.domiciliofcd ? 'disabled' : ''}`}
              onClick={() => extractData('domiciliofcd')}
              disabled={state.isExtracting.domiciliofcd || state.isRunning.domiciliofcd}
            >
              Extrair
            </button>
            <div className="progressContainer" style={{ display: state.progress.domiciliofcd > 0 ? 'flex' : 'none', flex: 1 }}>
              <div className="progress">
                <div
                  className="progressBar"
                  role="progressbar"
                  style={{ width: `${state.progress.domiciliofcd || 0}%` }}
                  aria-valuenow={state.progress.domiciliofcd || 0}
                  aria-valuemin="0"
                  aria-valuemax="100"
                  aria-live="polite"
                >
                  <span>{state.progress.domiciliofcd || 0}%</span>
                </div>
              </div>
            </div>
          </div>
          <p className="errorMessage" style={{ display: state.errorMessage.domiciliofcd ? 'block' : 'none' }}>
            {state.errorMessage.domiciliofcd}
          </p>
        </div>
        <div>
          <h3>Visitas  
            {state.lastImport.visitas && <small> (Última importação: {state.lastImport.visitas})</small>}
          </h3>
          <div className="flex-container">
            <button
              id="importvisitasButton"
              className={`btn btn-primary ${state.isRunning.visitas ? 'disabled' : ''}`}
              onClick={() => !state.isRunning.visitas && importData('visitas')}
              disabled={state.isRunning.visitas}
            >
              {state.isRunning.visitas ? 'Processando...' : 'Importar'}
            </button>
            <button
              id="exportvisitasButton"
              className={`btn btn-success ${state.isExtracting.visitas || state.isRunning.visitas || state.isFileAvailable.visitas ? 'disabled' : ''}`}
              onClick={() => extractData('visitas')}
              disabled={state.isExtracting.visitas || state.isRunning.visitas}
            >
              Extrair
            </button>
            <div className="progressContainer" style={{ display: state.progress.visitas > 0 ? 'flex' : 'none', flex: 1 }}>
              <div className="progress">
                <div
                  className="progressBar"
                  role="progressbar"
                  style={{ width: `${state.progress.visitas || 0}%` }}
                  aria-valuenow={state.progress.visitas || 0}
                  aria-valuemin="0"
                  aria-valuemax="100"
                  aria-live="polite"
                >
                  <span>{state.progress.visitas || 0}%</span>
                </div>
              </div>
            </div>
          </div>
          <p className="errorMessage" style={{ display: state.errorMessage.visitas ? 'block' : 'none' }}>
            {state.errorMessage.visitas}
          </p>
        </div>
  
        {/* Seção para BPA */}
        <div>
          <h3>BPA 
            {state.lastImport.bpa && <small> (Última importação: {state.lastImport.bpa})</small>}
          </h3>
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
  
          {/* Botões e Progresso */}
          <div className="flex-container">
            <button
              id="importBPAButton"
              className={`btn btn-primary ${state.isRunning.bpa ? 'disabled' : ''}`}
              onClick={() => !state.isRunning.bpa && importData('bpa')}
              disabled={state.isRunning.bpa}
            >
              {state.isRunning.bpa ? 'Processando...' : 'Importar'}
            </button>
            <button
              id="exportBPAButton"
              className={`btn btn-success ${state.isExtracting.bpa || state.isRunning.bpa || state.isFileAvailable.bpa ? 'disabled' : ''}`}
              onClick={() => extractData('bpa')}
              disabled={state.isExtracting.bpa || state.isRunning.bpa}
            >
              Extrair
            </button>
            <div className="progressContainer" style={{ display: state.progress.bpa > 0 ? 'flex' : 'none', flex: 1 }}>
              <div className="progress">
                <div
                  className="progressBar"
                  role="progressbar"
                  style={{ width: `${state.progress.bpa || 0}%` }}
                  aria-valuenow={state.progress.bpa || 0}
                  aria-valuemin="0"
                  aria-valuemax="100"
                  aria-live="polite"
                >
                  <span>{state.progress.bpa || 0}%</span>
                </div>
              </div>
            </div>
          </div>
          <p className="errorMessage" style={{ display: state.errorMessage.bpa ? 'block' : 'none' }}>
            {state.errorMessage.bpa}
          </p>
        </div>
  
        {/* Seção de Atualização Automática */}
        <div>
          <h3>Atualização Automática</h3>
          <Switch
            onChange={() => setIsAutoUpdateOn(!isAutoUpdateOn)}
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
