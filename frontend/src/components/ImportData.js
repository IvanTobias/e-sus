import React, { useReducer, useEffect, useCallback, useState, memo } from 'react';
import axios from 'axios';
import Switch from 'react-switch';
import io from 'socket.io-client';
import useExtractData from './useExtractData';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || `http://${window.location.hostname}:5000`;
axios.defaults.headers.common['Content-Type'] = 'application/json; charset=utf-8';

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
    return { ano: currentYear - 1, mes: `${currentMonth}`.padStart(2, '0') };
  }
  return { ano: currentYear, mes: `${currentMonth}`.padStart(2, '0') };
};

const initialDateValues = getInitialDateValues();
const sections = ['cadastro', 'domiciliofcd', 'bpa', 'visitas', 'iaf', 'pse', 'pse_prof', 'atendimentos'];

const generateStateObject = (defaultValue) => Object.fromEntries(sections.map((key) => [key, defaultValue]));

const initialState = {
  ano: initialDateValues.ano,
  mes: initialDateValues.mes,
  progresso: generateStateObject(0),
  mensagemErro: generateStateObject(''),
  botaoDesabilitado: generateStateObject(false),
  extraindo: generateStateObject(false),
  executando: generateStateObject(false),
  arquivoDisponivel: generateStateObject(false),
  ultimaImportacao: generateStateObject(''),
};

const reducer = (state, action) => {
  switch (action.type) {
    case 'SET_PROGRESSO':
      return { ...state, progresso: { ...state.progresso, [action.payload.type]: action.payload.value } };
    case 'SET_MENSAGEM_ERRO':
      return { ...state, mensagemErro: { ...state.mensagemErro, [action.payload.type]: action.payload.message } };
    case 'SET_BOTAO_DESABILITADO':
      return { ...state, botaoDesabilitado: { ...state.botaoDesabilitado, [action.payload.type]: action.payload.value } };
    case 'SET_EXECUTANDO':
      return { ...state, executando: { ...state.executando, [action.payload.type]: action.payload.value } };
    case 'SET_EXTRAINDO':
      return { ...state, extraindo: { ...state.extraindo, [action.payload.type]: action.payload.value } };
    case 'SET_ARQUIVO_DISPONIVEL':
      return { ...state, arquivoDisponivel: { ...state.arquivoDisponivel, [action.payload.type]: action.payload.value } };
    case 'SET_ULTIMA_IMPORTACAO':
      return { ...state, ultimaImportacao: { ...state.ultimaImportacao, [action.payload.type]: action.payload.value } };
    case 'SET_MES':
      return { ...state, mes: action.payload };
    case 'SET_ANO':
      return { ...state, ano: action.payload };
    default:
      return state;
  }
};

// Componente reutilizável para seções de dados
const DataSection = memo(({ type, title, state, importData, extractData }) => {
  return (
    <div>
      <h3>
        {title}
        {state.ultimaImportacao[type] && <small> (Última importação: {state.ultimaImportacao[type]})</small>}
      </h3>
      <div className="flex-container">
        <button
          className={`btn btn-primary ${state.executando[type] ? 'disabled' : ''}`}
          onClick={() => !state.executando[type] && importData(type)}
          disabled={state.botaoDesabilitado[type]}
        >
          {state.executando[type] ? 'Processando...' : 'Importar'}
        </button>
        <button
          className={`btn btn-success ${state.extraindo[type] || state.executando[type] || !state.arquivoDisponivel[type] ? 'disabled' : ''}`}
          onClick={() => extractData(type)}
          disabled={state.extraindo[type] || state.executando[type] || !state.arquivoDisponivel[type]}
        >
          Extrair
        </button>
        <div className="progressContainer" style={{ display: state.executando[type] || state.extraindo[type] ? 'flex' : 'none', flex: 1 }}>
          <div className="progress">
            <div
              className="progressBar"
              role="progressbar"
              style={{ width: `${state.progresso[type] || 0}%` }}
              aria-valuenow={state.progresso[type] || 0}
              aria-valuemin="0"
              aria-valuemax="100"
            />
            <span aria-live="polite">{state.progresso[type] || 0}%</span>
          </div>
        </div>
      </div>
      <p className="errorMessage" style={{ display: state.mensagemErro[type] ? 'block' : 'none' }}>
        {state.mensagemErro[type]}
      </p>
    </div>
  );
});

function ImportData() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const [isAutoUpdateOn, setIsAutoUpdateOn] = useState(false);
  const [autoUpdateTime, setAutoUpdateTime] = useState('00:00');

  const extractData = useExtractData(dispatch);

  // Inicialização combinada
  useEffect(() => {
    const initialize = async () => {
      try {
        // Configuração de autoatualização
        const config = await apiCall('/api/get-import-config');
        setIsAutoUpdateOn(config.isAutoUpdateOn);
        setAutoUpdateTime(config.autoUpdateTime);

        // Última importação
        const importConfig = await apiCall('/configimport');
        sections.forEach((type) => {
          dispatch({
            type: 'SET_ULTIMA_IMPORTACAO',
            payload: { type, value: importConfig[type] || 'N/A' },
          });
        });

        // Verificação de arquivos e sincronização de progresso/estado
        await Promise.all(
          sections.map(async (type) => {
            try {
              const [fileResponse, progressResponse, taskStatusResponse] = await Promise.all([
                apiCall(`/check-file/${type}`),
                apiCall(`/progress/${type}`),
                apiCall(`/task-status/${type}`).catch(() => ({ status: 'unknown' })),
              ]);

              dispatch({
                type: 'SET_ARQUIVO_DISPONIVEL',
                payload: { type, value: fileResponse.available },
              });

              const progress = Number(progressResponse.progress) || 0;
              dispatch({ type: 'SET_PROGRESSO', payload: { type, value: progress } });

              const validStatuses = ['running', 'idle', 'completed', 'error'];
              let taskStatus = taskStatusResponse.status || 'unknown';
              if (!validStatuses.includes(taskStatus)) {
                console.error(`[SYNC] Status inválido para ${type}: ${taskStatus}`);
                taskStatus = 'unknown';
              }
              const isRunning = taskStatus === 'running' || (progress > 0 && progress < 100);
              dispatch({ type: 'SET_EXECUTANDO', payload: { type, value: isRunning } });
              dispatch({ type: 'SET_BOTAO_DESABILITADO', payload: { type, value: isRunning } });
            } catch (error) {
              console.error(`[INIT] Erro ao inicializar ${type}:`, error);
            }
          })
        );
      } catch (error) {
        console.error('[INIT] Erro na inicialização:', error);
      }
    };

    initialize();
  }, []);

  // WebSocket
  useEffect(() => {
    const socket = io(API_BASE_URL);

    socket.on('connect', () => {
      console.log('[SOCKET] Conectado ao WebSocket');
    });

    socket.on('connect_error', (error) => {
      console.error('[SOCKET] Erro de conexão:', error.message);
    });

    socket.on('reconnect', () => {
      console.log('[SOCKET] Reconectado ao WebSocket');
      // Re-sincronizar estado ao reconectar
      sections.forEach(async (type) => {
        try {
          const progressResponse = await apiCall(`/progress/${type}`);
          const progress = Number(progressResponse.progress) || 0;
          dispatch({ type: 'SET_PROGRESSO', payload: { type, value: progress } });

          const taskStatusResponse = await apiCall(`/task-status/${type}`).catch(() => ({ status: 'unknown' }));
          const validStatuses = ['running', 'idle', 'completed', 'error'];
          let taskStatus = taskStatusResponse.status || 'unknown';
          if (!validStatuses.includes(taskStatus)) {
            console.error(`[SYNC] Status inválido para ${type}: ${taskStatus}`);
            taskStatus = 'unknown';
          }
          const isRunning = taskStatus === 'running' || (progress > 0 && progress < 100);
          dispatch({ type: 'SET_EXECUTANDO', payload: { type, value: isRunning } });
          dispatch({ type: 'SET_BOTAO_DESABILITADO', payload: { type, value: isRunning } });
          dispatch({ type: 'SET_ARQUIVO_DISPONIVEL', payload: { type, value: progress === 100 } });
        } catch (error) {
          console.error(`[RECONNECT] Erro ao sincronizar ${type}:`, error);
        }
      });
    });

    socket.on('start-task', ({ task }) => {
      console.log(`[SOCKET] start-task recebido: task=${task}`);
      dispatch({ type: 'SET_BOTAO_DESABILITADO', payload: { type: task, value: true } });
      dispatch({ type: 'SET_EXECUTANDO', payload: { type: task, value: true } });
    });

    socket.on('progress_update', ({ tipo, percentual, error }) => {
      if (!state.executando[tipo] && percentual !== 100) return;
      const progressValue = Number(percentual);
      console.log(`[SOCKET] progress_update recebido: tipo=${tipo}, percentual=${progressValue}, error=${error}`);

      if (progressValue >= state.progresso[tipo] || progressValue === 0) {
        dispatch({ type: 'SET_PROGRESSO', payload: { type: tipo, value: progressValue } });
      }

      if (error) {
        dispatch({ type: 'SET_MENSAGEM_ERRO', payload: { type: tipo, message: error } });
        dispatch({ type: 'SET_BOTAO_DESABILITADO', payload: { type: tipo, value: false } });
        dispatch({ type: 'SET_EXECUTANDO', payload: { type: tipo, value: false } });
        dispatch({ type: 'SET_EXTRAINDO', payload: { type: tipo, value: false } });
      } else {
        const isRunning = progressValue > 0 && progressValue < 100;
        dispatch({ type: 'SET_EXECUTANDO', payload: { type: tipo, value: isRunning } });
        dispatch({ type: 'SET_BOTAO_DESABILITADO', payload: { type: tipo, value: isRunning } });

        if (progressValue === 100) {
          dispatch({ type: 'SET_ARQUIVO_DISPONIVEL', payload: { type: tipo, value: true } });
          const now = new Date();
          const formattedDate = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')} ${now.getDate().toString().padStart(2, '0')}-${(now.getMonth() + 1).toString().padStart(2, '0')}-${now.getFullYear()}`;
          dispatch({ type: 'SET_ULTIMA_IMPORTACAO', payload: { type: tipo, value: formattedDate } });
        }
      }
    });

    socket.on('end-task', ({ task }) => {
      console.log(`[SOCKET] end-task recebido: task=${task}`);
      dispatch({ type: 'SET_BOTAO_DESABILITADO', payload: { type: task, value: false } });
      dispatch({ type: 'SET_EXECUTANDO', payload: { type: task, value: false } });
      dispatch({ type: 'SET_PROGRESSO', payload: { type: task, value: 100 } });
      dispatch({ type: 'SET_ARQUIVO_DISPONIVEL', payload: { type: task, value: true } });
    });

    return () => {
      socket.off('connect');
      socket.off('connect_error');
      socket.off('reconnect');
      socket.off('start-task');
      socket.off('progress_update');
      socket.off('end-task');
      socket.disconnect();
    };
  }, [state.executando, state.progresso]);

  const getRequestConfig = (type, state) => {
    if (type === 'bpa') {
      return {
        url: `/execute-queries/${type}`,
        method: 'POST',
        data: { ano: String(state.ano), mes: state.mes },
      };
    }
    return { url: `/execute-queries/${type}`, method: 'POST', data: {} };
  };

  const importData = useCallback((type) => {
    const { url, method, data } = getRequestConfig(type, state);
    dispatch({ type: 'SET_PROGRESSO', payload: { type, value: 0 } });
    dispatch({ type: 'SET_BOTAO_DESABILITADO', payload: { type, value: true } });
    dispatch({ type: 'SET_EXECUTANDO', payload: { type, value: true } });
    dispatch({ type: 'SET_ARQUIVO_DISPONIVEL', payload: { type, value: false } });
    dispatch({ type: 'SET_MENSAGEM_ERRO', payload: { type, message: '' } });

    apiCall(url, method, data)
      .then(() => {
        dispatch({ type: 'SET_EXECUTANDO', payload: { type, value: true } });
      })
      .catch((error) => {
        dispatch({
          type: 'SET_MENSAGEM_ERRO',
          payload: { type, message: `Erro ao importar ${type}: ${error.message}` },
        });
        dispatch({ type: 'SET_BOTAO_DESABILITADO', payload: { type, value: false } });
        dispatch({ type: 'SET_EXECUTANDO', payload: { type, value: false } });
      });
  }, [state.ano, state.mes]);

  const handleAutoUpdateToggle = useCallback(() => {
    setIsAutoUpdateOn((prevState) => !prevState);
  }, []);

  const saveAutoUpdateConfig = useCallback(() => {
    apiCall('/api/save-auto-update-config', 'POST', {
      isAutoUpdateOn,
      autoUpdateTime,
    })
      .then(() => alert('Configuração salva com sucesso!'))
      .catch((error) => alert('Erro ao salvar a configuração.'));
  }, [isAutoUpdateOn, autoUpdateTime]);

  return (
    <div className="config-container">
      <form>
        <h1>Importar Dados</h1>

        {/* Seções de Dados */}
        <DataSection type="cadastro" title="Cadastros Individuais (FCI)" state={state} importData={importData} extractData={extractData} />
        <DataSection type="domiciliofcd" title="Cadastros Domiciliares" state={state} importData={importData} extractData={extractData} />
        <DataSection type="visitas" title="Visitas" state={state} importData={importData} extractData={extractData} />
        <DataSection type="atendimentos" title="Atendimentos" state={state} importData={importData} extractData={extractData} />
        <DataSection type="iaf" title="IAF" state={state} importData={importData} extractData={extractData} />
        <DataSection type="pse" title="PSE" state={state} importData={importData} extractData={extractData} />
        <DataSection type="pse_prof" title="PSE Profissionais" state={state} importData={importData} extractData={extractData} />
        <DataSection type="bpa" title="BPA" state={state} importData={importData} extractData={extractData} />

        <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
          <label htmlFor="mes">Mês:</label>
          <select
            id="mes"
            name="mes"
            value={state.mes}
            onChange={(e) => dispatch({ type: 'SET_MES', payload: e.target.value })}
          >
            {['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'].map((mes, index) => (
              <option value={String(index + 1).padStart(2, '0')} key={index}>
                {mes}
              </option>
            ))}
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