import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { io } from 'socket.io-client';

const socket = io('http://127.0.0.1:5000');

function BPAForm() {
  const [bpaConfig, setBpaConfig] = useState({
    seq7: '',
    seq8: '',
    seq9: '',
    seq10: '',
    seq11: 'M',
  });
  const [progress, setProgress] = useState(0);
  const [isGenerating, setIsGenerating] = useState(() => localStorage.getItem('isGeneratingBPA') === 'true');
  const [isFileAvailable, setIsFileAvailable] = useState(() => localStorage.getItem('isFileAvailable') === 'true');
  const [isButtonLocked, setIsButtonLocked] = useState(() => localStorage.getItem('isButtonLocked') === 'true');
  const [showModal, setShowModal] = useState(false);
  const [bpaFiles, setBpaFiles] = useState([]);

  useEffect(() => {
    const handleProgressUpdate = (data) => {
      if (data.type === 'bpa') {
        setProgress(data.progress);

        if (data.progress === 100) {
          setIsGenerating(false);
          setIsFileAvailable(true);
          localStorage.setItem('isGeneratingBPA', 'false');
          localStorage.setItem('isFileAvailable', 'true');
          alert('Geração do BPA concluída com sucesso!');
        }
      }
    };

    socket.on('progress_update', handleProgressUpdate);

    axios.get('http://127.0.0.1:5000/api/load-bpa-config')
      .then(response => setBpaConfig(response.data))
      .catch(error => console.error('Erro ao carregar BPA config:', error));

    return () => {
      socket.off('progress_update', handleProgressUpdate);
    };
  }, []);

  const handleChange = (e) => {
    const { id, value } = e.target;
    setBpaConfig(prevConfig => ({ ...prevConfig, [id]: value }));
  };

  const salvarConfiguracoesBPA = async () => {
    try {
      const response = await axios.post('http://127.0.0.1:5000/api/save-bpa-config', bpaConfig);
      alert(response.data.status);
    } catch (error) {
      console.error('Erro ao salvar configuração BPA:', error);
    }
  };

  const gerarBPA = async () => {
    setProgress(0);
    setIsGenerating(true);
    setIsFileAvailable(false);
    setIsButtonLocked(true); // Bloqueia o botão após o clique
    localStorage.setItem('isGeneratingBPA', 'true');
    localStorage.setItem('isFileAvailable', 'false');
    localStorage.setItem('isButtonLocked', 'true'); // Armazena o estado do botão bloqueado

    const today = new Date();
    const day = String(today.getDate()).padStart(2, '0');
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const year = today.getFullYear();
    const formattedDate = `${day}-${month}-${year}`;

    try {
      const response = await axios.post('http://127.0.0.1:5000/api/gerar-bpa', {}, { responseType: 'blob' });
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `BPA_${formattedDate}.txt`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      setIsGenerating(false);
      setIsFileAvailable(true);
      localStorage.setItem('isGeneratingBPA', 'false');
      localStorage.setItem('isFileAvailable', 'true');
    } catch (error) {
      console.error('Erro ao gerar BPA:', error);
      setIsGenerating(false);
      localStorage.setItem('isGeneratingBPA', 'false');
    }
  };

  const abrirModal = async () => {
    setShowModal(true);
    try {
      const response = await axios.get('http://127.0.0.1:5000/api/list-bpa-files');
      setBpaFiles(response.data.files);
    } catch (error) {
      console.error('Erro ao listar arquivos BPA:', error);
    }
  };

  const fecharModal = () => setShowModal(false);

  const baixarArquivo = (filename) => {
    window.open(`http://127.0.0.1:5000/api/download-bpa-file?filename=${filename}`, '_blank');
  };

  const deletarArquivo = async (filename) => {
    try {
      await axios.delete(`http://127.0.0.1:5000/api/delete-bpa-file?filename=${filename}`);
      setBpaFiles(bpaFiles.filter(file => file !== filename));
      alert(`Arquivo ${filename} deletado com sucesso!`);
    } catch (error) {
      console.error(`Erro ao deletar arquivo ${filename}:`, error);
    }
  };

  return (
    <div className={`config-container ${isGenerating ? 'loading' : ''}`}> {/* Aplica a classe "loading" dinamicamente */}
      <h1>Gerador de BPA</h1>
      <form id="bpaForm">
        {/* Inputs para a configuração do BPA */}
        <div>
          <label htmlFor="seq7">Nome do Responsável:</label>
          <input type="text" id="seq7" value={bpaConfig.seq7} onChange={handleChange} maxLength="30" size="30" />
          <label htmlFor="seq8">Sigla do Responsável:</label>
          <input type="text" id="seq8" value={bpaConfig.seq8} onChange={handleChange} maxLength="6" size="6" />
          <label htmlFor="seq9">CPF / CNPJ:</label>
          <input type="text" id="seq9" value={bpaConfig.seq9} onChange={handleChange} maxLength="14" size="14" />
          <label htmlFor="seq10">Nome do órgão de saúde destino:</label>
          <input type="text" id="seq10" value={bpaConfig.seq10} onChange={handleChange} maxLength="40" size="40" />
          <label htmlFor="seq11">Indicador do órgão destino:</label>
          <select id="seq11" value={bpaConfig.seq11} onChange={handleChange}>
            <option value="M">M (Municipal)</option>
            <option value="E">E (Estadual)</option>
          </select>
        </div>

        {/* Contêiner Flexível para Botões e Barra de Progresso */}
        <div className="flex-container">
          <button
            type="button"
            onClick={salvarConfiguracoesBPA}
            className={`btn btn-secondary ${isGenerating ? 'btn-disabled' : ''}`}
            disabled={isGenerating}
          >
            Salvar Configurações
          </button>
          <button
            type="button"
            onClick={gerarBPA}
            className={`btn btn-primary ${isGenerating || isButtonLocked ? 'btn-disabled' : ''}`}
            disabled={isGenerating || isButtonLocked}
          >
            {isGenerating ? 'Processando...' : 'Gerar BPA'}
          </button>
          <button
            type="button"
            onClick={abrirModal}
            className={`btn btn-primary ${!isFileAvailable ? 'btn-disabled' : ''}`}
            disabled={!isFileAvailable}
          >
            Download
          </button>

          {/* Barra de Progresso ao Lado dos Botões */}
          {isGenerating && (
            <div className="progressContainer">
              <div className="progress">
                <div
                  className="progressBar"
                  role="progressbar"
                  style={{ width: `${progress}%` }}
                  aria-valuenow={progress}
                  aria-valuemin="0"
                  aria-valuemax="100"
                >
                  <span>{progress}%</span>
                </div>
              </div>
            </div>
          )}
        </div>
      </form>

      {/* Modal para exibir arquivos BPA */}
      {showModal && (
        <div className={`modal ${isGenerating ? 'loading' : ''}`}>
          <div className="modal-content">
            <div className="modal-header">
              <h2>Arquivos BPA Disponíveis</h2>
              <button onClick={fecharModal} className="close-button">✖</button>
            </div>
            <div className="modal-body">
              <ul>
                {bpaFiles.map((file, index) => (
                  <li key={index}>
                    {file}
                    <button onClick={() => baixarArquivo(file)} className="btn btn-link">Baixar</button>
                    <button onClick={() => deletarArquivo(file)} className="btn btn-link">Deletar</button>
                  </li>
                ))}
              </ul>
            </div>
            <div className="modal-footer">
              <button onClick={fecharModal} className="btn btn-secondary">Fechar</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default BPAForm;
