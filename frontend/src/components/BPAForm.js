// BPAForm.js (melhorado)

import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { io } from 'socket.io-client';

const socket = io(`http://${window.location.hostname}:5000`);

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
  const [totalRegistros, setTotalRegistros] = useState(0);
  const [registrosAtualizados, setRegistrosAtualizados] = useState(0);

  const [enderecosCandidatos, setEnderecosCandidatos] = useState([]);
  const [mostrarEscolhaEndereco, setMostrarEscolhaEndereco] = useState(false);
  const [cepSelecionado, setCepSelecionado] = useState(null);

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
      } else if (data.type === 'cep') {
        setTotalRegistros(data.total);
        setRegistrosAtualizados(data.atualizados);
      }
    };

    socket.on('progress_update', handleProgressUpdate);

    axios.get('http://127.0.0.1:5000/api/load-bpa-config')
      .then(response => setBpaConfig(response.data))
      .catch(console.error);

    return () => socket.off('progress_update', handleProgressUpdate);
  }, []);

  useEffect(() => {
    const verificarDisponibilidadeArquivo = async () => {
      try {
        const response = await axios.get('http://127.0.0.1:5000/api/list-bpa-files');
        const arquivos = response.data.files || [];
        const disponivel = arquivos.some(arquivo => arquivo.startsWith('bpa_'));
        setIsFileAvailable(disponivel);
        localStorage.setItem('isFileAvailable', disponivel.toString());
      } catch (error) {
        console.error('Erro ao verificar arquivos BPA:', error);
      }
    };
    verificarDisponibilidadeArquivo();
  }, []);

  const handleChange = ({ target: { id, value } }) => {
    setBpaConfig(prev => ({ ...prev, [id]: value }));
  };

  const salvarConfiguracoesBPA = async () => {
    try {
      const { data } = await axios.post('http://127.0.0.1:5000/api/save-bpa-config', bpaConfig);
      alert(data.status);
    } catch (error) {
      console.error('Erro ao salvar configuração BPA:', error);
    }
  };

  const AtualizarCEP = async () => {
    try {
      setRegistrosAtualizados(0);
      setTotalRegistros(0);
      const { data } = await axios.get('http://127.0.0.1:5000/api/corrigir-ceps');
      if (data?.multiplo) {
        setCepSelecionado(data.cep);
        setEnderecosCandidatos(data.candidatos);
        setMostrarEscolhaEndereco(true);
      }
    } catch (error) {
      console.error('Erro ao Atualizar CEP', error);
    }
  };

  const confirmarEndereco = async ({ logradouro, bairro }) => {
    try {
      await axios.post('http://127.0.0.1:5000/api/atualizar-cep-escolhido', {
        cep: cepSelecionado,
        logradouro,
        bairro,
      });
      setMostrarEscolhaEndereco(false);
      alert('Endereço escolhido com sucesso!');
    } catch (error) {
      console.error('Erro ao confirmar endereço:', error);
    }
  };

const gerarBPA = async () => {
  setProgress(0);
  setIsGenerating(true);
  setIsButtonLocked(true);
  localStorage.setItem('isGeneratingBPA', 'true');
  localStorage.setItem('isButtonLocked', 'true');

  const today = new Date();
  const formattedDate = today.toLocaleDateString('pt-BR').split('/').join('-');

  try {
    const response = await axios.post(
      'http://127.0.0.1:5000/api/gerar-bpa',
      {},
      { responseType: 'blob' }
    );

    // Detecta se o blob é um erro
    const contentType = response.headers['content-type'];
    const isText = contentType && contentType.includes('application/json') || contentType.includes('text');

    if (isText) {
      const text = await response.data.text(); // <- importante: transforma Blob em texto
      try {
        const json = JSON.parse(text);
        if (json.message) throw new Error(json.message);
      } catch {
        throw new Error(text);
      }
    }

    // Se não for erro, cria o link de download
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', `BPA_${formattedDate}.txt`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);

    setIsFileAvailable(true);
    localStorage.setItem('isFileAvailable', 'true');

  } catch (error) {
    console.error('Erro ao gerar BPA:', error);
    alert(`Erro ao gerar BPA: ${error.message}`);
  } finally {
    setIsGenerating(false);
    setIsButtonLocked(false);
    localStorage.setItem('isGeneratingBPA', 'false');
    localStorage.setItem('isButtonLocked', 'false');
  }
};


  const abrirModal = async () => {
    setShowModal(true);
    try {
      const { data } = await axios.get('http://127.0.0.1:5000/api/list-bpa-files');
      setBpaFiles(data.files);
    } catch (error) {
      console.error('Erro ao listar arquivos BPA:', error);
    }
  };

  const baixarArquivo = (filename) => {
    window.open(`http://127.0.0.1:5000/api/download-bpa-file?filename=${filename}`, '_blank');
  };

  const deletarArquivo = async (filename) => {
    try {
      await axios.delete(`http://127.0.0.1:5000/api/delete-bpa-file?filename=${filename}`);
      setBpaFiles(files => files.filter(f => f !== filename));
      alert(`Arquivo ${filename} deletado com sucesso!`);
    } catch (error) {
      console.error(`Erro ao deletar arquivo ${filename}:`, error);
    }
  };

  return (
    <div className={`config-container ${isGenerating ? 'loading' : ''}`}>
      <form id="bpaForm">
        <h1>Gerador de BPA</h1>
        <div>
          <label htmlFor="seq7">Nome do Responsável:</label>
          <input type="text" id="seq7" value={bpaConfig.seq7} onChange={handleChange} maxLength="30" />

          <label htmlFor="seq8">Sigla do Responsável:</label>
          <input type="text" id="seq8" value={bpaConfig.seq8} onChange={handleChange} maxLength="6" />

          <label htmlFor="seq9">CPF / CNPJ:</label>
          <input type="text" id="seq9" value={bpaConfig.seq9} onChange={handleChange} maxLength="14" />

          <label htmlFor="seq10">Nome do órgão de saúde destino:</label>
          <input type="text" id="seq10" value={bpaConfig.seq10} onChange={handleChange} maxLength="40" />

          <label htmlFor="seq11">Indicador do órgão destino:</label>
          <select id="seq11" value={bpaConfig.seq11} onChange={handleChange}>
            <option value="M">M (Municipal)</option>
            <option value="E">E (Estadual)</option>
          </select>
        </div>

        <div className="flex-container">
          <button type="button" onClick={salvarConfiguracoesBPA} className="btn btn-secondary" disabled={isGenerating}>Salvar Configurações</button>
          <button type="button" onClick={AtualizarCEP} className="btn btn-primary" disabled={isGenerating || isButtonLocked}>Atualizar CEP</button>
          <button type="button" onClick={gerarBPA} className="btn btn-primary" disabled={isGenerating || isButtonLocked}>
            {isGenerating ? 'Processando...' : 'Gerar BPA'}
          </button>
          <button type="button" onClick={abrirModal} className="btn btn-primary" disabled={!isFileAvailable}>Download</button>
        </div>

        {isGenerating && (
          <div className="progressContainer">
            <div className="progress">
              <div className="progressBar" style={{ width: `${progress}%` }} role="progressbar" aria-valuenow={progress} aria-valuemin="0" aria-valuemax="100">
                <span>{progress}%</span>
              </div>
            </div>
          </div>
        )}

        {registrosAtualizados > 0 && (
          <div style={{ marginTop: '10px' }}>
            CEPs atualizados: {registrosAtualizados} de {totalRegistros}
          </div>
        )}
      </form>

      {mostrarEscolhaEndereco && (
        <div className="modal modal-bpa">
          <div className="modal-content">
            <h2>Escolha o Endereço Correto para o CEP: {cepSelecionado}</h2>
            <ul>
              {enderecosCandidatos.map((endereco, idx) => (
                <li key={idx} className="endereco-opcao">
                  <strong>{endereco.logradouro}</strong>, {endereco.bairro} - {endereco.cep}
                  <button onClick={() => confirmarEndereco(endereco)} className="btn btn-primary">Usar este</button>
                </li>
              ))}
            </ul>
            <button onClick={() => setMostrarEscolhaEndereco(false)} className="btn btn-secondary">Cancelar</button>
          </div>
        </div>
      )}

      {showModal && (
        <div className={`modal modal-bpa ${isGenerating ? 'loading' : ''}`}>
          <div className="modal-content">
            <div className="modal-header">
              <h2>Arquivos BPA Disponíveis</h2>
              <button onClick={() => setShowModal(false)} className="close-button">&times;</button>
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
              <button onClick={() => setShowModal(false)} className="btn btn-secondary">Fechar</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default BPAForm;