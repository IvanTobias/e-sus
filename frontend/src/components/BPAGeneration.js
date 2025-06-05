import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Download, Trash2 } from 'react-feather';
import io from 'socket.io-client';
import './BPAGeneration.css';

const API_BASE_URL = `http://${window.location.hostname}:5000/api`;
const SOCKET_URL = `http://${window.location.hostname}:5000`;

function BPAGeneration() {
  const [loading, setLoading] = useState(false);
  const [bpaEmExecucao, setBpaEmExecucao] = useState(false);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const [generatedFiles, setGeneratedFiles] = useState([]);
  const [loadingFiles, setLoadingFiles] = useState(false);
  const [progressoCEP, setProgressoCEP] = useState(0);

  const socket = io(SOCKET_URL);

  useEffect(() => {
    fetchGeneratedFiles();
    verificarStatusBPA();

    const interval = setInterval(() => verificarStatusBPA(), 3000);
    socket.on('progress_update', ({ tipo, percentual }) => {
      if (tipo === 'cep') setProgressoCEP(percentual);
    });

    return () => {
      clearInterval(interval);
      socket.disconnect();
    };
  }, []);

  const fetchGeneratedFiles = async () => {
    setLoadingFiles(true);
    try {
      const response = await axios.get(`${API_BASE_URL}/list-bpa-files`);
      setGeneratedFiles(response.data?.files || []);
    } catch (err) {
      setError('Erro ao listar arquivos BPA.');
    } finally {
      setLoadingFiles(false);
    }
  };

  const verificarStatusBPA = async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/bpa-status`);
      setBpaEmExecucao(response.data?.running || false);
    } catch {
      setBpaEmExecucao(false);
    }
  };

  const handleGenerateBPA = async () => {
    setLoading(true);
    setMessage('');
    setError('');
    setBpaEmExecucao(true);

    try {
      const response = await axios.post(`${API_BASE_URL}/gerar-bpa`, {}, { responseType: 'blob' });
      const blob = new Blob([response.data]);
      const contentDisposition = response.headers['content-disposition'];
      let filename = 'bpa_gerado.txt';

      const match = contentDisposition?.match(/filename="?(.+?)"?/);
      if (match?.[1]) filename = match[1];

      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', filename);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);

      setMessage(`Arquivo ${filename} gerado com sucesso.`);
      fetchGeneratedFiles();
    } catch (err) {
      setError('Erro ao gerar BPA.');
    } finally {
      setLoading(false);
      setBpaEmExecucao(false);
    }
  };

  const handleDownloadFile = async (filename) => {
    try {
      const response = await axios.get(`${API_BASE_URL}/download-bpa-file`, {
        params: { filename },
        responseType: 'blob',
      });
      const url = window.URL.createObjectURL(response.data);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', filename);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
    } catch {
      setError(`Erro ao baixar o arquivo ${filename}`);
    }
  };

  const handleDeleteFile = async (filename) => {
    try {
      await axios.delete(`${API_BASE_URL}/delete-bpa-file`, {
        params: { filename },
      });
      setGeneratedFiles(generatedFiles.filter(f => f !== filename));
      setMessage(`Arquivo ${filename} deletado.`);
    } catch {
      setError('Erro ao deletar arquivo.');
    }
  };

  const AtualizarCEP = async () => {
    setMessage('');
    setError('');
    setProgressoCEP(0);
    try {
      await axios.get(`${API_BASE_URL}/corrigir-ceps`);
    } catch {
      setError('Erro ao atualizar CEP.');
    }
  };

  return (
    <div className="bpa-generation-container container">
      <h1>Gerar Arquivo BPA</h1>
      <p>Clique abaixo para gerar o arquivo BPA.</p>

      {message && <div className="success-message">{message}</div>}
      {error && <div className="error-message">Erro: {error}</div>}

      <button
        onClick={handleGenerateBPA}
        disabled={loading || bpaEmExecucao}
        className="botao-gerar-bpa"
      >
        {loading ? 'Gerando BPA...' : 'Gerar e Baixar BPA'}
      </button>

      <button
        onClick={AtualizarCEP}
        disabled={bpaEmExecucao}
        className="botao-gerar-bpa"
      >
        Atualizar CEP
      </button>

      {progressoCEP > 0 && progressoCEP < 100 && (
        <div className="progress-bar-container">
          <div className="progress-bar">
            <div className="progress-fill" style={{ width: `${progressoCEP}%` }}></div>
          </div>
          <span>{progressoCEP}%</span>
        </div>
      )}

      <div className="generated-files-section">
        <h2>Arquivos BPA Gerados</h2>
        {loadingFiles ? (
          <p>Carregando...</p>
        ) : generatedFiles.length > 0 ? (
          <ul className="file-list">
            {generatedFiles.map(file => (
              <li key={file} className="file-item">
                <span>{file}</span>
                <div className="file-actions">
                  <button onClick={() => handleDownloadFile(file)} className="botao-icon" disabled={bpaEmExecucao}>
                    <Download size={16} />
                  </button>
                  <button onClick={() => handleDeleteFile(file)} className="botao-icon" disabled={bpaEmExecucao}>
                    <Trash2 size={16} />
                  </button>
                </div>
              </li>
            ))}
          </ul>
        ) : (
          <p>Nenhum arquivo encontrado.</p>
        )}
      </div>
    </div>
  );
}

export default BPAGeneration;