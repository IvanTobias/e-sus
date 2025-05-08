import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Download } from 'react-feather';
import './BPAGeneration.css';

const API_BASE_URL = `http://${window.location.hostname}:5000/api`;

function BPAGeneration() {
    const [loading, setLoading] = useState(false);
    const [message, setMessage] = useState('');
    const [error, setError] = useState('');
    const [generatedFiles, setGeneratedFiles] = useState([]);
    const [loadingFiles, setLoadingFiles] = useState(false);

    // Estados adicionais para AtualizarCEP
    const [registrosAtualizados, setRegistrosAtualizados] = useState(0);
    const [totalRegistros, setTotalRegistros] = useState(0);
    const [cepSelecionado, setCepSelecionado] = useState('');
    const [enderecosCandidatos, setEnderecosCandidatos] = useState([]);
    const [mostrarEscolhaEndereco, setMostrarEscolhaEndereco] = useState(false);

    const fetchGeneratedFiles = async () => {
        setLoadingFiles(true);
        setError('');
        try {
            const response = await axios.get(`${API_BASE_URL}/list-bpa-files`);
            if (response.data && Array.isArray(response.data.files)) {
                setGeneratedFiles(response.data.files);
            } else {
                throw new Error(response.data.error || 'Erro ao listar arquivos BPA.');
            }
        } catch (err) {
            setError(err.message || 'Falha ao conectar com o servidor.');
            setGeneratedFiles([]);
        } finally {
            setLoadingFiles(false);
        }
    };

    useEffect(() => {
        fetchGeneratedFiles();
    }, []);

    const handleGenerateBPA = async () => {
        setLoading(true);
        setError('');
        setMessage('');
        try {
            const response = await axios.post(`${API_BASE_URL}/gerar-bpa`, {}, { responseType: 'blob' });

            if (response.status === 200 && response.headers['content-type'] === 'text/plain') {
                const url = window.URL.createObjectURL(new Blob([response.data]));
                const link = document.createElement('a');
                const contentDisposition = response.headers['content-disposition'];
                let filename = 'bpa_gerado.txt';
                if (contentDisposition) {
                    const filenameMatch = contentDisposition.match(/filename="?(.+)"?/);
                    if (filenameMatch && filenameMatch[1]) filename = filenameMatch[1];
                }
                link.href = url;
                link.setAttribute('download', filename);
                document.body.appendChild(link);
                link.click();
                link.remove();
                window.URL.revokeObjectURL(url);

                setMessage(`Arquivo ${filename} gerado com sucesso.`);
                fetchGeneratedFiles();
            } else {
                throw new Error('Resposta inesperada do servidor ao gerar BPA.');
            }
        } catch (err) {
            setError(err.message || 'Falha ao conectar com o servidor.');
        } finally {
            setLoading(false);
        }
    };

    const handleDownloadFile = async (filename) => {
        try {
            const response = await axios.get(`${API_BASE_URL}/download-bpa-file`, {
                params: { filename },
                responseType: 'blob',
            });

            if (response.status === 200) {
                const url = window.URL.createObjectURL(new Blob([response.data]));
                const link = document.createElement('a');
                link.href = url;
                link.setAttribute('download', filename);
                document.body.appendChild(link);
                link.click();
                link.remove();
                window.URL.revokeObjectURL(url);
            } else {
                throw new Error('Erro ao baixar arquivo.');
            }
        } catch (err) {
            setError(`Falha ao baixar o arquivo ${filename}.`);
        }
    };

    const AtualizarCEP = async () => {
        setRegistrosAtualizados(0);
        setTotalRegistros(0);
        setMostrarEscolhaEndereco(false);
        setError('');

        try {
            const response = await axios.get(`${API_BASE_URL}/corrigir-ceps`);
            const dados = response.data;

            if (dados?.multiplo) {
                setCepSelecionado(dados.cep);
                setEnderecosCandidatos(dados.candidatos);
                setMostrarEscolhaEndereco(true);
            } else {
                setMessage('CEPs atualizados com sucesso.');
            }
        } catch (error) {
            setError('Erro ao Atualizar CEP');
        }
    };

    return (
        <div className="bpa-generation-container container">
            <h1>Gerar Arquivo BPA</h1>
            <p>Clique abaixo para gerar o arquivo BPA.</p>

            {message && <div className="success-message">{message}</div>}
            {error && <div className="error-message">Erro: {error}</div>}

            <button onClick={handleGenerateBPA} disabled={loading} className="botao-gerar-bpa">
                {loading ? 'Gerando BPA...' : 'Gerar e Baixar BPA'}
            </button>

            <button onClick={AtualizarCEP} disabled={loading} className="botao-gerar-bpa">
                {loading ? 'Atualizando CEP...' : 'Atualizar CEP'}
            </button>

            <div className="generated-files-section">
                <h2>Arquivos BPA Gerados</h2>
                {loadingFiles ? (
                    <p>Carregando...</p>
                ) : generatedFiles.length > 0 ? (
                    <ul className="file-list">
                        {generatedFiles.map(file => (
                            <li key={file} className="file-item">
                                <span>{file}</span>
                                <button onClick={() => handleDownloadFile(file)} className="botao-download-file">
                                    <Download size={16} />
                                </button>
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
