import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Download, Trash2 } from 'react-feather';
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

        const contentType = response.headers['content-type'];
        const isPlainText = contentType?.includes('text/plain');

        const blob = new Blob([response.data]);

        // Detecta se erro foi retornado no blob
        if (!isPlainText || blob.size < 50) {
            const text = await blob.text();
            throw new Error(text || 'Arquivo vazio ou resposta inválida');
        }

        // Determina nome do arquivo
        let filename = 'bpa_gerado.txt';
        const contentDisposition = response.headers['content-disposition'];
        if (contentDisposition) {
            const match = contentDisposition.match(/filename="?(.+)"?/);
            if (match?.[1]) filename = match[1];
        }

        // Download
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

        const contentType = response.headers['content-type'];
        const blob = new Blob([response.data]);

        if (!contentType?.includes('text/plain') || blob.size < 50) {
            const text = await blob.text();
            throw new Error(text || 'Erro ao baixar o arquivo.');
        }

        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', filename);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
    } catch (err) {
        setError(`Falha ao baixar o arquivo ${filename}: ${err.message}`);
    }
};

const handleDeleteFile = async (filename) => {
    try {
        await axios.delete(`${API_BASE_URL}/delete-bpa-file`, {
            params: { filename },
        });
        setGeneratedFiles(files => files.filter(f => f !== filename));
        setMessage(`Arquivo ${filename} deletado com sucesso.`);
    } catch (err) {
        setError(`Erro ao deletar o arquivo ${filename}`);
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
                            <div className="file-actions">
                                <button
                                onClick={() => handleDownloadFile(file)}
                                className="botao-icon"
                                title="Baixar"
                                >
                                <Download size={16} />
                                </button>
                                <button
                                onClick={() => handleDeleteFile(file)}
                                className="botao-icon"
                                title="Deletar"
                                >
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
