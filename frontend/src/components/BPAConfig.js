// /home/ubuntu/esus_project/frontend/src/components/BPAConfig.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './BPAConfig.css'; // Create this CSS file for styling

// Use window.location.hostname to dynamically get the backend host
const API_BASE_URL = `http://${window.location.hostname}:5000/api`;

function BPAConfig() {
    const [config, setConfig] = useState({
        seq7: '', // Nome do Órgão Responsável
        seq8: '', // Sigla do Órgão
        seq9: '', // CGC/CPF do Prestador
        seq10: '', // Destino (Nome da Secretaria)
        seq11: 'M' // Indicador de Órgão (M=Municipal, E=Estadual)
    });
    const [loading, setLoading] = useState(false);
    const [message, setMessage] = useState('');
    const [error, setError] = useState('');

    useEffect(() => {
        // Load existing config on component mount
        const loadConfig = async () => {
            setLoading(true);
            setError('');
            setMessage('');
            try {
                const response = await axios.get(`${API_BASE_URL}/load-bpa-config`);
                if (response.data && typeof response.data === 'object' && !response.data.error) {
                    // Ensure default value for seq11 if missing
                    setConfig(prevConfig => ({
                        ...prevConfig,
                        ...response.data,
                        seq11: response.data.seq11 || 'M'
                    }));
                } else if (response.status === 404) {
                    // Config file doesn't exist yet, keep defaults
                    setMessage('Nenhuma configuração BPA encontrada. Preencha e salve.');
                } else {
                    throw new Error(response.data.error || 'Erro ao carregar configuração BPA.');
                }
            } catch (err) {
                console.error("Erro ao carregar configuração BPA:", err);
                setError(err.response?.data?.error || err.message || 'Falha ao conectar com o servidor.');
            } finally {
                setLoading(false);
            }
        };
        loadConfig();
    }, []);

    const handleChange = (e) => {
        const { name, value } = e.target;
        setConfig(prevConfig => ({
            ...prevConfig,
            [name]: value
        }));
    };

    const handleSaveConfig = async (e) => {
        e.preventDefault();
        setLoading(true);
        setError('');
        setMessage('');
        try {
            const response = await axios.post(`${API_BASE_URL}/save-bpa-config`, config);
            if (response.data.status === 'Configuração BPA salva com sucesso') {
                setMessage('Configuração salva com sucesso!');
            } else {
                throw new Error(response.data.status || 'Erro desconhecido ao salvar configuração.');
            }
        } catch (err) {
            console.error("Erro ao salvar configuração BPA:", err);
            setError(err.response?.data?.status || err.message || 'Falha ao conectar com o servidor.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="bpa-config-container container">
            <h1>Configuração do Cabeçalho BPA</h1>
            <p>Preencha as informações que serão usadas no cabeçalho dos arquivos BPA gerados.</p>

            {loading && <div className="loading-message">Carregando/Salvando...</div>}
            {message && <div className="success-message">{message}</div>}
            {error && <div className="error-message">Erro: {error}</div>}

            <form onSubmit={handleSaveConfig} className="bpa-config-form">
                <div className="form-group">
                    <label htmlFor="seq7">Nome do Órgão Responsável (Máx 30):</label>
                    <input
                        type="text"
                        id="seq7"
                        name="seq7"
                        value={config.seq7}
                        onChange={handleChange}
                        maxLength="30"
                        required
                    />
                </div>
                <div className="form-group">
                    <label htmlFor="seq8">Sigla do Órgão (Máx 6):</label>
                    <input
                        type="text"
                        id="seq8"
                        name="seq8"
                        value={config.seq8}
                        onChange={handleChange}
                        maxLength="6"
                        required
                    />
                </div>
                <div className="form-group">
                    <label htmlFor="seq9">CNPJ/CPF do Prestador (14 dígitos, sem formatação):</label>
                    <input
                        type="text"
                        id="seq9"
                        name="seq9"
                        value={config.seq9}
                        onChange={handleChange}
                        maxLength="14"
                        pattern="\d{14}" // Basic pattern for 14 digits
                        title="Digite exatamente 14 dígitos numéricos."
                        required
                    />
                </div>
                <div className="form-group">
                    <label htmlFor="seq10">Destino (Nome da Secretaria, Máx 40):</label>
                    <input
                        type="text"
                        id="seq10"
                        name="seq10"
                        value={config.seq10}
                        onChange={handleChange}
                        maxLength="40"
                        required
                    />
                </div>
                <div className="form-group">
                    <label htmlFor="seq11">Indicador de Órgão:</label>
                    <select
                        id="seq11"
                        name="seq11"
                        value={config.seq11}
                        onChange={handleChange}
                        required
                    >
                        <option value="M">Municipal</option>
                        <option value="E">Estadual</option>
                    </select>
                </div>

                <button type="submit" disabled={loading} className="botao-salvar-config">
                    {loading ? 'Salvando...' : 'Salvar Configuração BPA'}
                </button>
            </form>
        </div>
    );
}

export default BPAConfig;

