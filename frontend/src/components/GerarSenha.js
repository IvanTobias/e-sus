// /home/ubuntu/esus_project/frontend/src/components/GerarSenha.js
import React, { useState } from 'react';
import axios from 'axios';
import './GerarSenha.css'; // Create this CSS file for styling

// Use window.location.hostname to dynamically get the backend host
const API_BASE_URL = `http://${window.location.hostname}:5000/api`;

function GerarSenha() {
    const [tipoAtendimento, setTipoAtendimento] = useState('Normal');
    const [senhaGerada, setSenhaGerada] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(false);

    const handleGerarSenha = async (tipo) => {
        setLoading(true);
        setError(null);
        setSenhaGerada(null);
        try {
            const response = await axios.post(`${API_BASE_URL}/senhas`, {
                tipo_atendimento: tipo,
                // Add paciente_nome, paciente_cpf, paciente_cns if needed from inputs
                unidade_id: 1 // Example unit ID
            });
            if (response.status === 201) {
                setSenhaGerada(response.data);
            } else {
                throw new Error(response.data.error || 'Erro desconhecido ao gerar senha');
            }
        } catch (err) {
            console.error("Erro ao gerar senha:", err);
            setError(err.message || 'Falha ao conectar com o servidor.');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="gerar-senha-container">
            <h1>Gerar Nova Senha</h1>
            <div className="botoes-tipo">
                <button
                    onClick={() => handleGerarSenha('Normal')}
                    disabled={loading}
                    className="botao-normal"
                >
                    {loading && tipoAtendimento === 'Normal' ? 'Gerando...' : 'Atendimento Normal'}
                </button>
                <button
                    onClick={() => handleGerarSenha('Prioritário')}
                    disabled={loading}
                    className="botao-prioritario"
                >
                    {loading && tipoAtendimento === 'Prioritário' ? 'Gerando...' : 'Atendimento Prioritário'}
                </button>
                {/* Add more buttons for specific types if needed */}
            </div>

            {error && <div className="gerar-senha-error">Erro: {error}</div>}

            {senhaGerada && (
                <div className="senha-gerada-info">
                    <h2>Senha Gerada:</h2>
                    <div className="numero-senha-grande">{senhaGerada.numero_senha}</div>
                    <p>Tipo: {senhaGerada.tipo_atendimento}</p>
                    <p>Horário: {new Date(senhaGerada.dt_geracao).toLocaleTimeString('pt-BR')}</p>
                    <p>Aguarde ser chamado no painel.</p>
                    {/* Button to clear the generated ticket display */}
                    <button onClick={() => setSenhaGerada(null)} className="botao-limpar">Limpar</button>
                </div>
            )}
        </div>
    );
}

export default GerarSenha;

