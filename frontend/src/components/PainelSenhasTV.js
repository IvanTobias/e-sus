// /home/ubuntu/esus_project/frontend/src/components/PainelSenhasTV.js
import React, { useState, useEffect } from 'react';
import io from 'socket.io-client';
import './PainelSenhasTV.css'; // Create this CSS file for styling

// Connect to the Socket.IO server (adjust URL if needed)
// Use window.location.hostname to dynamically get the backend host
const SOCKET_SERVER_URL = `http://${window.location.hostname}:5000`;

function PainelSenhasTV() {
    const [ultimasChamadas, setUltimasChamadas] = useState([]);
    const [senhaAtual, setSenhaAtual] = useState(null); // The most recently called ticket
    const [historicoChamadas, setHistoricoChamadas] = useState([]); // History excluding the current one
    const [error, setError] = useState(null);

    useEffect(() => {
        // Fetch initial data
        const fetchInitialData = async () => {
            try {
                const response = await fetch(`${SOCKET_SERVER_URL}/api/senhas/painel-tv?limit=5`);
                if (!response.ok) {
                    throw new Error('Falha ao buscar dados iniciais do painel');
                }
                const data = await response.json();
                if (data && data.length > 0) {
                    setSenhaAtual(data[0]);
                    setHistoricoChamadas(data.slice(1));
                } else {
                    setSenhaAtual(null);
                    setHistoricoChamadas([]);
                }
                setError(null);
            } catch (err) {
                console.error("Erro ao buscar dados iniciais:", err);
                setError(err.message);
                // Initialize with empty state on error
                setSenhaAtual(null);
                setHistoricoChamadas([]);
            }
        };

        fetchInitialData();

        // Setup Socket.IO connection
        const socket = io(`${SOCKET_SERVER_URL}/senhas`, {
            transports: ['websocket', 'polling'] // Specify transports
        });

        socket.on('connect', () => {
            console.log('Conectado ao servidor Socket.IO (Painel TV)');
            setError(null); // Clear error on successful connection
        });

        socket.on('connect_error', (err) => {
            console.error('Erro de conexão Socket.IO (Painel TV):', err);
            setError(`Falha na conexão real-time: ${err.message}. Tentando reconectar...`);
        });

        socket.on('disconnect', (reason) => {
            console.log(`Desconectado do servidor Socket.IO (Painel TV): ${reason}`);
            if (reason !== 'io client disconnect') {
                 setError('Desconectado do servidor. Tentando reconectar...');
            }
        });

        // Listen for new called tickets
        socket.on('senha_chamada', (novaSenhaChamada) => {
            console.log('Nova senha chamada recebida:', novaSenhaChamada);
            setError(null); // Clear error on receiving data
            // Update state: new ticket becomes current, current moves to history
            setHistoricoChamadas(prevHistorico => {
                const novoHistorico = senhaAtual ? [senhaAtual, ...prevHistorico] : [...prevHistorico];
                // Keep only the last 4 historical tickets (total 5 including current)
                return novoHistorico.slice(0, 4);
            });
            setSenhaAtual(novaSenhaChamada);
        });

        // Listen for general updates (e.g., status changes, might need refresh)
        socket.on('atualizacao_fila', () => {
            console.log('Recebido sinal de atualização da fila, buscando dados...');
            // Optionally refetch data if needed, though 'senha_chamada' should be primary
            // fetchInitialData(); // Re-fetch might cause flicker, use carefully
        });

        // Cleanup on component unmount
        return () => {
            console.log('Desconectando Socket.IO (Painel TV)');
            socket.disconnect();
        };
    }, [senhaAtual]); // Re-run effect if senhaAtual changes to update history correctly

    return (
        <div className="painel-tv-container">
            {error && <div className="painel-error">{error}</div>}
            <div className="painel-header">
                <h1>Painel de Chamadas</h1>
            </div>
            <div className="chamada-atual">
                {senhaAtual ? (
                    <>
                        <div className="senha-numero-atual">{senhaAtual.numero_senha}</div>
                        <div className="guiche-atual">{senhaAtual.guiche_chamada}</div>
                        {/* <div className="tipo-atendimento-atual">{senhaAtual.tipo_atendimento}</div> */}
                    </>
                ) : (
                    <div className="senha-numero-atual">- - -</div>
                )}
            </div>
            <div className="historico-chamadas">
                <h2>Últimas Chamadas</h2>
                <ul>
                    {historicoChamadas.length > 0 ? (
                        historicoChamadas.map((senha) => (
                            <li key={senha.id}>
                                <span className="senha-numero-historico">{senha.numero_senha}</span>
                                <span className="guiche-historico">{senha.guiche_chamada}</span>
                            </li>
                        ))
                    ) : (
                        <li>Nenhuma chamada anterior</li>
                    )}
                </ul>
            </div>
            <div className="painel-footer">
                 {/* Add time, date, or other info here */} 
                 {new Date().toLocaleTimeString('pt-BR')} - {new Date().toLocaleDateString('pt-BR')}
            </div>
        </div>
    );
}

export default PainelSenhasTV;

