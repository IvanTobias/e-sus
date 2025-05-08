// /home/ubuntu/esus_project/frontend/src/components/ChamarSenha.js
import React, { useState, useEffect } from "react";
import axios from "axios";
import io from "socket.io-client";
import "./ChamarSenha.css"; // Create this CSS file

// Use window.location.hostname to dynamically get the backend host
const API_BASE_URL = `http://${window.location.hostname}:5000/api`;
const SOCKET_SERVER_URL = `http://${window.location.hostname}:5000`;

function ChamarSenha() {
  const [senhaAtualChamada, setSenhaAtualChamada] = useState(null);
  const [guiche, setGuiche] = useState("Guichê 1"); // Example, make this configurable
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [filaAguardando, setFilaAguardando] = useState([]);

  // Function to fetch the current waiting list
  const fetchFila = async () => {
    try {
      const response = await axios.get(`${API_BASE_URL}/senhas/fila?limit=10`); // Get top 10 waiting
      setFilaAguardando(response.data.aguardando || []);
    } catch (err) {
      console.error("Erro ao buscar fila de espera:", err);
      // Don't set main error, just log for queue display
    }
  };

  // Fetch initial queue and setup Socket.IO
  useEffect(() => {
    fetchFila(); // Fetch initial queue

    const socket = io(`${SOCKET_SERVER_URL}/senhas`, {
      transports: ["websocket", "polling"],
    });

    socket.on("connect", () => {
      console.log("Conectado ao servidor Socket.IO (Chamar Senha)");
    });

    socket.on("connect_error", (err) => {
      console.error("Erro de conexão Socket.IO (Chamar Senha):", err);
    });

    // Listen for updates to refresh the waiting list
    socket.on("nova_senha_fila", () => {
      console.log("Nova senha na fila, atualizando lista...");
      fetchFila();
    });
    socket.on("status_senha_atualizado", () => {
        console.log("Status de senha atualizado, atualizando lista...");
        fetchFila();
      });
    socket.on("atualizacao_fila", () => {
        console.log("Atualização geral da fila, atualizando lista...");
        fetchFila();
    });

    // Cleanup on unmount
    return () => {
      console.log("Desconectando Socket.IO (Chamar Senha)");
      socket.disconnect();
    };
  }, []);

  const handleChamarProxima = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await axios.post(`${API_BASE_URL}/senhas/chamar`, {
        guiche: guiche,
        // profissional_id: 1 // Optional: Add professional ID if needed
      });
      if (response.status === 200) {
        setSenhaAtualChamada(response.data);
      } else if (response.status === 404) {
        setError("Nenhuma senha aguardando para chamar.");
        setSenhaAtualChamada(null);
      } else {
        throw new Error(response.data.error || "Erro desconhecido ao chamar senha");
      }
    } catch (err) {
      console.error("Erro ao chamar senha:", err);
      setError(err.message || "Falha ao conectar com o servidor.");
      setSenhaAtualChamada(null);
    } finally {
      setLoading(false);
      fetchFila(); // Refresh queue after calling
    }
  };

  const handleAtualizarStatus = async (novoStatus) => {
    if (!senhaAtualChamada) return;
    setLoading(true);
    setError(null);
    try {
      const response = await axios.put(
        `${API_BASE_URL}/senhas/${senhaAtualChamada.id}/status`,
        {
          status: novoStatus,
        }
      );
      if (response.status === 200) {
        // If finalized or cancelled, clear the current ticket display
        if (novoStatus === "Finalizado" || novoStatus === "Cancelado") {
            setSenhaAtualChamada(null);
        } else {
            setSenhaAtualChamada(response.data); // Update with potentially new timestamps
        }
      } else {
        throw new Error(response.data.error || "Erro desconhecido ao atualizar status");
      }
    } catch (err) {
      console.error("Erro ao atualizar status:", err);
      setError(err.message || "Falha ao conectar com o servidor.");
    } finally {
      setLoading(false);
      fetchFila(); // Refresh queue after status update
    }
  };

  return (
    <div className="chamar-senha-container container">
      <h1>Chamar Próxima Senha</h1>

      <div className="config-guiche">
        <label htmlFor="guicheInput">Seu Guichê/Sala:</label>
        <input
          type="text"
          id="guicheInput"
          value={guiche}
          onChange={(e) => setGuiche(e.target.value)}
          placeholder="Ex: Guichê 1, Sala 5"
        />
      </div>

      <button
        onClick={handleChamarProxima}
        disabled={loading || !guiche}
        className="botao-chamar"
      >
        {loading ? "Chamando..." : "Chamar Próxima"}
      </button>

      {error && <div className="chamar-senha-error">Erro: {error}</div>}

      {senhaAtualChamada && (
        <div className="senha-chamada-atual-info">
          <h2>Senha Chamada:</h2>
          <div className="numero-senha-atual">{senhaAtualChamada.numero_senha}</div>
          <p>Tipo: {senhaAtualChamada.tipo_atendimento}</p>
          <p>Guichê: {senhaAtualChamada.guiche_chamada}</p>
          <p>Status: {senhaAtualChamada.status}</p>
          <div className="botoes-status">
            <button
              onClick={() => handleAtualizarStatus("Em Atendimento")}
              disabled={loading || senhaAtualChamada.status !== 'Chamado'}
              className="botao-status iniciar"
            >
              Iniciar Atendimento
            </button>
            <button
              onClick={() => handleAtualizarStatus("Finalizado")}
              disabled={loading || senhaAtualChamada.status !== 'Em Atendimento'}
              className="botao-status finalizar"
            >
              Finalizar Atendimento
            </button>
             <button
              onClick={() => handleAtualizarStatus("Aguardando")}
              disabled={loading || senhaAtualChamada.status !== 'Chamado'}
              className="botao-status requeue"
              title="Devolver senha para a fila"
            >
              Re-chamar
            </button>
            <button
              onClick={() => handleAtualizarStatus("Cancelado")}
              disabled={loading}
              className="botao-status cancelar"
            >
              Cancelar Senha
            </button>
          </div>
        </div>
      )}

      <div className="fila-espera-preview">
          <h3>Fila de Espera ({filaAguardando.length})</h3>
          <ul>
              {filaAguardando.length > 0 ? (
                  filaAguardando.map(senha => (
                      <li key={senha.id} className={senha.numero_senha.startsWith("P") ? "prioritario" : "normal"}>
                          {senha.numero_senha} ({senha.tipo_atendimento})
                      </li>
                  ))
              ) : (
                  <li>Fila vazia</li>
              )}
          </ul>
      </div>

    </div>
  );
}

export default ChamarSenha;

