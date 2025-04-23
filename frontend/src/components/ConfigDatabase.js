// src/components/ConfigDatabase.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';

function ConfigDatabase() {
  const [config, setConfig] = useState({
    ip: '',
    port: '',
    database: '',
    username: '',
    password: '',
  });

  // Carrega as configurações iniciais do banco de dados ao montar o componente
  useEffect(() => {
    axios.get(`http://${window.location.hostname}:5000/api/get-config`)
      .then(response => {
        setConfig(response.data);
      })
      .catch(error => console.error('Erro ao carregar configuração:', error));
  }, []);

  // Função para lidar com mudanças nos inputs
  const handleChange = (e) => {
    const { id, value } = e.target;
    setConfig((prevConfig) => ({ ...prevConfig, [id]: value }));
  };

  // Função para salvar as configurações
  const saveConfig = () => {
    axios.post(`http://${window.location.hostname}:5000/api/save-config`, config)
      .then(response => alert(response.data.status))
      .catch(error => console.error('Erro ao salvar configuração:', error));
  };
  
  const testConnection = () => {
    axios.post(`http://${window.location.hostname}:5000/api/test-connection`, config)
      .then(response => alert(response.data.message))
      .catch(error => console.error('Erro ao testar conexão:', error));
  };

  return (
    <div className="config-container">
      <form>
      <h1>Configuração do Banco de Dados</h1>

        <div>
          <label>IP do Servidor:</label>
          <input type="text" id="ip" value={config.ip} onChange={handleChange} placeholder="IP do Servidor" />
          <label>Porta:</label>
          <input type="text" id="port" value={config.port} onChange={handleChange} placeholder="Porta" />
          <label>Nome do Banco:</label>
          <input type="text" id="database" value={config.database} onChange={handleChange} placeholder="Nome do Banco" />
          <label>Usuário:</label>
          <input type="text" id="username" value={config.username} onChange={handleChange} placeholder="Usuário" />
          <label>Senha:</label>
          <input type="password" id="password" value={config.password} onChange={handleChange} placeholder="Senha" />
        </div>
        <div className="button-group">
        <button type="button" onClick={saveConfig}>Salvar Configuração</button>
        <button type="button" onClick={testConnection}>Testar Conexão</button>
        </div>
      </form>
    </div>
  );
}

export default ConfigDatabase;
