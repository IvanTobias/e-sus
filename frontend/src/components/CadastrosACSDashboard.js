// src/components/CadastrosACSDashboard.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';
// Import a charting library if needed, e.g., import { Bar } from 'react-chartjs-2';

function CadastrosACSDashboard() {
  const [summaryData, setSummaryData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    setLoading(true);
    axios.get(`http://${window.location.hostname}:5000/api/reports/cadastros_acs/summary`)
      .then(response => {
        setSummaryData(response.data);
        setLoading(false);
      })
      .catch(error => {
        console.error('Erro ao buscar dados de resumo dos cadastros ACS:', error);
        setError('Não foi possível carregar os dados.');
        setLoading(false);
      });
  }, []);

  if (loading) {
    return <div className="container">Carregando dados do dashboard de Cadastros ACS...</div>;
  }

  if (error) {
    return <div className="container error-message">{error}</div>;
  }

  if (!summaryData) {
    return <div className="container">Nenhum dado encontrado para o dashboard de Cadastros ACS.</div>;
  }

  // Basic display - enhance with charts later
  return (
    <div className="container">
      <h1>Dashboard: Cadastros ACS (Resumo)</h1>
      <div className="card-container">
        <div className="card">
          <h3>Total de Cadastros Ativos</h3>
          <p className="count">{summaryData.total_cadastros?.toLocaleString('pt-BR') || '0'}</p>
        </div>
        <div className="card">
          <h3>% com CPF</h3>
          <p className="count">{summaryData.percent_cpf?.toFixed(2) || '0.00'}%</p>
        </div>
        <div className="card">
          <h3>% com CNS</h3>
          <p className="count">{summaryData.percent_cns?.toFixed(2) || '0.00'}%</p>
        </div>
      </div>

      {/* Placeholder for charts - Implement using a library like Chart.js or Recharts */}
      <div className="dashboard-grid">
        <div className="dashboard-chart-container">
          <h2>Distribuição por Unidade</h2>
          {/* Bar chart component for summaryData.distribuicao_unidade */}
          <pre>{JSON.stringify(summaryData.distribuicao_unidade, null, 2)}</pre> 
        </div>
        <div className="dashboard-chart-container">
          <h2>Distribuição por Equipe</h2>
          {/* Bar chart component for summaryData.distribuicao_equipe */}
          <pre>{JSON.stringify(summaryData.distribuicao_equipe, null, 2)}</pre>
        </div>
         <div className="dashboard-chart-container">
          <h2>Distribuição por Microárea</h2>
          {/* Bar chart component for summaryData.distribuicao_microarea */}
          <pre>{JSON.stringify(summaryData.distribuicao_microarea, null, 2)}</pre>
        </div>
      </div>
    </div>
  );
}

export default CadastrosACSDashboard;

