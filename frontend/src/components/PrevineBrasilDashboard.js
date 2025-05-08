// src/components/PrevineBrasilDashboard.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';
// Import a charting library if needed, e.g., for gauges

function PrevineBrasilDashboard() {
  const [summaryData, setSummaryData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    setLoading(true);
    axios.get(`http://${window.location.hostname}:5000/api/reports/previne/summary`)
      .then(response => {
        setSummaryData(response.data);
        setLoading(false);
      })
      .catch(error => {
        console.error('Erro ao buscar dados de resumo do Previne Brasil:', error);
        setError('Não foi possível carregar os dados.');
        setLoading(false);
      });
  }, []);

  if (loading) {
    return <div className="container">Carregando dados do dashboard Previne Brasil...</div>;
  }

  if (error) {
    return <div className="container error-message">{error}</div>;
  }

  if (!summaryData) {
    return <div className="container">Nenhum dado encontrado para o dashboard Previne Brasil.</div>;
  }

  return (
    <div className="container">
      <h1>Dashboard: Previne Brasil ({summaryData.periodo || 'N/A'})</h1>
      
      <div className="card-container" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))' }}> {/* Adjust grid for potentially more cards */}
        <div className="card">
          <h3>Indicador Sintético Final (ISF)</h3>
          {/* Placeholder for a gauge chart */}
          <p className="count">{summaryData.isf?.toFixed(2) || 'N/A'}</p>
        </div>

        {/* Display individual indicators */}
        {summaryData.indicadores && Object.entries(summaryData.indicadores).map(([key, ind]) => (
          <div className="card" key={key}>
            <h3>{ind.nome || key}</h3>
            <p className="count">{ind.valor?.toFixed(2) || 'N/A'}%</p>
            <p><small>Numerador: {ind.numerador?.toLocaleString('pt-BR') || 'N/A'}</small></p>
            <p><small>Denominador: {ind.denominador?.toLocaleString('pt-BR') || 'N/A'}</small></p>
          </div>
        ))}
      </div>

      {/* Placeholder for other charts/tables from the Previne report (e.g., ranking) */}
      {/* <div className="dashboard-grid">
        <div className="dashboard-chart-container">
          <h2>Ranking por Equipe/Microárea (Placeholder)</h2>
          <p>Implementar tabela ou gráfico de ranking aqui.</p>
        </div>
      </div> */}
    </div>
  );
}

export default PrevineBrasilDashboard;

