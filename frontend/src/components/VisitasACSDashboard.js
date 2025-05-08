// src/components/VisitasACSDashboard.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';
// Import a charting library if needed

function VisitasACSDashboard() {
  const [summaryData, setSummaryData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [startDate, setStartDate] = useState(''); // Add state for date filters if needed
  const [endDate, setEndDate] = useState('');

  useEffect(() => {
    fetchData(); // Initial fetch
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // Fetch on mount

  const fetchData = () => {
    setLoading(true);
    // Construct query parameters for dates if they are set
    const params = {};
    if (startDate) params.start_date = startDate;
    if (endDate) params.end_date = endDate;

    axios.get(`http://${window.location.hostname}:5000/api/reports/visitas_acs/summary`, { params })
      .then(response => {
        setSummaryData(response.data);
        // Update date states if they were defaulted by backend
        if (response.data.start_date && !startDate) setStartDate(response.data.start_date);
        if (response.data.end_date && !endDate) setEndDate(response.data.end_date);
        setLoading(false);
      })
      .catch(error => {
        console.error('Erro ao buscar dados de resumo das visitas ACS:', error);
        setError('Não foi possível carregar os dados.');
        setLoading(false);
      });
  };

  // Handlers for date changes and filter button
  const handleStartDateChange = (e) => setStartDate(e.target.value);
  const handleEndDateChange = (e) => setEndDate(e.target.value);
  const handleFilterClick = () => fetchData();

  if (loading) {
    return <div className="container">Carregando dados do dashboard de Visitas ACS...</div>;
  }

  if (error) {
    return <div className="container error-message">{error}</div>;
  }

  if (!summaryData) {
    return <div className="container">Nenhum dado encontrado para o dashboard de Visitas ACS.</div>;
  }

  return (
    <div className="container">
      <h1>Dashboard: Visitas ACS (Resumo)</h1>
      
      {/* Date Filters */}
      <div className="filters">
        <label>
          Data Início:
          <input type="date" value={startDate} onChange={handleStartDateChange} />
        </label>
        <label>
          Data Fim:
          <input type="date" value={endDate} onChange={handleEndDateChange} />
        </label>
        <button onClick={handleFilterClick} disabled={loading}>Filtrar</button>
        <small> Padrão: Últimos 12 meses</small>
      </div>

      <div className="card-container">
        <div className="card">
          <h3>Total de Visitas</h3>
          <p className="count">{summaryData.total_visitas?.toLocaleString('pt-BR') || '0'}</p>
        </div>
        <div className="card">
          <h3>Visitas Identificadas</h3>
          <p className="count">{summaryData.visitas_identificadas?.toLocaleString('pt-BR') || '0'}</p>
        </div>
        <div className="card">
          <h3>Visitas Não Identificadas</h3>
          <p className="count">{summaryData.visitas_nao_identificadas?.toLocaleString('pt-BR') || '0'}</p>
        </div>
      </div>

      {/* Placeholder for charts */}
      <div className="dashboard-grid">
        <div className="dashboard-chart-container">
          <h2>Distribuição por Unidade</h2>
          <pre>{JSON.stringify(summaryData.distribuicao_unidade, null, 2)}</pre>
        </div>
        <div className="dashboard-chart-container">
          <h2>Distribuição por Equipe</h2>
          <pre>{JSON.stringify(summaryData.distribuicao_equipe, null, 2)}</pre>
        </div>
        <div className="dashboard-chart-container">
          <h2>Distribuição por Microárea</h2>
          <pre>{JSON.stringify(summaryData.distribuicao_microarea, null, 2)}</pre>
        </div>
      </div>
    </div>
  );
}

export default VisitasACSDashboard;

