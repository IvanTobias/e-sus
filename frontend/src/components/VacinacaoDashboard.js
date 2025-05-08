// src/components/VacinacaoDashboard.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';
// Import a charting library if needed

function VacinacaoDashboard() {
  const [summaryData, setSummaryData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');

  useEffect(() => {
    fetchData();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const fetchData = () => {
    setLoading(true);
    const params = {};
    if (startDate) params.start_date = startDate;
    if (endDate) params.end_date = endDate;

    axios.get(`http://${window.location.hostname}:5000/api/reports/vacinas/summary`, { params })
      .then(response => {
        setSummaryData(response.data);
        if (response.data.start_date && !startDate) setStartDate(response.data.start_date);
        if (response.data.end_date && !endDate) setEndDate(response.data.end_date);
        setLoading(false);
      })
      .catch(error => {
        console.error('Erro ao buscar dados de resumo da vacinação:', error);
        setError('Não foi possível carregar os dados.');
        setLoading(false);
      });
  };

  const handleStartDateChange = (e) => setStartDate(e.target.value);
  const handleEndDateChange = (e) => setEndDate(e.target.value);
  const handleFilterClick = () => fetchData();

  if (loading) {
    return <div className="container">Carregando dados do dashboard de Vacinação...</div>;
  }

  if (error) {
    return <div className="container error-message">{error}</div>;
  }

  if (!summaryData) {
    return <div className="container">Nenhum dado encontrado para o dashboard de Vacinação.</div>;
  }

  return (
    <div className="container">
      <h1>Dashboard: Vacinação (Resumo)</h1>
      
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
        <small> Padrão: Desde 2018</small> 
      </div>

      <div className="card-container">
        <div className="card">
          <h3>Cidadãos Atendidos</h3>
          <p className="count">{summaryData.total_cidadaos?.toLocaleString('pt-BR') || '0'}</p>
        </div>
        <div className="card">
          <h3>Doses Aplicadas</h3>
          <p className="count">{summaryData.total_doses?.toLocaleString('pt-BR') || '0'}</p>
        </div>
        <div className="card">
          <h3>Transcrições Realizadas</h3>
          <p className="count">{summaryData.total_transcricoes?.toLocaleString('pt-BR') || '0'}</p>
        </div>
      </div>

      {/* Placeholder for charts */}
      <div className="dashboard-grid">
        <div className="dashboard-chart-container">
          <h2>Distribuição por Turno</h2>
          <pre>{JSON.stringify(summaryData.distribuicao_turno, null, 2)}</pre>
        </div>
        <div className="dashboard-chart-container">
          <h2>Distribuição por Sexo</h2>
          <pre>{JSON.stringify(summaryData.distribuicao_sexo, null, 2)}</pre>
        </div>
        <div className="dashboard-chart-container">
          <h2>Distribuição por Local</h2>
          <pre>{JSON.stringify(summaryData.distribuicao_local, null, 2)}</pre>
        </div>
         <div className="dashboard-chart-container">
          <h2>Distribuição por Origem</h2>
          <pre>{JSON.stringify(summaryData.distribuicao_origem, null, 2)}</pre>
        </div>
         <div className="dashboard-chart-container">
          <h2>Distribuição por Estratégia</h2>
          <pre>{JSON.stringify(summaryData.distribuicao_estrategia, null, 2)}</pre>
        </div>
        <div className="dashboard-chart-container">
          <h2>Vacinas por Unidade</h2>
          <pre>{JSON.stringify(summaryData.distribuicao_vac_unidade, null, 2)}</pre>
        </div>
        <div className="dashboard-chart-container">
          <h2>Vacinas por Equipe</h2>
          <pre>{JSON.stringify(summaryData.distribuicao_vac_equipe, null, 2)}</pre>
        </div>
      </div>
    </div>
  );
}

export default VacinacaoDashboard;

