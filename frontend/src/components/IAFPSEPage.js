import React, { useState, useEffect } from 'react';
import { writeFileXLSX, utils } from 'xlsx';
import '../styles/IAFPSEPage.css';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:5000';

function IAFPSEPage() {
  const [selectedTab, setSelectedTab] = useState('IAF');
  const [data, setData] = useState([]);
  const [columns, setColumns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [ano, setAno] = useState(new Date().getFullYear());
  const [mes, setMes] = useState(new Date().getMonth() + 1);

  const updateData = async () => {
    try {
      setLoading(true);
      const response = await fetch(`${API_BASE_URL}/api/data`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tipo: selectedTab.toLowerCase(), ano, mes }),
      });

      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.error || 'Erro desconhecido ao buscar dados');
      }

      setData(result.data || []);
      setColumns(result.columns || []);
    } catch (error) {
      console.error('Erro ao atualizar dados:', error);
      alert('Erro ao atualizar dados');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    updateData();
  }, [selectedTab, ano, mes]);

  const formatColumnName = (col) => (
    col.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
  );

  const exportToExcel = () => {
    if (data.length === 0) {
      alert('Não há dados para exportar.');
      return;
    }

    const worksheet = utils.json_to_sheet(data);
    const workbook = utils.book_new();
    utils.book_append_sheet(workbook, worksheet, 'Dados');
    writeFileXLSX(workbook, `dados_${selectedTab}_${ano}_${mes}.xlsx`);
  };

  const renderTable = () => {
    if (data.length === 0 || columns.length === 0) {
      return <p>Nenhum dado disponível.</p>;
    }

    return (
      <div className="styled-table-wrapper">
        <div style={{ overflowX: 'auto' }}>
          <table className="styled-table">
            <thead>
              <tr>
                {columns.map((col) => (
                  <th key={col}>{formatColumnName(col)}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((row, i) => (
                <tr key={i}>
                  {columns.map((col) => (
                    <td key={`${i}-${col}`} title={row[col]}>
                      {row[col]}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  return (
    <div className="container">
      <div className="tabs">
        {['IAF', 'PSE', 'pse_prof'].map((tab) => (
          <button
            key={tab}
            className={`tab ${selectedTab === tab ? 'active' : ''}`}
            onClick={() => setSelectedTab(tab)}
          >
            {tab === 'pse_prof' ? 'PSE Profissionais' : tab}
          </button>
        ))}

        <div className="date-selectors">
          <label>
            Ano:
            <input
              type="number"
              value={ano}
              onChange={(e) => setAno(Number(e.target.value))}
              min="2000"
              max="2100"
            />
          </label>
          <label>
            Mês:
            <input
              type="number"
              value={mes}
              onChange={(e) => setMes(Number(e.target.value))}
              min="1"
              max="12"
            />
          </label>
        </div>

        <button onClick={updateData} className="export-button" disabled={loading}>
          {loading ? 'Atualizando...' : 'Atualizar'}
        </button>
        <button onClick={exportToExcel} className="export-button" disabled={loading}>
          {loading ? 'Exportando...' : 'Exportar Excel'}
        </button>
      </div>

      {renderTable()}
    </div>
  );
}

export default IAFPSEPage;
