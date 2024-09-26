import React, { useState, useEffect } from 'react';
import { writeFileXLSX, utils } from 'xlsx'; // Importa as funções da biblioteca xlsx
import '../styles/IAFPSEPage.css'; // Importa o CSS

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
      const response = await fetch('/api/data', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ tipo: selectedTab.toLowerCase(), ano, mes }),
      });
      const result = await response.json();
      
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

  // Função para formatar os nomes das colunas
  const formatColumnName = (col) => {
    return col.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase());
  };

  // Função para exportar os dados para um arquivo Excel
  const exportToExcel = () => {
    if (data.length === 0) {
      alert('Não há dados para exportar.');
      return;
    }

    // Criar uma planilha a partir dos dados da tabela
    const worksheet = utils.json_to_sheet(data);
    
    // Criar um novo workbook
    const workbook = utils.book_new();
    
    // Adicionar a planilha ao workbook
    utils.book_append_sheet(workbook, worksheet, 'Dados');
    
    // Exportar o arquivo
    writeFileXLSX(workbook, `dados_${selectedTab}_${ano}_${mes}.xlsx`);
  };

  const renderTable = () => {
    if (data.length === 0 || columns.length === 0) {
      return <p>Nenhum dado disponível.</p>;
    }

    return (
      <div className="styled-table-wrapper">
        <table className="styled-table">
          <thead>
            <tr>
              {columns.map((col) => (
                <th key={col}>{formatColumnName(col)}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {columns.map((col) => (
                  <td key={`${rowIndex}-${col}`} title={row[col]}>{row[col]}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  return (
    <div className="container">
      <div className="tabs">
        <button className={selectedTab === 'IAF' ? 'tab active' : 'tab'} onClick={() => setSelectedTab('IAF')}>
          IAF
        </button>
        <button className={selectedTab === 'PSE' ? 'tab active' : 'tab'} onClick={() => setSelectedTab('PSE')}>
          PSE
        </button>
        <button className={selectedTab === 'PSE_Prof' ? 'tab active' : 'tab'} onClick={() => setSelectedTab('PSE_Prof')}>
          PSE Prof
        </button>

        <div className="date-selectors">
          <label>
            Ano:
            <input type="number" value={ano} onChange={(e) => setAno(e.target.value)} />
          </label>
          <label>
            Mês:
            <input type="number" value={mes} onChange={(e) => setMes(e.target.value)} min="1" max="12" />
          </label>
        </div>

        <button className="export-button" onClick={updateData} disabled={loading}>
          {loading ? 'Atualizando...' : 'Atualizar'}
        </button>

        {/* Botão de Exportar para XLS */}
        <button className="export-button" onClick={exportToExcel} disabled={loading}>
          {loading ? 'Exportando...' : 'Exportar'}
        </button>
      </div>

      {/* Renderiza a tabela de dados */}
      {renderTable()}
    </div>
  );
}

export default IAFPSEPage;
