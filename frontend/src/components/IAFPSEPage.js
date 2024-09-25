import React, { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Line } from 'recharts';

function IAFPSEPage() {
  const [selectedTab, setSelectedTab] = useState('IAF');
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [ano, setAno] = useState(new Date().getFullYear());
  const [mes, setMes] = useState(new Date().getMonth() + 1); // Mês vai de 0-11, por isso +1

  // Função para atualizar os dados do backend
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
      
      console.log('Dados recebidos do backend:', result);  // Verificar os dados retornados
  
      // Filtrar dados com valores válidos (evitar 0 ou nulos)
      const filteredData = result.filter(item => item.total_atividades > 0);
      
      setData(filteredData);
    } catch (error) {
      console.error('Erro ao atualizar dados:', error);
      alert('Erro ao atualizar dados');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    updateData();
  }, [selectedTab]);

  // Função para gerar as barras dinamicamente com base nos dados disponíveis
  const renderBars = () => {
    if (data.length === 0) return null; // Não renderizar barras se não houver dados

    const availableKeys = Object.keys(data[0]); // Pegue as chaves do primeiro objeto de dados

    return (
      <>
        {/* Renderize a barra apenas se a chave correspondente estiver presente nos dados */}
        {availableKeys.includes('total_de_atividades') && (
          <Bar dataKey="total_de_atividades" fill="#82ca9d" />
        )}
        {availableKeys.includes('total_de_participantes') && (
          <Bar dataKey="total_de_participantes" fill="#8884d8" />
        )}
      </>
    );
  };

  return (
    <div className="container">
      <div className="tabs">
        {/* Botões de Abas */}
        <button className={selectedTab === 'IAF' ? 'tab active' : 'tab'} onClick={() => setSelectedTab('IAF')}>
          IAF
        </button>
        <button className={selectedTab === 'PSE' ? 'tab active' : 'tab'} onClick={() => setSelectedTab('PSE')}>
          PSE
        </button>
        <button className={selectedTab === 'PSE Prof' ? 'tab active' : 'tab'} onClick={() => setSelectedTab('PSE Prof')}>
          PSE Prof
        </button>

        {/* Inputs de Ano e Mês */}
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

        {/* Botão para Atualizar Dados */}
        <button className="update-button" onClick={updateData} disabled={loading}>
          {loading ? 'Atualizando...' : 'Atualizar Dados'}
        </button>
      </div>

      {/* Verificando se há dados */}
      {data.length === 0 ? (
        <p>Nenhum dado disponível.</p>
      ) : (
        <ResponsiveContainer width="100%" height={400}>
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            {/* Exibir nome da unidade no eixo X */}
            <XAxis dataKey="nome_unidade" />
            {/* Exibir total de atividades no eixo Y */}
            <YAxis />
            <Tooltip 
              formatter={(value, name) => [value, name.replace(/_/g, ' ')]} // Formatação para remover underscores
            />
            <Legend />

            {/* Renderizar dinamicamente as barras com base nas chaves disponíveis */}
            {renderBars()}

            {/* Exemplo de gráfico de linha, se necessário */}
            <Line type="monotone" dataKey="total_de_atividades" stroke="#ff7300" />
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}

export default IAFPSEPage;
