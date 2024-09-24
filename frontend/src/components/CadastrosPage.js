import React, { useState, useEffect, useCallback, useMemo } from 'react';
import Select from 'react-select';
import Card from './Card';
import Pagination from './Pagination';
import Modal from 'react-modal';
import * as XLSX from 'xlsx'; 
import { saveAs } from 'file-saver'; 
import {
  fetchUnidadesSaude
  } from '../api/API'; 

Modal.setAppElement('#root');

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://127.0.0.1:5000'; // Atualiza para centralizar as URLs

function CadastrosPage() {
  const [currentPage, setCurrentPage] = useState(1);
  const [unidades, setUnidades] = useState([]);
  const [equipes, setEquipes] = useState([]);
  const [profissionais, setProfissionais] = useState([]);
  const [modalIsOpen, setModalIsOpen] = useState(false);
  const [miniModalIsOpen, setMiniModalIsOpen] = useState(false);
  const [modalData, setModalData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCard, setSelectedCard] = useState('');
  const [unidadesData, setUnidadesData] = useState([]);
  const [selectedDetail, setSelectedDetail] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedTab, setSelectedTab] = useState('cadastros'); 
  const [counts, setCounts] = useState({});
  const [detalhesCounts, setDetalhesCounts] = useState({});
  const [selectedCitizenName, setSelectedCitizenName] = useState('');
  const [cadastrosUnidades, setCadastrosUnidades] = useState([]);
  const [cadastrosEquipes, setCadastrosEquipes] = useState([]);
  const [cadastrosProfissionais, setCadastrosProfissionais] = useState([]);
  const [detalhesUnidades, setDetalhesUnidades] = useState([]);
  const [detalhesEquipes, setDetalhesEquipes] = useState([]);
  const [detalhesProfissionais, setDetalhesProfissionais] = useState([]);

  // Função para carregar dados das unidades de saúde
  const loadUnidadesSaude = useCallback(async () => {
    console.log('Carregando Unidades de Saúde');
    try {
      const data = await fetchUnidadesSaude();  // Verifique se esse fetch está correto
      if (data) {
        setUnidadesData(data);
        console.log('Unidades de Saúde Carregadas:', data);
      } else {
        console.error('Erro ao carregar dados das unidades de saúde.');
      }
    } catch (error) {
      console.error('Erro ao buscar unidades de saúde:', error);
    }
  }, []);
  

  // Controla o carregamento dos dados de contagens com base na aba ativa
  useEffect(() => {
    const loadData = async () => {
      if (selectedTab === 'cadastros' && !counts.length) {
        // Carrega Unidades e Contagens só se não estiverem carregadas
        await loadUnidadesSaude(); 
        await loadContagens();
      } else if (selectedTab === 'detalhes' && !detalhesCounts.length) {
        await loadUnidadesSaude();
        await loadDetalhesCounts();
      }
    };
    loadData();
  }, [selectedTab, unidades, equipes, profissionais]);
  
  const handleTabChange = (tab) => {
    setSelectedTab(tab);
    if (tab === 'cadastros') {
      setUnidades(cadastrosUnidades);
      setEquipes(cadastrosEquipes);
      setProfissionais(cadastrosProfissionais);
    } else if (tab === 'detalhes') {
      setUnidades(detalhesUnidades);
      setEquipes(detalhesEquipes);
      setProfissionais(detalhesProfissionais);
    }
  };
  
  useEffect(() => {
    console.log("Dados no modal:", modalData);
  }, [modalData]);
  
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const loadInitialData = async () => {
      setLoading(true);
      await loadUnidadesSaude();  // Carrega as unidades de saúde
      await loadContagens();      // Carrega as contagens após isso
      setLoading(false);
    };
    loadInitialData();
  }, [selectedTab, unidades, equipes, profissionais]);
  
  const handleExportToExcel = () => {
    const dataWithCardName = [
      { Informacao: `Detalhes do Card: ${selectedCard}` },
      ...filteredData 
    ];
  
    const worksheet = XLSX.utils.json_to_sheet(dataWithCardName);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Dados');
  
    const excelBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
    const data = new Blob([excelBuffer], { type: 'application/octet-stream' });
    saveAs(data, `dados_${selectedCard}.xlsx`); 
  };

  const loadEquipeAndProfissionais = useCallback(async (selectedUnidades = []) => {
    try {
      if (selectedUnidades.length === 0) {
        setEquipes([]);
        setProfissionais([]);
        return;
      }
  
      const data = await fetchUnidadesSaude(selectedUnidades.join(','));
      if (data) {
        setUnidadesData(data);
  
        const equipesDisponiveis = [...new Set(
          data.filter(item => selectedUnidades.includes(item.unidadeSaude)).map(item => item.equipe)
        )];
        setEquipes(prevEquipes => prevEquipes.filter(equipe => equipesDisponiveis.includes(equipe)));
  
        const profissionaisDisponiveis = [...new Set(
          data.filter(item => selectedUnidades.includes(item.unidadeSaude)).map(item => item.profissional)
        )];
        setProfissionais(prevProfissionais => prevProfissionais.filter(profissional => profissionaisDisponiveis.includes(profissional)));
      } else {
        console.error('Erro ao carregar equipes e profissionais.');
      }
    } catch (error) {
      console.error('Erro ao buscar equipes e profissionais:', error);
    }
  }, []);

  const loadContagens = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/contagens?unidade_saude=${unidades.join(',')}&equipe=${equipes.join(',')}&profissional=${profissionais.join(',')}`);
      const data = await response.json();
  
      setCounts(data);
    } catch (error) {
      console.error('Erro ao carregar contagens:', error);
    }
  }, [unidades, equipes, profissionais]);

  const loadDetalhesCounts = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/contagem-detalhes?unidade_saude=${unidades.join(',')}&equipe=${equipes.join(',')}&profissional=${profissionais.join(',')}`);
      const data = await response.json();
      setDetalhesCounts(data);
    } catch (error) {
      console.error("Erro ao carregar contagem de detalhes:", error);
    }
  }, [unidades, equipes, profissionais]);

  const handleCardClick = async (tipo) => {
    setSelectedCard(tipo);
    setModalIsOpen(true);
    setCurrentPage(1);
    setIsLoading(true);

    setModalData([]); // Limpa os dados antes de carregar novos
    setFilteredData([]);

    let url = '';

    // Decide qual URL usar com base no tipo de card clicado
    if (tipo === "Cadastros Domiciliares") {
        url = `${API_BASE_URL}/api/cadastros-domiciliares?unidade_saude=${unidades.join(',')}&equipe=${equipes.join(',')}&profissional=${profissionais.join(',')}`;
    } else {
        url = `${API_BASE_URL}/api/detalhes?tipo=${encodeURIComponent(tipo)}&unidade_saude=${encodeURIComponent(unidades.join(','))}&equipe=${encodeURIComponent(equipes.join(','))}&profissional=${encodeURIComponent(profissionais.join(','))}`;
    }

    try {
        const response = await fetch(url);
        const data = await response.json();

        // Atualiza os dados de acordo com o tipo selecionado
        if (tipo === "Cadastros Domiciliares") {
            setModalData(data);  // Atualiza modalData apenas com cadastros domiciliares
            setFilteredData(data);  // Atualiza os dados filtrados corretamente
        } else {
            setModalData(data);  // Atualiza modalData apenas com detalhes
            setFilteredData(data);  // Atualiza os dados filtrados corretamente
        }

    } catch (error) {
        console.error('Erro ao carregar dados do modal:', error);
    }

    setIsLoading(false);  // Desativa o loading quando os dados forem carregados
};



  const loadModalData = useCallback(async (tipo) => {
    const unidadeSaudeParam = unidades.join(',');
    const equipeParam = equipes.join(',');
    const profissionalParam = profissionais.join(',');

    try {
      const url = `${API_BASE_URL}/api/detalhes?tipo=${encodeURIComponent(tipo)}&unidade_saude=${encodeURIComponent(unidadeSaudeParam)}&equipe=${encodeURIComponent(equipeParam)}&profissional=${encodeURIComponent(profissionalParam)}`;
      
      const response = await fetch(url);
      const data = await response.json();

      const sortedData = data.sort((a, b) => (a.nome || '').localeCompare(b.nome || ''));
      setModalData(sortedData);
      setFilteredData(sortedData);
    } catch (error) {
      console.error('Erro ao carregar dados do modal:', error);
    }
  }, [unidades, equipes, profissionais]);

  const modalClassName = `modal ${isLoading ? 'loading' : ''} fade-in`;

  const handleSearchChange = (e) => {
    const query = e.target.value.toLowerCase();
    setSearchQuery(query);
    const filtered = modalData.filter(
      (item) =>
        (item.nome || '').toLowerCase().includes(query) ||
        (item.cpf || '').includes(query) ||
        (item.cns || '').includes(query) ||
        formatDate(item.data_nascimento).includes(query)
    );
    setFilteredData(filtered);
    setCurrentPage(1);
  };

  const handleUnidadeChange = (selected) => {
    const selectedValues = selected.map((option) => option.value);
    setUnidades(selectedValues);
    setEquipes([]);
    setProfissionais([]);
    loadEquipeAndProfissionais(selectedValues);
  
    if (selectedTab === 'cadastros') {
      setCadastrosUnidades(selectedValues);
    } else {
      setDetalhesUnidades(selectedValues);
    }
  };
  
  const handleEquipeChange = (selected) => {
    const selectedValues = selected.map((option) => option.value);
    setEquipes(selectedValues);
  
    if (selectedTab === 'cadastros') {
      setCadastrosEquipes(selectedValues);
    } else {
      setDetalhesEquipes(selectedValues);
    }
  };
  
  const handleProfissionalChange = (selected) => {
    const selectedValues = selected.map((option) => option.value);
    setProfissionais(selectedValues);
  
    if (selectedTab === 'cadastros') {
      setCadastrosProfissionais(selectedValues);
    } else {
      setDetalhesProfissionais(selectedValues);
    }
  };

  const formatDate = (dateString) => {
    if (!dateString) return '';
    const date = new Date(dateString);
    return date.toLocaleDateString('pt-BR');
  };

  const handleNameClick = useCallback(async (item) => {
    if (!item.co_cidadao) {
      console.warn('ID do cidadão não está definido para o item:', item);
      return;
    }
  
    setIsLoading(true);
  
    setSelectedDetail(null);
    setSelectedCitizenName(''); 

    try {
      const response = await fetch(`${API_BASE_URL}/api/detalhes-hover?co_cidadao=${item.co_cidadao}`);
      if (!response.ok) {
        throw new Error(`Erro ao buscar detalhes: ${response.statusText}`);
      }

      const details = await response.json();

      setSelectedDetail(details);
      setSelectedCitizenName(item.nome);
      setMiniModalIsOpen(true);
    } catch (error) {
      console.error('Erro ao carregar detalhes ao clicar:', error);
    } finally {
      setIsLoading(false);
    }
  }, []);

  const paginatedData = filteredData.slice((currentPage - 1) * 10, currentPage * 10);

  const unidadeOptions = useMemo(() => 
    [...new Set(unidadesData.map((item) => item.unidadeSaude))].map((u) => ({
      value: u,
      label: u,
    })), [unidadesData]
  );
  
  const equipeOptions = useMemo(() =>
    [...new Set(unidadesData.filter((item) => unidades.includes(item.unidadeSaude)).map((item) => item.equipe))].map((e) => ({ value: e, label: e })), 
    [unidades, unidadesData]
  );
  
  const profissionalOptions = useMemo(() =>
    [...new Set(unidadesData.filter((item) => equipes.includes(item.equipe)).map((item) => item.profissional))].map((p) => ({ value: p, label: p })), 
    [equipes, unidadesData]
  );

  const Filtros = ({ unidadeOptions, equipeOptions, profissionalOptions, onUnidadeChange, onEquipeChange, onProfissionalChange, unidades, equipes, profissionais }) => (
    <div className="filters">
      <Select
        isMulti
        options={unidadeOptions}
        value={unidades.map((u) => ({ value: u, label: u }))}
        placeholder="Selecione Unidade de Saúde..."
        onChange={onUnidadeChange}
        className="basic-multi-select"
        classNamePrefix="select"
      />
      <Select
        isMulti
        options={equipeOptions}
        value={equipes.map((e) => ({ value: e, label: e }))}
        placeholder="Selecione Equipe..."
        onChange={onEquipeChange}
        className="basic-multi-select"
        classNamePrefix="select"
      />
      <Select
        isMulti
        options={profissionalOptions}
        value={profissionais.map((p) => ({ value: p, label: p }))}
        placeholder="Selecione Profissional..."
        onChange={onProfissionalChange}
        className="basic-multi-select"
        classNamePrefix="select"
      />
    </div>
  );

  const Cards = ({ data, handleCardClick }) => (
    <div className="card-container">
      {Object.entries(data).map(([key, count], index) => (
        <Card
          key={key}
          title={key.replace('_', ' ')}
          count={count}
          onClick={() => handleCardClick(key.replace('_', ' '))}
          animationDelay={index * 100}
        />
      ))}
    </div>
  );

  return (
    <div className="container">
      <div className="tabs">
        <button
          className={selectedTab === 'cadastros' ? 'tab active' : 'tab'}
          onClick={() => handleTabChange('cadastros')}
        >
          Cadastros
        </button>
        <button
          className={selectedTab === 'detalhes' ? 'tab active' : 'tab'}
          onClick={() => handleTabChange('detalhes')}
        >
          Detalhes
        </button>
      </div>

  
      <div>
        <h1>{selectedTab === 'cadastros' ? '' : ''}</h1>
  
        {/* Carregando spinner */}
        {loading ? (
          <div>Carregando dados...</div>
        ) : (
          <>
            <Filtros 
              unidadeOptions={unidadeOptions} 
              equipeOptions={equipeOptions} 
              profissionalOptions={profissionalOptions} 
              onUnidadeChange={handleUnidadeChange} 
              onEquipeChange={handleEquipeChange} 
              onProfissionalChange={handleProfissionalChange} 
              unidades={unidades} 
              equipes={equipes} 
              profissionais={profissionais} 
            />
  
            <Cards 
              data={selectedTab === 'cadastros' ? counts : detalhesCounts} 
              handleCardClick={handleCardClick} 
            />
          </>
        )}
      </div>
  
      <Modal
        isOpen={modalIsOpen}
        onRequestClose={() => setModalIsOpen(false)}
        message={selectedCard ? `Detalhes de ${selectedCard}` : ''}
        className={modalClassName}
        contentLabel="Detalhes do Cadastro"
        style={{ overlay: { cursor: isLoading ? 'wait' : 'default' } }}
      >
        <div className="modal-content">
          <div className="modal-header">
            <h2>Detalhes de {selectedCard}</h2>
            <button onClick={() => setModalIsOpen(false)} className="close-button">✖</button>
          </div>
  
          <div className="modal-body">
            <input
              type="text"
              placeholder="Pesquisar..."
              value={searchQuery}
              onChange={handleSearchChange}
            />
  
            {selectedCard === "Cadastros Domiciliares" ? (
              <table>
                <thead>
                  <tr>
                    <th>Rua</th>
                    <th>Número</th>
                    <th>Complemento</th>
                    <th>Bairro</th>
                    <th>CEP</th>
                    <th>Unidade de Saúde</th>
                    <th>Profissional</th>
                    <th>Equipe</th>
                  </tr>
                </thead>
                <tbody>
                  {paginatedData.map((item, index) => (
                    <tr key={index}>
                      <td>{item.rua}</td>
                      <td>{item.numero}</td>
                      <td>{item.complemento}</td>
                      <td>{item.bairro}</td>
                      <td>{item.cep}</td>
                      <td>{item.unidade_saude}</td>
                      <td>{item.profissional}</td>
                      <td>{item.equipe}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>Nome</th>
                    <th>CPF</th>
                    <th>CNS</th>
                    <th>Data de Nasc</th>
                    <th>Unidade de Saúde</th>
                    <th>Profissional</th>
                    <th>Equipe</th>
                    <th>Atualizado</th>
                  </tr>
                </thead>
                <tbody>
                  {paginatedData.map((item, index) => (
                    <tr key={index}>
                      <td onClick={() => handleNameClick(item)} style={{ cursor: 'pointer', color: 'blue', textDecoration: 'underline' }}>
                        {item.nome}
                      </td>
                      <td>{item.cpf}</td>
                      <td>{item.cns}</td>
                      <td>{item.data_nascimento}</td>
                      <td>{item.unidade_saude}</td>
                      <td>{item.profissional}</td>
                      <td>{item.equipe}</td>
                      <td>{item.dt_atualizado}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
  
          <div className="modal-footer">
            {filteredData.length > 10 && (
              <Pagination
                currentPage={currentPage}
                setCurrentPage={(page) => setCurrentPage(page)}
                hasNextPage={filteredData.length > currentPage * 10}
              />
            )}
            <button onClick={handleExportToExcel} style={{ marginRight: '10px' }}>Exportar</button>
            <button onClick={() => setModalIsOpen(false)}>Fechar</button>
          </div>
        </div>
      </Modal>
  
      {miniModalIsOpen && (
        <div
          className={`mini-modal-overlay ${isLoading ? 'loading' : ''}`}
          onClick={() => setMiniModalIsOpen(false)}
        >
          <div
            className="mini-modal-content"
            onClick={(e) => e.stopPropagation()}
            style={{ cursor: isLoading ? 'wait' : 'default' }}
          >
            <div className="mini-modal-header">
              <h3>{isLoading ? '' : selectedCitizenName}</h3>
              <button className="close-button" onClick={() => setMiniModalIsOpen(false)}>✖</button>
            </div>
            <div className="mini-modal-details">
              {isLoading ? (
                <p>Carregando...</p>
              ) : selectedDetail && selectedDetail.details && Object.keys(selectedDetail.details).length > 0 ? (
                Object.entries(selectedDetail.details).map(([key, value]) => (
                  <p key={key}><strong>{key}:</strong> {value}</p>
                ))
              ) : (
                <p>Nenhum detalhe disponível</p>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
  
}

export default CadastrosPage;
