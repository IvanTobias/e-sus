import React, { useState, useCallback, useEffect, useMemo } from 'react';
import Select from 'react-select';
import DatePicker from 'react-datepicker';
import 'react-datepicker/dist/react-datepicker.css';
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css'; 
import 'leaflet-extra-markers/dist/css/leaflet.extra-markers.min.css';  
import 'leaflet-extra-markers/dist/js/leaflet.extra-markers.min.js';   
import L from 'leaflet';
import Modal from 'react-modal';

Modal.setAppElement('#root');

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://127.0.0.1:5000';

// Criando ícones personalizados
const iconAbsent = L.ExtraMarkers.icon({
  icon: 'fa-number',
  markerColor: 'yellow',
  shape: 'circle',
  prefix: 'fa',
});

const iconRealized = L.ExtraMarkers.icon({
  icon: 'fa-check',
  markerColor: 'green',
  shape: 'circle',
  prefix: 'fa',
});

const iconRefused = L.ExtraMarkers.icon({
  icon: 'fa-times',
  markerColor: 'red',
  shape: 'circle',
  prefix: 'fa',
});

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

function VisitaPage() {
  const [startDate, setStartDate] = useState(new Date());
  const [endDate, setEndDate] = useState(new Date());
  const [unidadesData, setUnidadesData] = useState([]);
  const [unidades, setUnidades] = useState([]);
  const [equipes, setEquipes] = useState([]);
  const [profissionais, setProfissionais] = useState([]);
  const [mapMarkers, setMapMarkers] = useState([]);
  const [countRealizadas, setCountRealizadas] = useState(0);
  const [countRealizadasWithoutGeo, setCountRealizadasWithoutGeo] = useState(0);
  const [countAusentes, setCountAusentes] = useState(0);
  const [countAusentesWithoutGeo, setCountAusentesWithoutGeo] = useState(0);
  const [countRecusadas, setCountRecusadas] = useState(0);
  const [countRecusadasWithoutGeo, setCountRecusadasWithoutGeo] = useState(0);
  const [selectedTab, setSelectedTab] = useState('visitas');  // Define a aba inicial

  const handleTabChange = (tab) => {
    setSelectedTab(tab);  // Atualiza a aba ativa
  };
  
  // Função para carregar dados das unidades de saúde
  const loadUnidadesSaude = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/visitas-domiciliares?tipo_consulta=filtros`);
      const data = await response.json();
      setUnidadesData(data);
    } catch (error) {
      console.error('Erro ao buscar unidades de saúde:', error);
    }
  }, []);

  useEffect(() => {
    loadUnidadesSaude();  // Carrega as unidades de saúde quando o componente é montado
  }, [loadUnidadesSaude]);

  const loadVisitasMapa = useCallback(async () => {
    try {
      const queryParams = new URLSearchParams({
        unidade_saude: unidades.join(','),
        equipe: equipes.join(','),
        profissional: profissionais.join(','),
        start_date: startDate.toISOString().split('T')[0],
        end_date: endDate.toISOString().split('T')[0],
        tipo_consulta: 'mapa',
      });
  
      const response = await fetch(`${API_BASE_URL}/api/visitas-domiciliares?${queryParams}`);
      const data = await response.json();
  
      // Separar as visitas em duas categorias: com e sem geolocalização
      const visitsWithGeo = data.filter(item => item.nu_latitude && item.nu_longitude);
      const visitsWithoutGeo = data.filter(item => !item.nu_latitude || !item.nu_longitude);
  
      // Calcular contagens para as visitas com geolocalização
      const markers = visitsWithGeo.map(item => ({
        position: [item.nu_latitude, item.nu_longitude],
        unidade_saude: item.no_unidade_saude,
        profissional: item.no_profissional,
        equipe: item.no_equipe,
        dt_visita: item.dt_visita,
        status: item.co_dim_desfecho_visita,
      }));
  
      setCountRealizadas(markers.filter(marker => marker.status === 1).length);
      setCountAusentes(markers.filter(marker => marker.status === 3).length);
      setCountRecusadas(markers.filter(marker => marker.status === 2).length);
  
      // Calcular contagens para as visitas sem geolocalização
      const countRealizadasWithoutGeo = visitsWithoutGeo.filter(item => item.co_dim_desfecho_visita === 1).length;
      const countAusentesWithoutGeo = visitsWithoutGeo.filter(item => item.co_dim_desfecho_visita === 3).length;
      const countRecusadasWithoutGeo = visitsWithoutGeo.filter(item => item.co_dim_desfecho_visita === 2).length;
  
      setMapMarkers(markers);
  
      // Armazenar as contagens das visitas sem geolocalização
      setCountRealizadasWithoutGeo(countRealizadasWithoutGeo);
      setCountAusentesWithoutGeo(countAusentesWithoutGeo);
      setCountRecusadasWithoutGeo(countRecusadasWithoutGeo);
  
    } catch (error) {
      console.error('Erro ao carregar visitas para o mapa:', error);
    }
  }, [unidades, equipes, profissionais, startDate, endDate]);
  
  
  // Define o ícone pelo status
  const getIconByStatus = (status) => {
    switch (status) {
      case 1: return iconRealized;
      case 2: return iconRefused;
      case 3: return iconAbsent;
      default: return iconRealized;
    }
  };

  // Renderiza os markers no mapa
  const filteredMarkers = useMemo(() => 
    mapMarkers.map((marker, index) => (
      <Marker key={index} position={marker.position} icon={getIconByStatus(marker.status)}>
        <Popup>
          <strong>Unidade de Saúde:</strong> {marker.unidade_saude}<br />
          <strong>Profissional:</strong> {marker.profissional}<br />
          <strong>Equipe:</strong> {marker.equipe}<br />
          <strong>Data da Visita:</strong> {marker.dt_visita}<br />
          <strong>Status:</strong> {marker.status === 1 ? 'Realizada' : marker.status === 2 ? 'Recusada' : 'Ausente'}
        </Popup>
      </Marker>
    ))
  , [mapMarkers]);

  // Lógica de opções dos filtros
  const unidadeOptions = useMemo(() => 
    [...new Set(unidadesData.map((item) => item.no_unidade_saude))].map((u) => ({
      value: u,
      label: u,
    })), [unidadesData]
  );
  
  const equipeOptions = useMemo(() =>
    [...new Set(unidadesData.filter((item) => unidades.includes(item.no_unidade_saude)).map((item) => item.no_equipe))].map((e) => ({ value: e, label: e })), 
    [unidades, unidadesData]
  );
  
  const profissionalOptions = useMemo(() =>
    [...new Set(unidadesData.filter((item) => equipes.includes(item.no_equipe)).map((item) => item.no_profissional))].map((p) => ({ value: p, label: p })), 
    [equipes, unidadesData]
  );

  const handleUnidadeChange = (selected) => {
    const selectedValues = selected.map((option) => option.value);
    setUnidades(selectedValues);
    setEquipes([]);
    setProfissionais([]);
  };
  
  const handleEquipeChange = (selected) => {
    const selectedValues = selected.map((option) => option.value);
    setEquipes(selectedValues);
  };
  
  const handleProfissionalChange = (selected) => {
    const selectedValues = selected.map((option) => option.value);
    setProfissionais(selectedValues);
  };

  return (
    <div className="container">
      {/* Tabs */}
      <div className="tabs">
        <button
          className={selectedTab === 'visitas' ? 'tab active' : 'tab'}
          onClick={() => handleTabChange('visitas')}
        >
          Visitas
        </button>
        <button
          className={selectedTab === 'relatorio' ? 'tab active' : 'tab'}
          onClick={() => handleTabChange('relatorio')}
        >
          Relatório
        </button>
      </div>
      <br />
      {/* Conteúdo das Abas */}
      {selectedTab === 'visitas' && (
        <div className="form-content">
          <div className="date-picker">
            <label>Filtrar por Data:</label>
            <div className="date-inputs">
              <DatePicker selected={startDate} onChange={setStartDate} dateFormat="dd/MM/yyyy" />
              <DatePicker selected={endDate} onChange={setEndDate} dateFormat="dd/MM/yyyy" />
            </div>
          </div>

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
          <div>
            <button onClick={loadVisitasMapa}>Carregar Visitas</button>   
          </div>
          <br />
          <MapContainer center={[-23.3945, -46.3144]} zoom={13} style={{ height: "500px", width: "100%" }}>
            <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
            {filteredMarkers}
          </MapContainer>
          <br />

          {/* Resumo das Visitas */}
          <div className="visitas-container">
        {/* Resumo das Visitas com geolocalização */}
            <div className="visitas-summary">
                <h4>Visitas com Geolocalização</h4>
                <p><strong>Visitas Realizadas:</strong> {countRealizadas}</p>
                <p><strong>Visitas Ausentes:</strong> {countAusentes}</p>
                <p><strong>Visitas Recusadas:</strong> {countRecusadas}</p>
            </div>
            {/* Resumo das Visitas sem geolocalização */}
            <div className="visitas-summary">
                <h4>Visitas sem Geolocalização</h4>
                <p><strong>Visitas Realizadas:</strong> {countRealizadasWithoutGeo}</p>
                <p><strong>Visitas Ausentes:</strong> {countAusentesWithoutGeo}</p>
                <p><strong>Visitas Recusadas:</strong> {countRecusadasWithoutGeo}</p>
            </div>
            </div>
        </div>
      )}

      {selectedTab === 'relatorio' && (
        <div>
          <div className="date-picker">
            <label>Filtrar por Data:</label>
            <div className="date-inputs">
              <DatePicker selected={startDate} onChange={setStartDate} dateFormat="dd/MM/yyyy" />
              <DatePicker selected={endDate} onChange={setEndDate} dateFormat="dd/MM/yyyy" />
            </div>
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
          </div>
        </div>
      )}
    </div>
  );
}

export default VisitaPage;
