//App.js
import React, { useState } from 'react';
import Sidebar from './components/Sidebar';
import ImportData from './components/ImportData';
import ConfigDatabase from './components/ConfigDatabase';
// import BPAForm from './components/BPAForm'; // Keep or remove based on final decision
import BPAConfig from './components/BPAConfig'; // Import new BPA Config component
import BPAGeneration from './components/BPAGeneration'; // Import new BPA Generation component
import CadastrosPage from './components/CadastrosPage';
import Home from './components/Home'; // Assuming Home exists or will be created
import DashboardPage from './components/DashboardPage'; // Import DashboardPage (Original Fiocruz)
import VisitaPage from './components/VisitasPage';
import IAFPSEPage from './components/IAFPSEPage';
import ProntuarioSupport from './components/ProntuarioSupport'; // Import ProntuarioSupport
import FiocruzIntegrationPage from './components/FiocruzIntegrationPage'; // Importar o componente de Integração Fiocruz

// Import new Power BI style dashboards
import CadastrosACSDashboard from './components/CadastrosACSDashboard';
import VisitasACSDashboard from './components/VisitasACSDashboard';
import VacinacaoDashboard from './components/VacinacaoDashboard';
import PrevineBrasilDashboard from './components/PrevineBrasilDashboard';

import './styles/styles.css';

function App() {
  // Define 'config' como página inicial, conforme Sidebar.js
  const [currentPage, setCurrentPage] = useState('config');

  // Função para carregar o conteúdo com base na escolha do usuário
  const loadContent = (page) => {
    setCurrentPage(page.toLowerCase());  // Garante que o valor será minúsculo
  };

  // Determine if the sidebar should be shown
  // Hide sidebar for the TV panel view
  const showSidebar = currentPage !== 'painel_tv';

  return (
    <div className={`App ${!showSidebar ? 'no-sidebar' : ''}`}>
      {showSidebar && <Sidebar onLoadContent={loadContent} />}
      <div className="content">
        {/* Renderiza o componente baseado na página atual selecionada */}
        {currentPage === 'home' && <Home />} {/* Example Home route */}
        {currentPage === 'config' && <ConfigDatabase />}
        {currentPage === 'import' && <ImportData />}
        {currentPage === 'fiocruz_integration' && <FiocruzIntegrationPage />} {/* Nova rota para Integração Fiocruz */}
        {/* Relatórios Originais */}
        {currentPage === 'cadastro' && <CadastrosPage />}
        {currentPage === 'visitas' && <VisitaPage />}
        {currentPage === 'iaf_pse' && <IAFPSEPage />}
        {currentPage === 'dashboards' && <DashboardPage />} {/* Original Fiocruz Dashboards */}
        {/* Novos Dashboards (Power BI Style) */}
        {currentPage === 'db_cadastros_acs' && <CadastrosACSDashboard />}
        {currentPage === 'db_visitas_acs' && <VisitasACSDashboard />}
        {currentPage === 'db_vacinacao' && <VacinacaoDashboard />}
        {currentPage === 'db_previne' && <PrevineBrasilDashboard />}
        {/* BPA */}
        {currentPage === 'bpa_config' && <BPAConfig />} {/* New BPA Config route */}
        {currentPage === 'bpa_generate' && <BPAGeneration />} {/* New BPA Generation route */}
        {/* Apoio */}
        {currentPage === 'prontuario_support' && <ProntuarioSupport />} {/* Route for Prontuario Support */}
        {/* Adicione outras condições conforme necessário */}
        {/* Default or fallback page could be added here */}
      </div>
    </div>
  );
}

export default App;
