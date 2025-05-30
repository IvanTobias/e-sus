import React, { useState } from 'react';
import { Home, Settings, UploadCloud, FileText, Users, UserCheck, Activity, BarChart2, Tv, Edit, PhoneCall, MessageSquare, Tool, BookOpen, Map, Shield, Heart, Activity as ActivityIcon } from 'react-feather'; // Importando mais ícones

function Sidebar({ onLoadContent }) {
  const [selectedModule, setSelectedModule] = useState('config'); // Iniciar com 'config' selecionado

  const handleModuleClick = (module) => {
    setSelectedModule(module);
    // Handle special case for TV panel - maybe open in new window or just navigate
    if (module === 'painel_tv') {
      // Option 1: Open in new window
      // window.open('/painel_tv', '_blank'); // This requires client-side routing setup
      // Option 2: Navigate within the app (as currently implemented in App.js)
      onLoadContent(module);
    } else {
      onLoadContent(module);
    }
  };

  // Módulos e seus ícones correspondentes
  const modules = [
    { name: 'Configuração DB', id: 'config', icon: Settings, section: 'Config' },
    { name: 'Importar Dados', id: 'import', icon: UploadCloud, section: 'Dados' },
    // Relatórios Originais
    { name: 'Cadastros Orig.', id: 'cadastro', icon: Users, section: 'Relatórios Originais' },
    { name: 'Visitas Orig.', id: 'visitas', icon: UserCheck, section: 'Relatórios Originais' },
    { name: 'IAF / PSE Orig.', id: 'iaf_pse', icon: Activity, section: 'Relatórios Originais' },
    { name: 'Dashboards Orig.', id: 'dashboards', icon: BarChart2, section: 'Relatórios Originais' }, // Fiocruz Dashboards
    // Novos Dashboards (Power BI Style)
    { name: 'Cadastros ACS', id: 'db_cadastros_acs', icon: Users, section: 'Dashboards (Novo)' },
    { name: 'Visitas ACS', id: 'db_visitas_acs', icon: Map, section: 'Dashboards (Novo)' },
    { name: 'Vacinação', id: 'db_vacinacao', icon: Shield, section: 'Dashboards (Novo)' },
    { name: 'Previne Brasil', id: 'db_previne', icon: Heart, section: 'Dashboards (Novo)' },
    // BPA
    { name: 'Configurar BPA', id: 'bpa_config', icon: Tool, section: 'BPA' },
    { name: 'Gerar BPA', id: 'bpa_generate', icon: FileText, section: 'BPA' },
 // Apoio
    { name: 'Apoio Prontuário', id: 'prontuario_support', icon: BookOpen, section: 'Apoio' },
    // { name: 'Chatbot', id: 'chatbot', icon: MessageSquare, section: 'Outros' }, // Future module
  ];

  // Group modules by section
  const sections = modules.reduce((acc, module) => {
    acc[module.section] = acc[module.section] || [];
    acc[module.section].push(module);
    return acc;
  }, {});

  // Define order of sections
  const sectionOrder = ['Config', 'Dados', 'Dashboards (Novo)', 'Relatórios Originais', 'BPA', 'Senhas', 'Apoio'];

  return (
    <div className="sidebar">
      {sectionOrder.map(sectionName => {
        const sectionModules = sections[sectionName];
        if (!sectionModules) return null; // Skip if section doesn't exist for some reason
        return (
          <React.Fragment key={sectionName}>
            <h3>{sectionName}</h3>
            {sectionModules.map(module => {
              const Icon = module.icon; // Pega o componente do ícone
              return (
                <a
                  key={module.id}
                  href="#" // Use href="#" for accessibility and prevent page reload
                  className={selectedModule === module.id ? 'active' : ''}
                  onClick={(e) => {
                    e.preventDefault(); // Prevent default anchor behavior
                    handleModuleClick(module.id);
                  }}
                >
                  <Icon size={18} /> {/* Renderiza o ícone */}
                  {module.name}
                </a>
              );
            })}
          </React.Fragment>
        );
      })}
    </div>
  );
}

export default Sidebar;

