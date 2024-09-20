import React, { useState } from 'react';

function Sidebar({ onLoadContent }) {
  // Estado para armazenar o módulo selecionado
  const [selectedModule, setSelectedModule] = useState('cadastro'); // Assume que "cadastro" é o padrão

  // Função para manipular o clique no módulo
  const handleModuleClick = (module) => {
    setSelectedModule(module); // Atualiza o módulo selecionado
    onLoadContent(module); // Chama a função passada para carregar o conteúdo
  };

  return (
    <div className="sidebar">
      <h3>Módulos</h3>
      <a 
        className={selectedModule === 'config' ? 'active' : ''}
        onClick={() => handleModuleClick('config')}
      >
        Configuração do Banco
      </a>
      <a 
        className={selectedModule === 'import' ? 'active' : ''}
        onClick={() => handleModuleClick('import')}
      >
        Importar Dados
      </a>
      <a 
        className={selectedModule === 'cadastro' ? 'active' : ''}
        onClick={() => handleModuleClick('cadastro')}
      >
        Cadastros
      </a>
      <a 
        className={selectedModule === 'visitas' ? 'active' : ''}
        onClick={() => handleModuleClick('visitas')}
      >
        Visitas
      </a>
      <a 
        className={selectedModule === 'bpa' ? 'active' : ''}
        onClick={() => handleModuleClick('bpa')}
      >
        Gerador BPA
      </a>
    </div>
  );
}

export default Sidebar;
