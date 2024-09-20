// src/App.js
import React, { useState } from 'react';
import Sidebar from './components/Sidebar';
import ImportData from './components/ImportData';
import ConfigDatabase from './components/ConfigDatabase';
import BPAForm from './components/BPAForm';
import CadastrosPage from './components/CadastrosPage';  // Importa o componente de Cadastros
import './styles/styles.css';


function App() {
  const [currentPage, setCurrentPage] = useState('home'); // Define qual página mostrar

  // Função para carregar o conteúdo com base na escolha do usuário
  const loadContent = (page) => {
    setCurrentPage(page);  // Atualiza o estado com o nome da página a ser exibida
  };

  return (
    <div className="App">
      <Sidebar onLoadContent={loadContent} /> {/* Sidebar recebe a função para alterar o conteúdo */}
      <div className="content">
        {/* Renderiza o componente baseado na página atual selecionada */}
        {currentPage === 'config' && <ConfigDatabase />}
        {currentPage === 'import' && <ImportData />}
        {currentPage === 'bpa' && <BPAForm />}
        {currentPage === 'cadastro' && <CadastrosPage />}
        {/* Adicione outras condições conforme necessário */}
      </div>
    </div>
  );
}

export default App;
