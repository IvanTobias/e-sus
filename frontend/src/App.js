import React, { useState } from 'react';
import Sidebar from './components/Sidebar';
import ImportData from './components/ImportData';
import ConfigDatabase from './components/ConfigDatabase';
import BPAForm from './components/BPAForm';
import CadastrosPage from './components/CadastrosPage';
import Home from './components/Home';
import './styles/styles.css';
import VisitaPage from './components/VisitasPage';

function App() {
  // Define 'home' como página inicial
  const [currentPage, setCurrentPage] = useState('home');

  // Função para carregar o conteúdo com base na escolha do usuário
  const loadContent = (page) => {
    setCurrentPage(page.toLowerCase());  // Garante que o valor será minúsculo
  };

  return (
    <div className="App">
      {/* Aqui você passa a função loadContent corretamente */}
      <Sidebar onLoadContent={loadContent} /> {/* Passando a função para o Sidebar */}
      <div className="content">
        {/* Renderiza o componente baseado na página atual selecionada */}
        {currentPage === 'home' && <Home />}
        {currentPage === 'config' && <ConfigDatabase />}
        {currentPage === 'import' && <ImportData />}
        {currentPage === 'bpa' && <BPAForm />}
        {currentPage === 'cadastro' && <CadastrosPage />}
        {currentPage === 'visitas' && <VisitaPage />}
        {/* Adicione outras condições conforme necessário */}
      </div>
    </div>
  );
}

export default App;
