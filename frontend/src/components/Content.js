// src/components/Content.js
import React from 'react';

function Content({ content }) {
  return (
    <div className="content">
      <h1>Bem-vindo ao Sistema</h1>
      <p>Escolha um módulo à esquerda para começar.</p>
      <div dangerouslySetInnerHTML={{ __html: content }}></div> {/* Renderiza o conteúdo HTML dinamicamente */}
    </div>
  );
}

export default Content;
