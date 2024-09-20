import React from 'react';

// Função para buscar o ícone correspondente ao tipo de card
const getIconForCard = (key) => {
  switch (key.toLowerCase()) {
    case 'cadastros domiciliares':
      return <i className="fas fa-home" aria-hidden="true"></i>; // Ícone de casa
    case 'cadastros individuais':
      return <i className="fas fa-user" aria-hidden="true"></i>; // Ícone de pessoa
    case 'cadastros com cns':
      return <i className="fas fa-user" aria-hidden="true"></i>; // Ícone de pessoa
    case 'cadastros com cpf':
      return <i className="fas fa-user" aria-hidden="true"></i>; // Ícone de pessoa
    case 'cadastros desatualizados':
      return <i className="fas fa-user" aria-hidden="true"></i>; // Ícone de pessoa
    case 'moradores de rua':
      return <i className="fas fa-street-view" aria-hidden="true"></i>; // Ícone de rua
    case 'óbitos':
      return <i className="fas fa-skull-crossbones" aria-hidden="true"></i>; // Ícone de óbito
    case 'mudou-se':
      return <i className="fas fa-arrow-right" aria-hidden="true"></i>; // Ícone de mudança
    case 'cadastros ativos':
      return <i className="fas fa-check-circle" aria-hidden="true"></i>; // Ícone de ativos
    case 'fora de área':
      return <i className="fas fa-map-marker-alt" aria-hidden="true"></i>; // Ícone de fora de área
    default:
      return <i className="fas fa-info-circle" aria-hidden="true"></i>; // Ícone padrão
  }
};

function Card({ title, count, onClick, animationDelay }) {
  // Obtém o ícone baseado no título do card
  const icon = getIconForCard(title);

  return (
    <div
      className="card fade-in-cards"
      onClick={onClick}
      style={{ animationDelay: `${animationDelay}ms` }} // Adiciona atraso na animação
      role="button" // Indica que o card é clicável como um botão
      tabIndex={0} // Permite que o card seja focado com o teclado
    >
      <h3>
        {icon} {title}
      </h3>
      <p className="count">{count}</p>
    </div>
  );
}

export default Card;
