import React, { useEffect, useState } from 'react';

function Modal({ message, onClose }) {
  // Estado para controlar a animação
  const [isVisible, setIsVisible] = useState(false);

  // useEffect para controlar o fade-in quando o modal é aberto
  useEffect(() => {
    if (message) {
      // Ativa o fade-in
      setIsVisible(true);
    } else {
      // Remove o fade-in após um pequeno atraso para garantir que o fade-out seja visto
      setTimeout(() => setIsVisible(false), 300); // Tempo correspondente ao da animação
    }
  }, [message]);

  // Definir a classe do modal dinamicamente
  const modalClassName = `modal ${isVisible ? 'fade-in' : 'fade-out'}`;

  return (
    <div className={modalClassName}>
      <div className="modal-content">
        <span className="close" onClick={onClose}>&times;</span>
        <p>{message}</p>
      </div>
    </div>
  );
}

export default Modal;
