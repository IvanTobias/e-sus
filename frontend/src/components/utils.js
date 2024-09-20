// src/utils.js
import axios from 'axios';

// Função para capturar o progresso do backend
export const updateProgress = (tipo, setProgress, onComplete) => {
  const interval = setInterval(() => {
    axios.get(`http://127.0.0.1:5000/progress/${tipo}`)
      .then(response => {
        const progress = response.data.progress;
        setProgress(progress);
        if (progress >= 100) { // Alcançou 100%, chama a função de conclusão
          clearInterval(interval);
          if (onComplete) onComplete();
        }
      })
      .catch(error => {
        console.error('Erro ao acompanhar o progresso:', error);
        clearInterval(interval); // Para o intervalo em caso de erro
      });
  }, 2000); // A cada 2 segundos, ajusta conforme necessário
};

export const capitalize = (text) => {
  if (!text) return '';
  return text
    .toLowerCase()
    .split(' ')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
};

export const formatDate = (dateString) => {
  if (!dateString) return '';
  const date = new Date(dateString);
  return date.toLocaleDateString('pt-BR');
};

export const debounce = (func, delay) => {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => func(...args), delay);
  };
};
