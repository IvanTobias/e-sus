const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://127.0.0.1:5000/api';

export const fetchUnidadesSaude = async () => {
  try {
    const response = await fetch(`${API_BASE_URL}/unidades-saude`);
    if (!response.ok) throw new Error('Erro ao buscar unidades de saúde.');
    return await response.json();
  } catch (error) {
    console.error('Erro ao buscar unidades de saúde:', error);
  }
};


