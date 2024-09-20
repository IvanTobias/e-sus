const API_BASE_URL = 'http://0.0.0.0:5000/api'; // Substitua pela URL do seu backend

export const fetchUnidadesSaude = async () => {
  try {
    const response = await fetch(`${API_BASE_URL}/unidades-saude`);
    if (!response.ok) throw new Error('Erro ao buscar unidades de saúde.');
    return await response.json();
  } catch (error) {
    console.error('Erro ao buscar unidades de saúde:', error);
  }
};


