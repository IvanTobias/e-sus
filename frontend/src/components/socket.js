import { io } from 'socket.io-client';

// Garante que o código só execute em ambiente browser
const API_BASE_URL =
  typeof window !== 'undefined'
    ? `http://${window.location.hostname}:5000`
    : 'http://localhost:5000'; // Fallback para SSR ou testes

const socket = io(API_BASE_URL, {
  path: '/socket.io',
  transports: ['websocket', 'polling'], // Permitir fallback para polling
  reconnection: true,
  reconnectionAttempts: 5,
  reconnectionDelay: 1000, // Atraso inicial menor
  reconnectionDelayMax: 5000, // Atraso máximo maior
  randomizationFactor: 0.5, // Variação aleatória no atraso
  timeout: 20000, // Timeout para conexão inicial
});

console.log(`[SOCKET] Tentando conectar ao WebSocket: ${API_BASE_URL}`);

// Adicionar logs para eventos de conexão
socket.on('connect', () => {
  console.log('[SOCKET] Conectado ao WebSocket');
});

socket.on('connect_error', (error) => {
  console.error('[SOCKET] Erro de conexão:', error.message);
});

socket.on('reconnect_attempt', (attempt) => {
  console.log(`[SOCKET] Tentativa de reconexão #${attempt}`);
});

socket.on('reconnect_failed', () => {
  console.error('[SOCKET] Falha ao reconectar após múltiplas tentativas');
});

export default socket;