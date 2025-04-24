// src/socket.js
import { io } from 'socket.io-client';

// Garante que o código só execute em ambiente browser
const API_BASE_URL =
  typeof window !== 'undefined'
    ? `http://${window.location.hostname}:5000`
    : 'http://localhost:5000'; // fallback para SSR ou testes

const socket = io(API_BASE_URL, {
  path: '/socket.io',
  transports: ['websocket'],
  reconnection: true,
  reconnectionAttempts: 5,
  reconnectionDelay: 2000,
});

export default socket;
