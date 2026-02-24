import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './styles/global.css';

// Initialize theme from localStorage
const savedTheme = localStorage.getItem('theme-mode') || 'dark';
document.body.setAttribute('theme-mode', savedTheme);

const root = ReactDOM.createRoot(document.getElementById('root') as HTMLElement);
root.render(<App />);
