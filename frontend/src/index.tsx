import React from 'react';
import ReactDOM from 'react-dom/client';
import { ConfigProvider, theme } from 'antd';
import App from './App';
import 'antd/dist/reset.css';
import './styles/global.css';

const root = ReactDOM.createRoot(document.getElementById('root') as HTMLElement);
root.render(
  <ConfigProvider
    theme={{
      algorithm: theme.darkAlgorithm,
      token: {
        colorPrimary: '#00d4ff',
        colorSuccess: '#10b981',
        colorWarning: '#ffc107',
        colorError: '#ef4444',
        colorInfo: '#00d4ff',
        colorBgContainer: 'rgba(15, 23, 42, 0.8)',
        colorBgElevated: 'rgba(30, 41, 59, 0.9)',
        colorBorder: 'rgba(100, 116, 139, 0.2)',
        colorText: '#f1f5f9',
        colorTextSecondary: '#94a3b8',
        borderRadius: 8,
        fontSize: 14,
      },
      components: {
        Button: {
          primaryShadow: '0 4px 15px rgba(0, 212, 255, 0.3)',
        },
        Card: {
          colorBgContainer: 'rgba(15, 23, 42, 0.8)',
        },
        Table: {
          colorBgContainer: 'transparent',
          headerBg: 'rgba(15, 23, 42, 0.6)',
          headerColor: '#00d4ff',
        },
        Tabs: {
          inkBarColor: '#00d4ff',
          itemActiveColor: '#00d4ff',
          itemHoverColor: '#00d4ff',
        },
        Input: {
          colorBgContainer: 'rgba(15, 23, 42, 0.6)',
        },
        Select: {
          colorBgContainer: 'rgba(15, 23, 42, 0.6)',
        },
      },
    }}
  >
    <App />
  </ConfigProvider>
);
