import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, App as AntApp } from 'antd';
import { AppLayout } from './components/Layout';
import { ErrorBoundary } from './components/ErrorBoundary';
import MarketCenter from './pages/MarketCenter';
import DataCenter from './pages/DataCenter';
import FactorCenter from './pages/FactorCenter/index';
import ConfigCenter from './pages/ConfigCenter';
import BacktestCenter from './pages/BacktestCenter';
import { useThemeStore } from './store';
import { darkTheme, lightTheme } from './theme';
import './styles/global.css';

const AppContent: React.FC = () => {
  const { mode } = useThemeStore();
  const themeConfig = mode === 'dark' ? darkTheme : lightTheme;

  return (
    <ConfigProvider theme={themeConfig}>
      <AntApp>
        <ErrorBoundary>
          <BrowserRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
            <AppLayout>
              <Routes>
                <Route path="/market" element={<ErrorBoundary><MarketCenter /></ErrorBoundary>} />
                <Route path="/" element={<ErrorBoundary><DataCenter /></ErrorBoundary>} />
                <Route path="/factor" element={<ErrorBoundary><FactorCenter /></ErrorBoundary>} />
                <Route path="/config" element={<ErrorBoundary><ConfigCenter /></ErrorBoundary>} />
                <Route path="/backtest" element={<ErrorBoundary><BacktestCenter /></ErrorBoundary>} />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Routes>
            </AppLayout>
          </BrowserRouter>
        </ErrorBoundary>
      </AntApp>
    </ConfigProvider>
  );
};

const App: React.FC = () => <AppContent />;

export default App;
