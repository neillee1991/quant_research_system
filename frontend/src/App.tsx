import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { ConfigProvider, App as AntApp } from 'antd';
import { AppLayout } from './components/Layout';
import MarketCenter from './pages/MarketCenter';
import DataCenter from './pages/DataCenter';
import FactorCenter from './pages/FactorCenter/index';
import StrategyCenter from './pages/StrategyCenter';
import ConfigCenter from './pages/ConfigCenter';
import { useThemeStore } from './store';
import { darkTheme, lightTheme } from './theme';
import './styles/global.css';

const AppContent: React.FC = () => {
  const { mode } = useThemeStore();
  const themeConfig = mode === 'dark' ? darkTheme : lightTheme;

  return (
    <ConfigProvider theme={themeConfig}>
      <AntApp>
        <BrowserRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
          <AppLayout>
            <Routes>
              <Route path="/market" element={<MarketCenter />} />
              <Route path="/" element={<DataCenter />} />
              <Route path="/factor" element={<FactorCenter />} />
              <Route path="/strategy" element={<StrategyCenter />} />
              <Route path="/config" element={<ConfigCenter />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </AppLayout>
        </BrowserRouter>
      </AntApp>
    </ConfigProvider>
  );
};

const App: React.FC = () => <AppContent />;

export default App;
