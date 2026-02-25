import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { AppLayout } from './components/Layout';
import MarketCenter from './pages/MarketCenter';
import DataCenter from './pages/DataCenter';
import FactorCenter from './pages/FactorCenter';
import StrategyCenter from './pages/StrategyCenter';
import SchedulerCenter from './pages/SchedulerCenter';
import './styles/global.css';

const App: React.FC = () => (
  <BrowserRouter>
    <AppLayout>
      <Routes>
        <Route path="/market" element={<MarketCenter />} />
        <Route path="/" element={<DataCenter />} />
        <Route path="/factor" element={<FactorCenter />} />
        <Route path="/strategy" element={<StrategyCenter />} />
        <Route path="/scheduler" element={<SchedulerCenter />} />
      </Routes>
    </AppLayout>
  </BrowserRouter>
);

export default App;
