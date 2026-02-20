import React from 'react';
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import { Layout, Menu } from 'antd';
import { DatabaseOutlined, ExperimentOutlined, FundOutlined } from '@ant-design/icons';
import DataCenter from './pages/DataCenter';
import FactorCenter from './pages/FactorCenter';
import StrategyCenter from './pages/StrategyCenter';
import './styles/global.css';

const { Sider, Content } = Layout;

const App: React.FC = () => (
  <BrowserRouter>
    <div className="tech-background" />
    <Layout style={{ minHeight: '100vh', background: 'transparent', position: 'relative', zIndex: 1 }}>
      <Sider
        theme="dark"
        style={{
          background: 'rgba(10, 14, 39, 0.95)',
          borderRight: '1px solid rgba(0, 212, 255, 0.2)',
          backdropFilter: 'blur(10px)'
        }}
      >
        <div style={{
          padding: '24px 16px',
          background: 'linear-gradient(135deg, rgba(0, 212, 255, 0.1), rgba(124, 58, 237, 0.1))',
          borderBottom: '1px solid rgba(0, 212, 255, 0.3)',
          marginBottom: '16px'
        }}>
          <div style={{
            color: '#00d4ff',
            fontWeight: 700,
            fontSize: 20,
            textShadow: '0 0 10px rgba(0, 212, 255, 0.5)',
            letterSpacing: '1px'
          }}>
            ⚡ 量化研究系统
          </div>
          <div style={{
            color: '#94a3b8',
            fontSize: 12,
            marginTop: '4px',
            letterSpacing: '0.5px'
          }}>
            AI 驱动的量化交易平台
          </div>
        </div>
        <Menu
          theme="dark"
          mode="inline"
          style={{ background: 'transparent', border: 'none' }}
          items={[
            {
              key: '/',
              icon: <DatabaseOutlined style={{ fontSize: 16 }} />,
              label: <NavLink to="/" style={{ fontWeight: 500 }}>数据中心</NavLink>
            },
            {
              key: '/factor',
              icon: <ExperimentOutlined style={{ fontSize: 16 }} />,
              label: <NavLink to="/factor" style={{ fontWeight: 500 }}>因子中心</NavLink>
            },
            {
              key: '/strategy',
              icon: <FundOutlined style={{ fontSize: 16 }} />,
              label: <NavLink to="/strategy" style={{ fontWeight: 500 }}>策略中心</NavLink>
            },
          ]}
        />
      </Sider>
      <Layout style={{ background: 'transparent' }}>
        <Content style={{ minHeight: '100vh', padding: '24px' }}>
          <Routes>
            <Route path="/" element={<DataCenter />} />
            <Route path="/factor" element={<FactorCenter />} />
            <Route path="/strategy" element={<StrategyCenter />} />
          </Routes>
        </Content>
      </Layout>
    </Layout>
  </BrowserRouter>
);

export default App;
