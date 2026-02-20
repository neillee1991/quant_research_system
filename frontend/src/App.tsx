import React from 'react';
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import { Layout, Menu } from 'antd';
import { DatabaseOutlined, FundOutlined, RobotOutlined } from '@ant-design/icons';
import DataCenter from './pages/DataCenter';
import StrategyLab from './pages/StrategyLab';
import MLAuto from './pages/MLAuto';
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
            ⚡ QuantResearch
          </div>
          <div style={{
            color: '#94a3b8',
            fontSize: 12,
            marginTop: '4px',
            letterSpacing: '0.5px'
          }}>
            AI-Powered Trading System
          </div>
        </div>
        <Menu
          theme="dark"
          mode="inline"
          style={{
            background: 'transparent',
            border: 'none'
          }}
          items={[
            {
              key: '/',
              icon: <DatabaseOutlined style={{ fontSize: 16 }} />,
              label: <NavLink to="/" style={{ fontWeight: 500 }}>Data Center</NavLink>
            },
            {
              key: '/strategy',
              icon: <FundOutlined style={{ fontSize: 16 }} />,
              label: <NavLink to="/strategy" style={{ fontWeight: 500 }}>Strategy Lab</NavLink>
            },
            {
              key: '/ml',
              icon: <RobotOutlined style={{ fontSize: 16 }} />,
              label: <NavLink to="/ml" style={{ fontWeight: 500 }}>AutoML</NavLink>
            },
          ]}
        />
      </Sider>
      <Layout style={{ background: 'transparent' }}>
        <Content style={{ minHeight: '100vh', padding: '24px' }}>
          <Routes>
            <Route path="/" element={<DataCenter />} />
            <Route path="/strategy" element={<StrategyLab />} />
            <Route path="/ml" element={<MLAuto />} />
          </Routes>
        </Content>
      </Layout>
    </Layout>
  </BrowserRouter>
);

export default App;
