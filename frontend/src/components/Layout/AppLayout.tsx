import React from 'react';
import { Layout } from '@douyinfe/semi-ui';
import Sidebar from './Sidebar';
import TopBar from './TopBar';

const { Content } = Layout;

interface AppLayoutProps {
  children: React.ReactNode;
}

const AppLayout: React.FC<AppLayoutProps> = ({ children }) => {
  return (
    <Layout style={{ display: 'flex', flexDirection: 'row', height: '100vh' }}>
      <Sidebar />
      <Layout style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <TopBar />
        <Content
          style={{
            padding: 20,
            overflow: 'auto',
            height: 'calc(100vh - 48px)',
            background: 'var(--bg-app)',
          }}
        >
          {children}
        </Content>
      </Layout>
    </Layout>
  );
};

export default AppLayout;
