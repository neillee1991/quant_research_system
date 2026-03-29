/**
 * SQL 查询面板组件
 */
import React from 'react';
import { Card, Table, Button } from 'antd';
import { PlayCircleOutlined, ReloadOutlined, LinkOutlined } from '@ant-design/icons';
import Editor from '@monaco-editor/react';
import type { TableInfo } from '../../types';

interface DataTableProps {
  tables: TableInfo[];
  sqlQuery: string;
  queryResult: Record<string, unknown>[];
  queryColumns: string[];
  queryLoading: boolean;
  onSqlQueryChange: (sql: string) => void;
  onExecuteQuery: () => void;
  onTruncateTable: (tableName: string) => void;
  onRefreshTables: () => void;
  theme: 'light' | 'dark';
}

export const DataTable: React.FC<DataTableProps> = ({
  tables,
  sqlQuery,
  queryResult,
  queryColumns,
  queryLoading,
  onSqlQueryChange,
  onExecuteQuery,
  onTruncateTable,
  onRefreshTables,
  theme,
}) => {
  const tableColumns = [
    {
      title: '表名',
      dataIndex: 'table_name',
      key: 'table_name',
      width: 200,
      render: (text: string) => (
        <code style={{ color: 'var(--color-primary)', fontSize: '12px' }}>{text}</code>
      ),
    },
    {
      title: '行数',
      dataIndex: 'row_count',
      key: 'row_count',
      width: 100,
      render: (text: number) => text.toLocaleString(),
    },
    { title: '列数', dataIndex: 'column_count', key: 'column_count', width: 80 },
    {
      title: '列名',
      dataIndex: 'columns',
      key: 'columns',
      render: (text: string[]) => (
        <span
          style={{
            fontSize: '12px',
            wordBreak: 'break-word',
            whiteSpace: 'normal',
            lineHeight: '1.6',
          }}
        >
          {text.join(', ')}
        </span>
      ),
    },
    {
      title: '操作',
      key: 'action',
      width: 80,
      render: (_text: any, record: TableInfo) => (
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <Button
            danger
            size="middle"
            type="text"
            onClick={() => onTruncateTable(record.table_name)}
          >
            清空
          </Button>
        </div>
      ),
    },
  ];

  const queryResultColumns = queryColumns.map((col) => ({
    title: col,
    dataIndex: col,
    key: col,
    ellipsis: true,
    render: (text: any) => {
      if (text === null || text === undefined)
        return <span style={{ color: '#999' }}>NULL</span>;
      if (typeof text === 'number') return text.toLocaleString();
      return String(text);
    },
  }));

  return (
    <>
      <Card
        className="content-card"
        style={{ marginBottom: '12px' }}
        title={
          <span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>
            SQL 查询
          </span>
        }
        extra={
          <Button
            icon={<LinkOutlined />}
            onClick={() => window.open('http://localhost:8848', '_blank')}
            size="middle"
            type="text"
          >
            DolphinDB 后台
          </Button>
        }
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <div>
            <div
              style={{
                marginBottom: 8,
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <span style={{ fontSize: '13px', fontWeight: 500, color: 'var(--text-primary)' }}>
                SQL 语句
              </span>
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={onExecuteQuery}
                loading={queryLoading}
                size="middle"
              >
                执行查询
              </Button>
            </div>
            <div
              style={{
                border: '1px solid var(--border-color)',
                borderRadius: '6px',
                overflow: 'hidden',
              }}
            >
              <Editor
                height="200px"
                language="sql"
                value={sqlQuery}
                onChange={(value) => onSqlQueryChange(value || '')}
                theme={theme === 'dark' ? 'vs-dark' : 'light'}
                options={{
                  minimap: { enabled: false },
                  fontSize: 13,
                  lineNumbers: 'on',
                  scrollBeyondLastLine: false,
                  automaticLayout: true,
                }}
              />
            </div>
          </div>

          {queryResult.length > 0 && (
            <div>
              <div
                style={{
                  marginBottom: 8,
                  color: 'var(--color-primary)',
                  fontSize: '14px',
                  fontWeight: 600,
                }}
              >
                查询结果 ({queryResult.length} 行)
              </div>
              <Table
                dataSource={queryResult}
                columns={queryResultColumns}
                rowKey={(record: any) => JSON.stringify(record).slice(0, 100)}
                size="middle"
                pagination={{ pageSize: 50 }}
                scroll={{ x: 'max-content', y: 500 }}
              />
            </div>
          )}
        </div>
      </Card>

      <Card
        className="content-card"
        style={{ marginTop: 12 }}
        title={
          <span style={{ color: 'var(--text-primary)', fontSize: '16px', fontWeight: 600 }}>
            数据表管理
          </span>
        }
        extra={
          <Button icon={<ReloadOutlined />} onClick={onRefreshTables} size="middle">
            刷新
          </Button>
        }
      >
        <Table
          dataSource={tables}
          columns={tableColumns}
          rowKey="table_name"
          size="middle"
          pagination={false}
          scroll={{ x: 'max-content' }}
        />
      </Card>
    </>
  );
};
