/**
 * 因子代码测试面板
 */

import React, { useState } from 'react';
import {
  Button, Alert, Select, Table, Tag,
} from 'antd';
import { WarningOutlined } from '@ant-design/icons';
import { useMessage } from '../../hooks/useMessage';
import QuantDatePicker from '../../components/QuantDatePicker';
import { productionApi } from '../../api';
import type { PreprocessOptions } from '../../types';
import type { TestResult, TestResultData, TestLog } from './types';

interface TestPanelProps {
  code: string;
  dependsOn?: string[];
  preprocess?: PreprocessOptions;
  lookbackDays?: number;  // 回溯天数
}

const TestPanel: React.FC<TestPanelProps> = ({ code, dependsOn, preprocess, lookbackDays = 60 }) => {
  const message = useMessage();
  const [dateRange, setDateRange] = useState<[string, string]>(['', '']);
  const [testing, setTesting] = useState<boolean>(false);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [testError, setTestError] = useState<string | null>(null);
  const [testLogs, setTestLogs] = useState<TestLog[]>([]);
  const [testStdout, setTestStdout] = useState<string>('');
  const [filterStock, setFilterStock] = useState<string | undefined>(undefined);
  const [filterDate, setFilterDate] = useState<string | undefined>(undefined);
  const [showLogs, setShowLogs] = useState<boolean>(true);

  const handleTest = async (): Promise<void> => {
    if (!code.trim()) {
      message.warning('请先编写因子代码');
      return;
    }
    if (!dateRange[0] || !dateRange[1]) {
      message.warning('请选择测试日期范围');
      return;
    }

    setTesting(true);
    setTestResult(null);
    setTestError(null);
    setTestLogs([]);
    setTestStdout('');
    setFilterStock(undefined);
    setFilterDate(undefined);

    try {
      const res = await productionApi.testFactorCode({
        code,
        start_date: dateRange[0],
        end_date: dateRange[1],
        depends_on: dependsOn || ['sync_daily_data'],
        params: {},
        preprocess: preprocess || undefined,
        lookback_days: lookbackDays,
      });

      const d = res.data;
      if (d.status === 'error') {
        setTestError(`[${d.phase}] ${d.error}`);
        setTestLogs(d.logs || []);
        setTestStdout(d.stdout || '');
      } else {
        setTestResult(d.data);
        setTestLogs(d.data?.logs || []);
        setTestStdout(d.data?.stdout || '');
      }
    } catch (e) {
      const error = e as { response?: { data?: { detail?: string } }; message?: string };
      setTestError(error.response?.data?.detail || error.message || '测试请求失败');
    } finally {
      setTesting(false);
    }
  };

  const filteredPreview: TestResultData[] = testResult?.preview?.filter((row) => {
    if (filterStock && row.ts_code !== filterStock) return false;
    if (filterDate && row.trade_date !== filterDate) return false;
    return true;
  }) || [];

  const resultColumns = [
    { title: '股票代码', dataIndex: 'ts_code', key: 'ts_code', width: 120 },
    { title: '交易日期', dataIndex: 'trade_date', key: 'trade_date', width: 110 },
    {
      title: '因子值',
      dataIndex: 'factor_value',
      key: 'factor_value',
      width: 140,
      render: (v: number) => v != null ? v.toFixed(6) : <span style={{ color: 'var(--text-muted)' }}>null</span>,
    },
  ];

  const logColorMap: Record<string, string> = {
    info: '#94a3b8',
    error: '#ef4444',
    warn: '#f59e0b',
  };

  const phaseColorMap: Record<string, string> = {
    compile: '#818cf8',
    exec: '#38bdf8',
    resolve: '#a78bfa',
    data: '#34d399',
    compute: '#fbbf24',
    validate: '#fb923c',
    result: '#22d3ee',
  };

  return (
    <div style={{ marginTop: 8, borderTop: '1px solid var(--border-color)', paddingTop: 8 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <span style={{ color: 'var(--text-secondary)', fontSize: 12, whiteSpace: 'nowrap' }}>因子计算区间:</span>
        <QuantDatePicker
          style={{ flex: 1 }}
          onChange={(s, e) => setDateRange([s, e])}
        />
        <Button
          size="small"
          type="primary"
          icon={<WarningOutlined />}
          loading={testing}
          onClick={handleTest}
        >
          编译测试
        </Button>
      </div>
      {dateRange[0] && dateRange[1] && (
        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 8, paddingLeft: 4 }}>
          数据加载范围将根据回溯天数({lookbackDays}天)自动向前扩展
        </div>
      )}

      {/* 日志面板 */}
      {(testLogs.length > 0 || testStdout || testError) && (
        <div style={{ marginBottom: 8 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
            <span
              style={{ color: 'var(--text-secondary)', fontSize: 11, cursor: 'pointer' }}
              onClick={() => setShowLogs(!showLogs)}
            >
              {showLogs ? '▼' : '▶'} 执行日志 ({testLogs.length})
            </span>
          </div>
          {showLogs && (
            <div
              style={{
                background: 'var(--bg-primary)',
                border: '1px solid var(--border-color)',
                borderRadius: 4,
                padding: '6px 8px',
                maxHeight: 200,
                overflowY: 'auto',
                fontFamily: 'monospace',
                fontSize: 11,
                lineHeight: '18px',
              }}
            >
              {testLogs.map((log, i: number) => (
                <div key={i} style={{ color: logColorMap[log.level] || '#94a3b8' }}>
                  <span style={{ color: phaseColorMap[log.phase] || 'var(--text-muted)', marginRight: 6 }}>
                    [{log.phase}]
                  </span>
                  <span style={{ marginRight: 6 }}>[{log.level}]</span>
                  {log.message}
                </div>
              ))}
              {testStdout && (
                <div style={{ marginTop: 8, paddingTop: 8, borderTop: '1px solid var(--border-color)', color: '#94a3b8' }}>
                  <div style={{ color: '#22d3ee', marginBottom: 4 }}>[stdout]</div>
                  {testStdout}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* 错误信息 */}
      {testError && (
        <Alert type="error" message={testError} closable={false} style={{ marginBottom: 8, fontSize: 12 }} />
      )}

      {/* 测试结果 */}
      {testResult && !testError && (
        <div>
          <div style={{ display: 'flex', gap: 12, marginBottom: 8, fontSize: 12 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ color: 'var(--text-secondary)' }}>总行数:</span>
              <div style={{ color: 'var(--color-primary)', fontWeight: 600 }}>{testResult.stats?.total_rows?.toLocaleString()}</div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ color: 'var(--text-secondary)' }}>有效值:</span>
              <div style={{ color: 'var(--color-gain)', fontWeight: 600 }}>{testResult.stats?.count?.toLocaleString()}</div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ color: 'var(--text-secondary)' }}>空值数:</span>
              <div
                style={{
                  color: testResult.stats?.null_count && testResult.stats.null_count > 0 ? 'var(--color-loss)' : 'var(--color-gain)',
                  fontWeight: 600,
                }}
              >
                {testResult.stats?.null_count?.toLocaleString()}
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ color: 'var(--text-secondary)' }}>最小值:</span>
              <div style={{ color: 'var(--color-primary)', fontWeight: 600 }}>
                {testResult.stats?.min != null ? testResult.stats.min.toFixed(4) : '-'}
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ color: 'var(--text-secondary)' }}>最大值:</span>
              <div style={{ color: 'var(--color-primary)', fontWeight: 600 }}>
                {testResult.stats?.max != null ? testResult.stats.max.toFixed(4) : '-'}
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ color: 'var(--text-secondary)' }}>均值:</span>
              <div style={{ color: 'var(--color-primary)', fontWeight: 600 }}>
                {testResult.stats?.mean != null ? testResult.stats.mean.toFixed(4) : '-'}
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ color: 'var(--text-secondary)' }}>标准差:</span>
              <div style={{ color: 'var(--color-primary)', fontWeight: 600 }}>
                {testResult.stats?.std != null ? testResult.stats.std.toFixed(4) : '-'}
              </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ color: 'var(--text-secondary)' }}>中位数:</span>
              <div style={{ color: 'var(--color-primary)', fontWeight: 600 }}>
                {testResult.stats?.median != null ? testResult.stats.median.toFixed(4) : '-'}
              </div>
            </div>
          </div>

          {testResult.truncated && (
            <Alert
              type="warning"
              message="结果已截断，仅显示前 2000 行"
              closable={false}
              style={{ marginBottom: 8, fontSize: 12 }}
            />
          )}

          <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
            <Select
              size="small"
              allowClear
              placeholder="筛选股票"
              style={{ width: 160 }}
              value={filterStock}
              onChange={v => setFilterStock(v as string | undefined)}
              showSearch
              options={testResult.stocks?.map((s: string) => ({ label: s, value: s })) || []}
            />
            <Select
              size="small"
              allowClear
              placeholder="筛选日期"
              style={{ width: 140 }}
              value={filterDate}
              onChange={v => setFilterDate(v as string | undefined)}
              showSearch
              options={testResult.dates?.map((d: string) => ({ label: d, value: d })) || []}
            />
            <span style={{ color: 'var(--text-muted)', fontSize: 12, lineHeight: '24px' }}>
              显示 {filteredPreview.length} 条
            </span>
          </div>

          <Table
            dataSource={filteredPreview}
            columns={resultColumns}
            rowKey={(r: any) => `${r?.ts_code}-${r?.trade_date}`}
            size="small"
            pagination={{ pageSize: 10 }}
            scroll={{ y: 240 }}
          />
        </div>
      )}
    </div>
  );
};

export default TestPanel;
