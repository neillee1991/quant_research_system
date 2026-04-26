/**
 * 快速订阅指数 Modal
 * 搜索订阅 + 显示已订阅列表
 */
import { notify, extractApiError } from '../../utils/notify';
import React, { useState, useCallback, useEffect } from 'react';
import { Modal, Select, Space, Tag, Table, Button, Popconfirm, Spin } from 'antd';
import { CheckOutlined, DeleteOutlined } from '@ant-design/icons';
import { indexApi } from '../../api';

interface QuickSubscribeModalProps {
  visible: boolean;
  onClose: () => void;
  onSuccess: () => void;
}

const QuickSubscribeModal: React.FC<QuickSubscribeModalProps> = ({
  visible,
  onClose,
  onSuccess,
}) => {
  const [indexCode, setIndexCode] = useState<string>('');
  const [options, setOptions] = useState<Array<{ label: string; value: string; name: string; is_subscribed: boolean }>>([]);
  const [searching, setSearching] = useState(false);
  const [subscribing, setSubscribing] = useState(false);
  const [subscribed, setSubscribed] = useState<Array<{ ts_code: string; name: string; market: string }>>([]);
  const [loadingSubscribed, setLoadingSubscribed] = useState(false);
  const [unsubscribingCode, setUnsubscribingCode] = useState<string>('');

  // 加载已订阅列表
  const loadSubscribed = useCallback(async () => {
    setLoadingSubscribed(true);
    try {
      const res = await indexApi.listAvailableIndices({ show_subscribed_only: true, limit: 100 });
      setSubscribed(res.data.indices || []);
    } catch {
      setSubscribed([]);
    } finally {
      setLoadingSubscribed(false);
    }
  }, []);

  useEffect(() => {
    if (visible) loadSubscribed();
  }, [visible, loadSubscribed]);

  // 搜索指数
  const handleSearch = useCallback(async (value: string) => {
    if (!value || value.length < 2) {
      setOptions([]);
      return;
    }
    setSearching(true);
    try {
      const res = await indexApi.listAvailableIndices({ search: value, limit: 10 });
      const indices = res.data.indices || [];
      setOptions(indices.map((idx: any) => ({
        label: `${idx.ts_code} - ${idx.name}`,
        value: idx.ts_code,
        name: idx.name,
        is_subscribed: idx.is_subscribed,
      })));
    } catch {
      setOptions([]);
    } finally {
      setSearching(false);
    }
  }, []);

  // 订阅
  const handleSubscribe = async () => {
    if (!indexCode) {
      notify.warning('请选择要订阅的指数');
      return;
    }
    const selected = options.find(o => o.value === indexCode);
    if (selected?.is_subscribed) {
      notify.warning('该指数已订阅');
      return;
    }
    setSubscribing(true);
    try {
      await indexApi.subscribeIndex({ index_code: indexCode });
      notify.success(`成功订阅指数 ${selected?.name || indexCode}`);
      setIndexCode('');
      setOptions([]);
      await loadSubscribed();
      onSuccess();
    } catch (error: any) {
      notify.error(`订阅失败: ${extractApiError(error.response?.data?.detail, error.message)}`);
    } finally {
      setSubscribing(false);
    }
  };

  // 取消订阅
  const handleUnsubscribe = async (tsCode: string, name: string) => {
    setUnsubscribingCode(tsCode);
    try {
      await indexApi.unsubscribeIndex(tsCode);
      notify.success(`已取消订阅 ${name}`);
      await loadSubscribed();
      onSuccess();
    } catch (error: any) {
      notify.error(`取消订阅失败: ${extractApiError(error.response?.data?.detail, error.message)}`);
    } finally {
      setUnsubscribingCode('');
    }
  };

  const columns = [
    {
      title: '指数代码',
      dataIndex: 'ts_code',
      key: 'ts_code',
      width: 120,
      render: (code: string) => <code style={{ fontSize: 12 }}>{code}</code>,
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: '市场',
      dataIndex: 'market',
      key: 'market',
      width: 80,
      render: (market: string) => <Tag>{market}</Tag>,
    },
    {
      title: '',
      key: 'action',
      width: 80,
      render: (_: any, record: any) => (
        <Popconfirm
          title={`取消订阅 ${record.name}？`}
          description="相关同步任务及数据表将被删除，不可撤销。"
          okText="确认"
          okButtonProps={{ danger: true }}
          cancelText="取消"
          onConfirm={() => handleUnsubscribe(record.ts_code, record.name)}
        >
          <Button
            type="text"
            size="small"
            danger
            icon={<DeleteOutlined />}
            loading={unsubscribingCode === record.ts_code}
          />
        </Popconfirm>
      ),
    },
  ];

  return (
    <Modal
      title="指数订阅"
      open={visible}
      onCancel={onClose}
      footer={null}
      width={560}
      destroyOnClose
    >
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        {/* 搜索订阅 */}
        <div style={{ display: 'flex', gap: 8 }}>
          <Select
            showSearch
            style={{ flex: 1 }}
            placeholder="输入指数代码或名称搜索..."
            value={indexCode || undefined}
            onChange={setIndexCode}
            onSearch={handleSearch}
            loading={searching}
            filterOption={false}
            notFoundContent={searching ? '搜索中...' : '输入关键词搜索'}
            options={options.map(o => ({
              label: (
                <span>
                  {o.label}
                  {o.is_subscribed && <Tag color="green" style={{ marginLeft: 8, fontSize: 11 }}>已订阅</Tag>}
                </span>
              ),
              value: o.value,
            }))}
            allowClear
          />
          <Button
            type="primary"
            icon={<CheckOutlined />}
            onClick={handleSubscribe}
            loading={subscribing}
            disabled={!indexCode}
          >
            订阅
          </Button>
        </div>

        {/* 已订阅列表 */}
        <div>
          <div style={{ marginBottom: 8, fontSize: 13, fontWeight: 500 }}>
            已订阅指数
            <Tag style={{ marginLeft: 8 }}>{subscribed.length}</Tag>
          </div>
          <Spin spinning={loadingSubscribed}>
            <Table
              dataSource={subscribed}
              columns={columns}
              rowKey="ts_code"
              size="small"
              pagination={false}
              locale={{ emptyText: '暂无已订阅指数' }}
              scroll={{ y: 240 }}
            />
          </Spin>
        </div>
      </Space>
    </Modal>
  );
};

export default QuickSubscribeModal;
