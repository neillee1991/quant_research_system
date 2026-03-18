import React, { useState, useEffect } from 'react';
import {
  Table,
  Button,
  Modal,
  Form,
  Input,
  Space,
  Card,
  Typography,
  Popconfirm,
  Tag,
  Spin,
} from 'antd';
import { UploadOutlined, DeleteOutlined, DownloadOutlined, PlusOutlined } from '@ant-design/icons';
import { productionApi } from '../api';
import { useMessage } from '../hooks/useMessage';
import './IndexPoolCenter.css';

const { Title, Text } = Typography;

interface IndexPool {
  index_code: string;
  index_name: string;
  description: string;
  stock_count: number;
  latest_date: string;
  updated_at: string;
}

interface Constituent {
  ts_code: string;
  trade_date: string;
  weight: number;
}

const IndexPoolCenter: React.FC = () => {
  const message = useMessage();
  const [indexPools, setIndexPools] = useState<IndexPool[]>([]);
  const [loading, setLoading] = useState(false);
  const [uploadModalVisible, setUploadModalVisible] = useState(false);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState<string | null>(null);
  const [constituents, setConstituents] = useState<Constituent[]>([]);
  const [uploadType, setUploadType] = useState<'json' | 'csv'>('json');
  const [form] = Form.useForm();

  useEffect(() => {
    loadIndexPools();
  }, []);

  const loadIndexPools = async () => {
    setLoading(true);
    try {
      const response = await productionApi.listIndexPools();
      if (response.data.status === 'success') {
        setIndexPools(response.data.data);
      }
    } catch (error: any) {
      message.error(`加载失败: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleUploadJSON = async (values: any) => {
    try {
      const data = JSON.parse(values.jsonData);
      const payload = {
        index_code: values.index_code,
        index_name: values.index_name || '',
        description: values.description || '',
        data: data,
      };

      await productionApi.batchUploadIndexPool(payload);
      message.success('上传成功');
      setUploadModalVisible(false);
      form.resetFields();
      loadIndexPools();
    } catch (error: any) {
      message.error(`上传失败: ${error.message}`);
    }
  };

  const handleUploadCSV = async (values: any) => {
    try {
      const payload = {
        index_code: values.index_code,
        index_name: values.index_name || '',
        description: values.description || '',
        csv_content: values.csvContent,
      };

      await productionApi.csvUploadIndexPool(payload);
      message.success('上传成功');
      setUploadModalVisible(false);
      form.resetFields();
      loadIndexPools();
    } catch (error: any) {
      message.error(`上传失败: ${error.message}`);
    }
  };

  const handleViewDetails = async (indexCode: string) => {
    setSelectedIndex(indexCode);
    setDetailModalVisible(true);
    setLoading(true);

    try {
      const response = await productionApi.getIndexPool(indexCode);
      if (response.data.status === 'success') {
        setConstituents(response.data.data.constituents);
      }
    } catch (error: any) {
      message.error(`加载失败: ${error.message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (indexCode: string) => {
    try {
      await productionApi.deleteIndexPool(indexCode);
      message.success('删除成功');
      loadIndexPools();
    } catch (error: any) {
      message.error(`删除失败: ${error.message}`);
    }
  };

  const handleDownloadTemplate = async () => {
    try {
      const response = await productionApi.downloadIndexPoolTemplate();
      const blob = new Blob([response.data], { type: 'text/csv' });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'index_pool_template.csv';
      a.click();
      window.URL.revokeObjectURL(url);
      message.success('模板下载成功');
    } catch (error: any) {
      message.error(`下载失败: ${error.message}`);
    }
  };

  const columns = [
    {
      title: '指数代码',
      dataIndex: 'index_code',
      key: 'index_code',
      render: (text: string) => <Text strong>{text}</Text>,
    },
    {
      title: '指数名称',
      dataIndex: 'index_name',
      key: 'index_name',
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: '股票数量',
      dataIndex: 'stock_count',
      key: 'stock_count',
      render: (count: number) => <Tag color="blue">{count} 只</Tag>,
    },
    {
      title: '最新日期',
      dataIndex: 'latest_date',
      key: 'latest_date',
    },
    {
      title: '更新时间',
      dataIndex: 'updated_at',
      key: 'updated_at',
      render: (text: string) => text ? new Date(text).toLocaleString('zh-CN') : '-',
    },
    {
      title: '操作',
      key: 'action',
      render: (_: any, record: IndexPool) => (
        <Space>
          <Button
            size="small"
            onClick={() => handleViewDetails(record.index_code)}
          >
            查看成分股
          </Button>
          <Popconfirm
            title="确定删除该指数吗？"
            description="删除后将无法恢复"
            onConfirm={() => handleDelete(record.index_code)}
          >
            <Button size="small" danger icon={<DeleteOutlined />}>
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  const constituentColumns = [
    {
      title: '股票代码',
      dataIndex: 'ts_code',
      key: 'ts_code',
    },
    {
      title: '交易日期',
      dataIndex: 'trade_date',
      key: 'trade_date',
    },
    {
      title: '权重',
      dataIndex: 'weight',
      key: 'weight',
      render: (weight: number) => `${(weight * 100).toFixed(2)}%`,
    },
  ];

  return (
    <div className="index-pool-center">
      <Card
        title={<Title level={3}>指数股票池管理</Title>}
        extra={
          <Space>
            <Button
              icon={<DownloadOutlined />}
              onClick={handleDownloadTemplate}
            >
              下载模板
            </Button>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setUploadModalVisible(true)}
            >
              上传指数
            </Button>
          </Space>
        }
      >
        <Spin spinning={loading}>
          <Table
            columns={columns}
            dataSource={indexPools}
            rowKey="index_code"
            pagination={{ pageSize: 10 }}
          />
        </Spin>
      </Card>

      {/* 上传模态框 */}
      <Modal
        title="上传指数成分股"
        open={uploadModalVisible}
        onCancel={() => {
          setUploadModalVisible(false);
          form.resetFields();
        }}
        footer={null}
        width={600}
      >
        <div style={{ marginBottom: 16 }}>
          <Space>
            <Button
              type={uploadType === 'json' ? 'primary' : 'default'}
              onClick={() => setUploadType('json')}
            >
              JSON 格式
            </Button>
            <Button
              type={uploadType === 'csv' ? 'primary' : 'default'}
              onClick={() => setUploadType('csv')}
            >
              CSV 格式
            </Button>
          </Space>
        </div>

        <Form
          form={form}
          onFinish={uploadType === 'json' ? handleUploadJSON : handleUploadCSV}
          labelCol={{ span: 5 }}
          wrapperCol={{ span: 19 }}
        >
          <Form.Item
            name="index_code"
            label="指数代码"
            rules={[{ required: true, message: '请输入指数代码' }]}
          >
            <Input placeholder="例如: 000300.SH" />
          </Form.Item>
          <Form.Item name="index_name" label="指数名称">
            <Input placeholder="例如: 沪深300" />
          </Form.Item>
          <Form.Item name="description" label="描述">
            <Input placeholder="指数描述信息" />
          </Form.Item>

          {uploadType === 'json' ? (
            <Form.Item
              name="jsonData"
              label="JSON 数据"
              rules={[{ required: true, message: '请输入 JSON 数据' }]}
            >
              <Input.TextArea
                placeholder='[{"trade_date": "20240101", "ts_code": "000001.SZ", "weight": 0.05}]'
                rows={10}
              />
            </Form.Item>
          ) : (
            <Form.Item
              name="csvContent"
              label="CSV 内容"
              rules={[{ required: true, message: '请输入 CSV 内容' }]}
            >
              <Input.TextArea
                placeholder="trade_date,ts_code,weight&#10;20240101,000001.SZ,0.05"
                rows={10}
              />
            </Form.Item>
          )}

          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 16 }}>
            <Button type="primary" htmlType="submit" icon={<UploadOutlined />}>
              上传
            </Button>
            <Button onClick={() => {
              setUploadModalVisible(false);
              form.resetFields();
            }}>
              取消
            </Button>
          </div>
        </Form>
      </Modal>

      {/* 成分股详情模态框 */}
      <Modal
        title={`成分股详情 - ${selectedIndex}`}
        open={detailModalVisible}
        onCancel={() => setDetailModalVisible(false)}
        footer={null}
        width={800}
      >
        <Spin spinning={loading}>
          <Table
            columns={constituentColumns}
            dataSource={constituents}
            rowKey={(record) => record ? `${record.ts_code}_${record.trade_date}` : ''}
            pagination={{ pageSize: 20 }}
          />
        </Spin>
      </Modal>
    </div>
  );
};

export default IndexPoolCenter;
