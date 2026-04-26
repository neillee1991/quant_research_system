import React, { useState, useEffect } from 'react';
import { Modal, Form, Select, Button, Input, Row, Col, Table } from 'antd';
import { SaveOutlined } from '@ant-design/icons';

const { Option } = Select;

interface FieldMapping {
  engineField: string;
  dbField: string;
  description: string;
}

interface DataSourceMappingModalProps {
  visible: boolean;
  onCancel: () => void;
  onSave: (mapping: any) => void;
  existingMapping?: any;
}

const ENGINE_FIELDS: FieldMapping[] = [
  { engineField: 'open', dbField: '', description: '开盘价' },
  { engineField: 'high', dbField: '', description: '最高价' },
  { engineField: 'low', dbField: '', description: '最低价' },
  { engineField: 'close', dbField: '', description: '收盘价' },
  { engineField: 'volume', dbField: '', description: '成交量' },
  { engineField: 'amount', dbField: '', description: '成交额' },
  { engineField: 'limit_up', dbField: '', description: '涨停价' },
  { engineField: 'limit_down', dbField: '', description: '跌停价' },
];

export const DataSourceMappingModal: React.FC<DataSourceMappingModalProps> = ({
  visible,
  onCancel,
  onSave,
  existingMapping,
}) => {
  const [form] = Form.useForm();
  const [mappings, setMappings] = useState<FieldMapping[]>(ENGINE_FIELDS);
  const [tableSchemas, setTableSchemas] = useState<any[]>([]);
  const [selectedPriceTable, setSelectedPriceTable] = useState<string>('');
  const [selectedFactorTable, setSelectedFactorTable] = useState<string>('');

  useEffect(() => {
    if (visible) {
      loadTableSchemas();
      if (existingMapping) {
        form.setFieldsValue({
          name: existingMapping.name,
          price_table: existingMapping.price_table,
          factor_table: existingMapping.factor_table,
        });
        setSelectedPriceTable(existingMapping.price_table);
        setSelectedFactorTable(existingMapping.factor_table);
        if (existingMapping.field_mappings) {
          setMappings(ENGINE_FIELDS.map(f => ({
            ...f,
            dbField: existingMapping.field_mappings[f.engineField] || '',
          })));
        }
      } else {
        form.resetFields();
        setMappings(ENGINE_FIELDS);
      }
    }
  }, [visible, existingMapping]);

  const loadTableSchemas = async () => {
    try {
      const response = await fetch('/api/v1/data_mappings/default/schema');
      if (response.ok) {
        const data = await response.json();
        setTableSchemas(data.tables || []);
      }
    } catch (error) {
      console.error('Failed to load schemas:', error);
    }
  };

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      const fieldMappings: any = {};
      mappings.forEach(m => {
        if (m.dbField) {
          fieldMappings[m.engineField] = m.dbField;
        }
      });

      const mappingData = {
        ...values,
        field_mappings: fieldMappings,
      };

      onSave(mappingData);
      onCancel();
    } catch (error) {
      console.error('Validation failed:', error);
    }
  };

  const handleMappingChange = (index: number, dbField: string) => {
    const newMappings = [...mappings];
    newMappings[index].dbField = dbField;
    setMappings(newMappings);
  };

  const getAvailableFields = () => {
    const priceTable = tableSchemas.find(t => t.name === selectedPriceTable);
    const factorTable = tableSchemas.find(t => t.name === selectedFactorTable);
    const allFields = [
      ...(priceTable?.fields || []),
      ...(factorTable?.fields || []),
    ];
    const uniqueFields = Array.from(new Set(allFields.map(f => f.name))).map(name =>
      allFields.find(f => f.name === name)
    );
    return uniqueFields.filter(Boolean);
  };

  const columns = [
    {
      title: '引擎字段',
      dataIndex: 'engineField',
      key: 'engineField',
      width: 150,
    },
    {
      title: '字段描述',
      dataIndex: 'description',
      key: 'description',
      width: 150,
    },
    {
      title: '数据库字段映射',
      key: 'dbField',
      width: 300,
      render: (_: any, record: FieldMapping, index: number) => (
        <Select
          value={record.dbField}
          onChange={(value) => handleMappingChange(index, value)}
          style={{ width: '100%' }}
          placeholder="请选择字段"
          showSearch
          filterOption={(input, option) =>
            (option?.value as string).toLowerCase().includes(input.toLowerCase())
          }
        >
          {getAvailableFields().map(field => (
            <Option key={field?.name} value={field?.name}>
              {field?.name} - {field?.description}
            </Option>
          ))}
        </Select>
      ),
    },
  ];

  return (
    <Modal
      title="数据源映射配置"
      open={visible}
      onCancel={onCancel}
      onOk={handleSave}
      width={900}
      footer={[
        <Button key="cancel" onClick={onCancel}>
          取消
        </Button>,
        <Button key="save" type="primary" onClick={handleSave} icon={<SaveOutlined />}>
          保存映射
        </Button>,
      ]}
    >
      <Form form={form} layout="vertical">
        <Form.Item
          name="name"
          label="映射名称"
          rules={[{ required: true, message: '请输入映射名称' }]}
        >
          <Input placeholder="例如：A股日线数据映射" />
        </Form.Item>

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              name="price_table"
              label="价格数据表"
              rules={[{ required: true, message: '请选择价格数据表' }]}
            >
              <Select
                placeholder="请选择价格数据表"
                onChange={setSelectedPriceTable}
              >
                {tableSchemas.map(table => (
                  <Option key={table.name} value={table.name}>
                    {table.name} - {table.description}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              name="factor_table"
              label="因子数据表"
              rules={[{ required: true, message: '请选择因子数据表' }]}
            >
              <Select
                placeholder="请选择因子数据表"
                onChange={setSelectedFactorTable}
              >
                {tableSchemas.map(table => (
                  <Option key={table.name} value={table.name}>
                    {table.name} - {table.description}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
        </Row>

        <div style={{ marginBottom: 16 }}>
          <h4 style={{ marginBottom: 8 }}>字段映射配置</h4>
          <p style={{ color: '#888', fontSize: 12, marginBottom: 16 }}>
            将回测引擎需要的标准字段映射到您的数据表字段
          </p>
        </div>

        <Table
          columns={columns}
          dataSource={mappings}
          rowKey="engineField"
          pagination={false}
          size="small"
          bordered
          style={{ marginBottom: 16 }}
        />
      </Form>
    </Modal>
  );
};
