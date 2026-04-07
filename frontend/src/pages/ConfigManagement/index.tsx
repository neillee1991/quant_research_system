import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Row,
  Col,
  Checkbox,
  Button,
  Radio,
  Upload,
  Space,
  Alert,
  Divider,
  Typography,
  message,
} from 'antd';
import {
  DownloadOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import type { UploadFile, UploadProps } from 'antd/es/upload/interface';
import {
  ConfigType,
  ImportMode,
  ConfigTypeOption,
  ExportRequest,
  ImportVerifyRequest,
  ImportApplyRequest,
  ConfigTypeDiff,
  ConfigItemDiff,
  ImportApplyResponse,
} from './types';
import { configApi } from '../../api';
import DiffViewer from './DiffViewer';
import ImportResult from './ImportResult';

const { Text } = Typography;
const { Group: CheckboxGroup } = Checkbox;
const { Group: RadioGroup } = Radio;

// 工具函数：下载文件
function downloadFile(content: string, filename: string) {
  const byteCharacters = atob(content);
  const byteNumbers = new Array(byteCharacters.length);
  for (let i = 0; i < byteCharacters.length; i++) {
    byteNumbers[i] = byteCharacters.charCodeAt(i);
  }
  const byteArray = new Uint8Array(byteNumbers);
  const blob = new Blob([byteArray], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

// 工具函数：读取文件为 Base64
function readFileAsBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    // 添加文件大小检查 (10MB)
    if (file.size > 10 * 1024 * 1024) {
      message.error('文件太大，请选择小于10MB的文件');
      return reject(new Error('File too large'));
    }

    const reader = new FileReader();
    reader.onload = () => {
      const result = reader.result as string;
      // 去掉 data:application/json;base64, 前缀
      const base64 = result.split(',')[1];
      resolve(base64);
    };
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });
}

const ConfigManagement: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [configTypes, setConfigTypes] = useState<ConfigTypeOption[]>([]);

  // 导出状态
  const [selectedExportTypes, setSelectedExportTypes] = useState<ConfigType[]>([]);

  // 导入状态
  const [importMode, setImportMode] = useState<ImportMode>(ImportMode.SAFE);
  const [importFile, setImportFile] = useState<UploadFile | null>(null);
  const [importContent, setImportContent] = useState<string>('');
  const [verifyResult, setVerifyResult] = useState<{
    valid: boolean;
    errors: string[];
    diffs: ConfigTypeDiff[] | null;
  } | null>(null);
  const [selectedItems, setSelectedItems] = useState<Record<ConfigType, string[]>>({} as Record<ConfigType, string[]>);
  const [importResult, setImportResult] = useState<ImportApplyResponse | null>(null);

  useEffect(() => {
    loadConfigTypes();
  }, []);

  const loadConfigTypes = async () => {
    try {
      const response = await configApi.getConfigTypes();
      setConfigTypes(response.data);
      // 默认全选
      setSelectedExportTypes(response.data.map(t => t.value));
    } catch (error) {
      message.error('加载配置类型失败');
    }
  };

  // 导出处理
  const handleExport = async () => {
    if (selectedExportTypes.length === 0) {
      message.warning('请至少选择一种配置类型');
      return;
    }

    setLoading(true);
    try {
      const request: ExportRequest = { config_types: selectedExportTypes };
      const response = await configApi.exportConfigs(request);
      downloadFile(response.data.content, response.data.filename);
      message.success('导出成功');
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : '未知错误';
      message.error(`导出失败: ${errorMsg}`);
    } finally {
      setLoading(false);
    }
  };

  // 导入文件选择
  const uploadProps: UploadProps = {
    beforeUpload: async (file) => {
      const isValidFileType = file.type === 'application/json' ||
                             file.name.toLowerCase().endsWith('.json');
      if (!isValidFileType) {
        message.error('请上传 JSON 文件');
        return false;
      }

      setImportFile(file);
      setVerifyResult(null);
      setImportResult(null);
      setSelectedItems({} as Record<ConfigType, string[]>);

      try {
        const content = await readFileAsBase64(file);
        setImportContent(content);
      } catch (error) {
        const errorMsg = error instanceof Error ? error.message : '读取文件失败';
        message.error(`读取文件失败: ${errorMsg}`);
      }

      return false;
    },
    fileList: importFile ? [importFile] : [],
    maxCount: 1,
  };

  // 验证导入
  const handleVerify = async () => {
    if (!importContent) {
      message.warning('请先选择文件');
      return;
    }

    setLoading(true);
    try {
      const request: ImportVerifyRequest = {
        content: importContent,
        mode: importMode,
      };
      const response = await configApi.verifyImport(request);
      setVerifyResult(response.data);

      // 默认选中所有新增和修改的项
      if (response.data.diffs) {
        const selections: Record<ConfigType, string[]> = {} as Record<ConfigType, string[]>;
        for (const diff of response.data.diffs) {
          selections[diff.config_type] = diff.items
            .filter(item => item.status === 'new' || item.status === 'modified')
            .map(item => item.item_id);
        }
        setSelectedItems(selections);
      }

      if (response.data.valid) {
        message.success('验证通过');
      } else {
        message.error('验证失败');
      }
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : '验证过程发生错误';
      message.error(`验证失败: ${errorMsg}`);
    } finally {
      setLoading(false);
    }
  };

  // 执行导入
  const handleApply = async () => {
    if (!verifyResult?.valid) {
      message.warning('请先通过验证');
      return;
    }

    setLoading(true);
    try {
      const request: ImportApplyRequest = {
        content: importContent,
        mode: importMode,
        selections: importMode === ImportMode.SAFE ? selectedItems : undefined,
      };
      const response = await configApi.applyImport(request);
      setImportResult(response.data);

      if (response.data.success) {
        message.success('导入成功');
      } else {
        message.error('导入失败');
      }
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : '导入过程发生错误';
      message.error(`导入失败: ${errorMsg}`);
    } finally {
      setLoading(false);
    }
  };

  // 切换选中项
  const toggleItemSelection = (configType: ConfigType, itemId: string, checked: boolean) => {
    setSelectedItems(prev => {
      const current = prev[configType] || [];
      return {
        ...prev,
        [configType]: checked
          ? [...current, itemId]
          : current.filter(id => id !== itemId)
      };
    });
  };

  // 全选/取消全选
  const toggleSelectAll = (configType: ConfigType, items: ConfigItemDiff[], checked: boolean) => {
    if (checked) {
      setSelectedItems(prev => ({
        ...prev,
        [configType]: items
          .filter(item => item.status !== 'unchanged')
          .map(item => item.item_id)
      }));
    } else {
      setSelectedItems(prev => ({
        ...prev,
        [configType]: []
      }));
    }
  };

  const getConfigTypeLabel = (value: ConfigType) => {
    return configTypes.find(t => t.value === value)?.label || value;
  };

  return (
    <div>
      <Row gutter={24}>
        {/* 导出配置 */}
        <Col span={12}>
          <Card title="导出配置" extra={<DownloadOutlined />}>
            <Space direction="vertical" style={{ width: '100%' }}>
              <div>
                <Text strong>选择要导出的配置类型：</Text>
              </div>
              <CheckboxGroup
                options={configTypes}
                value={selectedExportTypes}
                onChange={(values) => setSelectedExportTypes(values as ConfigType[])}
              />
              <Button
                type="primary"
                icon={<DownloadOutlined />}
                onClick={handleExport}
                loading={loading}
                disabled={selectedExportTypes.length === 0}
              >
                导出配置
              </Button>
            </Space>
          </Card>
        </Col>

        {/* 导入配置 */}
        <Col span={12}>
          <Card title="导入配置" extra={<UploadOutlined />}>
            <Space direction="vertical" style={{ width: '100%' }}>
              <div>
                <Text strong>导入模式：</Text>
              </div>
              <RadioGroup
                value={importMode}
                onChange={(e) => {
                  setImportMode(e.target.value);
                  setVerifyResult(null);
                  setImportResult(null);
                }}
              >
                <Radio value={ImportMode.FAST}>
                  快速模式 - 直接覆盖
                </Radio>
                <Radio value={ImportMode.SAFE}>
                  安全模式 - 预览差异
                </Radio>
              </RadioGroup>

              <div>
                <Text strong>选择备份文件：</Text>
              </div>
              <Upload {...uploadProps}>
                <Button icon={<UploadOutlined />}>
                  {importFile ? '重新选择文件' : '选择 JSON 文件'}
                </Button>
              </Upload>

              {importFile && (
                <Space>
                  <Button
                    type="primary"
                    onClick={handleVerify}
                    loading={loading}
                  >
                    验证文件
                  </Button>
                </Space>
              )}
            </Space>
          </Card>
        </Col>
      </Row>

      {/* 验证结果 */}
      {verifyResult && (
        <>
          <Divider />
          <Card title="验证结果">
            {verifyResult.errors.length > 0 && (
              <Alert
                message="验证错误"
                description={
                  <ul>
                    {verifyResult.errors.map((err, idx) => (
                      <li key={idx}>{err}</li>
                    ))}
                  </ul>
                }
                type="error"
                showIcon
                style={{ marginBottom: 16 }}
              />
            )}

            {verifyResult.valid && verifyResult.diffs && (
              <>
                <DiffViewer
                  diffs={verifyResult.diffs}
                  selectedItems={selectedItems}
                  onToggleItem={toggleItemSelection}
                  onToggleSelectAll={toggleSelectAll}
                  getConfigTypeLabel={getConfigTypeLabel}
                />
                <div style={{ marginTop: 16 }}>
                  <Button
                    type="primary"
                    size="large"
                    onClick={handleApply}
                    loading={loading}
                  >
                    确认导入
                  </Button>
                </div>
              </>
            )}

            {verifyResult.valid && !verifyResult.diffs && (
              <>
                <Alert
                  message="快速模式"
                  description="快速模式将直接覆盖现有配置，请确认后继续。"
                  type="warning"
                  showIcon
                  style={{ marginBottom: 16 }}
                />
                <Button
                  type="primary"
                  size="large"
                  onClick={handleApply}
                  loading={loading}
                >
                  确认导入（快速模式）
                </Button>
              </>
            )}
          </Card>
        </>
      )}

      {/* 导入结果 */}
      {importResult && (
        <>
          <Divider />
          <ImportResult result={importResult} getConfigTypeLabel={getConfigTypeLabel} />
        </>
      )}
    </div>
  );
};

export default ConfigManagement;
