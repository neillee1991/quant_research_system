/**
 * 同步任务配置模态框组件
 */
import React from 'react';
import { Modal, DatePicker, Tag } from '@douyinfe/semi-ui';
import dayjs from 'dayjs';
import type { SyncTask } from '../../types';

interface SyncModalProps {
  visible: boolean;
  task: SyncTask | null;
  targetDate: string;
  startDate: string;
  endDate: string;
  onTargetDateChange: (date: string) => void;
  onStartDateChange: (date: string) => void;
  onEndDateChange: (date: string) => void;
  onOk: () => void;
  onCancel: () => void;
}

export const SyncModal: React.FC<SyncModalProps> = ({
  visible,
  task,
  targetDate,
  startDate,
  endDate,
  onTargetDateChange,
  onStartDateChange,
  onEndDateChange,
  onOk,
  onCancel,
}) => {
  return (
    <Modal
      title={`同步任务: ${task?.task_id || ''}`}
      visible={visible}
      onOk={onOk}
      onCancel={onCancel}
      okText="开始同步"
      cancelText="取消"
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: '8px 0' }}>
          <div style={{ display: 'flex' }}>
            <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>任务ID</span>
            <span style={{ fontSize: 13 }}>{task?.task_id}</span>
          </div>
          <div style={{ display: 'flex' }}>
            <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>描述</span>
            <span style={{ fontSize: 13 }}>{task?.description}</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>类型</span>
            <Tag color={task?.sync_type === 'incremental' ? 'blue' : 'green'}>
              {task?.sync_type}
            </Tag>
          </div>
          <div style={{ display: 'flex' }}>
            <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>数据表</span>
            <span style={{ fontSize: 13 }}>{task?.table_name}</span>
          </div>
        </div>

        {task?.sync_type === 'incremental' && (
          <>
            <div>
              <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>
                日期范围（可选）
              </div>
              <DatePicker
                type="dateRange"
                placeholder={['开始日期', '结束日期']}
                defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
                value={
                  startDate && endDate
                    ? [
                        dayjs(startDate, 'YYYYMMDD').toDate(),
                        dayjs(endDate, 'YYYYMMDD').toDate(),
                      ]
                    : undefined
                }
                onChange={(date, dateStr) => {
                  const strs = dateStr as unknown as string[];
                  if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                    onStartDateChange(strs[0].replace(/-/g, ''));
                    onEndDateChange(strs[1].replace(/-/g, ''));
                  } else {
                    onStartDateChange('');
                    onEndDateChange('');
                  }
                }}
                style={{ width: '100%' }}
                size="small"
              />
              {startDate && endDate && (
                <div
                  style={{
                    marginTop: 8,
                    padding: '8px 12px',
                    background: 'var(--color-primary-light-default)',
                    borderRadius: '6px',
                  }}
                >
                  <span
                    style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}
                  >
                    共 {dayjs(endDate, 'YYYYMMDD').diff(dayjs(startDate, 'YYYYMMDD'), 'day') + 1}{' '}
                    天
                  </span>
                </div>
              )}
              <div style={{ marginTop: 6, color: 'var(--text-muted)', fontSize: 11 }}>
                指定日期范围进行同步，留空则只同步最新一天的数据。
              </div>
            </div>
          </>
        )}

        {task?.sync_type === 'full' && (
          <div
            style={{
              padding: '12px',
              background: 'var(--bg-surface)',
              borderRadius: '6px',
              border: '1px solid var(--border-color)',
            }}
          >
            <div style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
              全量同步模式
            </div>
            <div style={{ marginTop: 6, color: 'var(--text-secondary)', fontSize: 12 }}>
              此任务将执行全量数据同步，无需指定日期参数。
            </div>
          </div>
        )}
      </div>
    </Modal>
  );
};

interface BatchSyncModalProps {
  visible: boolean;
  selectedTaskIds: string[];
  syncTasks: SyncTask[];
  startDate: string;
  endDate: string;
  onStartDateChange: (date: string) => void;
  onEndDateChange: (date: string) => void;
  onOk: () => void;
  onCancel: () => void;
}

export const BatchSyncModal: React.FC<BatchSyncModalProps> = ({
  visible,
  selectedTaskIds,
  syncTasks,
  startDate,
  endDate,
  onStartDateChange,
  onEndDateChange,
  onOk,
  onCancel,
}) => {
  const fullIds = selectedTaskIds.filter(
    (id) => syncTasks.find((t) => t.task_id === id)?.sync_type === 'full'
  );
  const incIds = selectedTaskIds.filter((id) => !fullIds.includes(id));

  return (
    <Modal
      title={`批量同步 (${selectedTaskIds.length} 个任务)`}
      visible={visible}
      onOk={onOk}
      onCancel={onCancel}
      okText="开始同步"
      cancelText="取消"
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        <div
          style={{
            padding: '12px',
            background: 'var(--bg-surface)',
            borderRadius: '6px',
            border: '1px solid var(--border-color)',
          }}
        >
          {incIds.length > 0 && (
            <div style={{ marginBottom: fullIds.length > 0 ? 8 : 0 }}>
              <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 6 }}>
                增量任务（按日期范围同步）：
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {incIds.map((id) => (
                  <Tag key={id} color="blue" style={{ fontSize: '12px' }}>
                    {id}
                  </Tag>
                ))}
              </div>
            </div>
          )}
          {fullIds.length > 0 && (
            <div>
              <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 6 }}>
                全量任务（仅执行一次，忽略日期）：
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {fullIds.map((id) => (
                  <Tag key={id} color="green" style={{ fontSize: '12px' }}>
                    {id}
                  </Tag>
                ))}
              </div>
            </div>
          )}
        </div>

        {incIds.length > 0 && (
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>
              增量任务日期范围
            </div>
            <DatePicker
              type="dateRange"
              placeholder={['开始日期', '结束日期']}
              defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
              value={
                startDate && endDate
                  ? [dayjs(startDate, 'YYYYMMDD').toDate(), dayjs(endDate, 'YYYYMMDD').toDate()]
                  : undefined
              }
              onChange={(date, dateStr) => {
                const strs = dateStr as unknown as string[];
                if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                  onStartDateChange(strs[0].replace(/-/g, ''));
                  onEndDateChange(strs[1].replace(/-/g, ''));
                } else {
                  onStartDateChange('');
                  onEndDateChange('');
                }
              }}
              style={{ width: '100%' }}
              size="small"
            />
            {startDate && endDate && (
              <div
                style={{
                  marginTop: 8,
                  padding: '8px 12px',
                  background: 'var(--color-primary-light-default)',
                  borderRadius: '6px',
                }}
              >
                <span style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
                  共 {dayjs(endDate, 'YYYYMMDD').diff(dayjs(startDate, 'YYYYMMDD'), 'day') + 1} 天
                </span>
              </div>
            )}
          </div>
        )}
      </div>
    </Modal>
  );
};

interface ETLBackfillModalProps {
  visible: boolean;
  task?: any; // 单个任务对象
  taskId: string;
  startDate: string;
  endDate: string;
  onStartDateChange: (date: string) => void;
  onEndDateChange: (date: string) => void;
  onOk: () => void;
  onCancel: () => void;
  selectedTaskIds?: string[];
  etlTasks?: any[];
}

export const ETLBackfillModal: React.FC<ETLBackfillModalProps> = ({
  visible,
  task,
  taskId,
  startDate,
  endDate,
  onStartDateChange,
  onEndDateChange,
  onOk,
  onCancel,
  selectedTaskIds,
  etlTasks,
}) => {
  // 批量回溯模式
  const isBatchMode = selectedTaskIds && selectedTaskIds.length > 0 && etlTasks;

  // 单个任务模式
  const isSingleTask = !isBatchMode && task;
  const isSingleFullTask = isSingleTask && task.sync_type === 'full';

  let fullIds: string[] = [];
  let incIds: string[] = [];

  if (isBatchMode) {
    fullIds = selectedTaskIds.filter(
      (id) => etlTasks.find((t: any) => t.task_id === id)?.sync_type === 'full'
    );
    incIds = selectedTaskIds.filter((id) => !fullIds.includes(id));
  }

  return (
    <Modal
      title={isBatchMode ? `批量回溯 (${selectedTaskIds.length} 个任务)` : `ETL 任务回溯: ${taskId}`}
      visible={visible}
      onOk={onOk}
      onCancel={onCancel}
      okText="开始回溯"
      cancelText="取消"
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        {/* 单个任务信息 */}
        {isSingleTask && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: '8px 0' }}>
            <div style={{ display: 'flex' }}>
              <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>任务ID</span>
              <span style={{ fontSize: 13 }}>{task.task_id}</span>
            </div>
            <div style={{ display: 'flex' }}>
              <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>描述</span>
              <span style={{ fontSize: 13 }}>{task.description}</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center' }}>
              <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>类型</span>
              <Tag color={task.sync_type === 'incremental' ? 'blue' : 'green'}>
                {task.sync_type}
              </Tag>
            </div>
            <div style={{ display: 'flex' }}>
              <span style={{ width: 80, color: 'var(--text-secondary)', fontSize: 13 }}>数据表</span>
              <span style={{ fontSize: 13 }}>{task.table_name}</span>
            </div>
          </div>
        )}

        {/* 批量任务信息 */}
        {isBatchMode && (
          <div
            style={{
              padding: '12px',
              background: 'var(--bg-surface)',
              borderRadius: '6px',
              border: '1px solid var(--border-color)',
            }}
          >
            {incIds.length > 0 && (
              <div style={{ marginBottom: fullIds.length > 0 ? 8 : 0 }}>
                <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 6 }}>
                  增量任务（按日期范围回溯）：
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                  {incIds.map((id) => (
                    <Tag key={id} color="blue" style={{ fontSize: '12px' }}>
                      {id}
                    </Tag>
                  ))}
                </div>
              </div>
            )}
            {fullIds.length > 0 && (
              <div>
                <div style={{ color: 'var(--text-secondary)', fontSize: '13px', marginBottom: 6 }}>
                  全量任务（回溯最新数据，忽略日期）：
                </div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                  {fullIds.map((id) => (
                    <Tag key={id} color="green" style={{ fontSize: '12px' }}>
                      {id}
                    </Tag>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {(!isBatchMode || incIds.length > 0) && !isSingleFullTask && (
          <div>
            <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>
              {isBatchMode ? '增量任务回溯日期范围' : '回溯日期范围'}
            </div>
            <DatePicker
              type="dateRange"
              placeholder={['开始日期', '结束日期']}
              defaultPickerValue={dayjs().subtract(1, 'month').toDate()}
              value={
                startDate && endDate
                  ? [dayjs(startDate, 'YYYYMMDD').toDate(), dayjs(endDate, 'YYYYMMDD').toDate()]
                  : undefined
              }
              onChange={(date, dateStr) => {
                const strs = dateStr as unknown as string[];
                if (strs && Array.isArray(strs) && strs[0] && strs[1]) {
                  onStartDateChange(strs[0].replace(/-/g, ''));
                  onEndDateChange(strs[1].replace(/-/g, ''));
                } else {
                  onStartDateChange('');
                  onEndDateChange('');
                }
              }}
              style={{ width: '100%' }}
              size="small"
            />
            {startDate && endDate && (
              <div
                style={{
                  marginTop: 8,
                  padding: '8px 12px',
                  background: 'var(--color-primary-light-default)',
                  borderRadius: '6px',
                }}
              >
                <span style={{ color: 'var(--color-primary)', fontSize: '13px', fontWeight: 500 }}>
                  共 {dayjs(endDate, 'YYYYMMDD').diff(dayjs(startDate, 'YYYYMMDD'), 'day') + 1} 天
                </span>
              </div>
            )}
          </div>
        )}

        {isBatchMode && fullIds.length > 0 && incIds.length === 0 && (
          <div
            style={{
              padding: '12px',
              background: 'var(--color-success-light-default)',
              borderRadius: '6px',
            }}
          >
            <div style={{ fontWeight: 500, fontSize: '13px', marginBottom: 4 }}>
              全量回溯模式
            </div>
            <div style={{ marginTop: 6, color: 'var(--text-secondary)', fontSize: 12 }}>
              所有选中的任务都是全量任务，将回溯最新数据，无需指定日期参数。
            </div>
          </div>
        )}

        {isSingleFullTask && (
          <div
            style={{
              padding: '12px',
              background: 'var(--color-success-light-default)',
              borderRadius: '6px',
              border: '1px solid var(--border-color)',
            }}
          >
            <div style={{ color: 'var(--color-success)', fontSize: '13px', fontWeight: 500 }}>
              全量任务回溯
            </div>
            <div style={{ marginTop: 6, color: 'var(--text-secondary)', fontSize: 12 }}>
              此任务为全量任务，将回溯最新数据，无需指定日期参数。
            </div>
          </div>
        )}
      </div>
    </Modal>
  );
};

interface DeleteConfirmModalProps {
  visible: boolean;
  taskId: string;
  type: 'sync' | 'etl';
  onOk: () => void;
  onCancel: () => void;
}

export const DeleteConfirmModal: React.FC<DeleteConfirmModalProps> = ({
  visible,
  taskId,
  type,
  onOk,
  onCancel,
}) => {
  return (
    <Modal
      title="确认删除"
      visible={visible}
      onOk={onOk}
      onCancel={onCancel}
      okText="确认删除"
      cancelText="取消"
      type="warning"
    >
      <div style={{ padding: '12px 0' }}>
        <p style={{ marginBottom: 8 }}>
          确定要删除{type === 'sync' ? '同步' : 'ETL'}任务 <code>{taskId}</code> 吗？
        </p>
        <p style={{ color: 'var(--color-danger)', fontSize: '13px', margin: 0 }}>
          此操作将同时删除关联的数据表，且不可撤销。
        </p>
      </div>
    </Modal>
  );
};

/**
 * 表结构变化确认对话框
 */
interface SchemaChangeConfirmModalProps {
  visible: boolean;
  tableName: string;
  taskType: 'sync' | 'etl';
  changes?: {
    old_schema?: Record<string, string>;
    new_schema?: Record<string, string>;
    script_changed?: boolean;
    primary_keys_changed?: boolean;
  };
  onOk: () => void;
  onCancel: () => void;
}

export const SchemaChangeConfirmModal: React.FC<SchemaChangeConfirmModalProps> = ({
  visible,
  tableName,
  taskType,
  changes,
  onOk,
  onCancel,
}) => {
  return (
    <Modal
      title="⚠️ 表结构变化确认"
      visible={visible}
      onOk={onOk}
      onCancel={onCancel}
      okText="确认并清空数据"
      cancelText="取消"
      okButtonProps={{ type: 'danger' }}
      style={{ width: 600 }}
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <div
          style={{
            padding: '12px 16px',
            backgroundColor: 'var(--semi-color-warning-light-default)',
            borderRadius: '6px',
            border: '1px solid var(--semi-color-warning)',
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 8, color: 'var(--semi-color-warning-dark)' }}>
            检测到表结构变化
          </div>
          <div style={{ fontSize: '13px', color: 'var(--text-secondary)', lineHeight: 1.6 }}>
            {taskType === 'sync' ? '同步任务的表结构配置' : 'ETL 任务的脚本或主键配置'}
            发生了变化，这可能导致数据表结构不兼容。
          </div>
        </div>

        <div>
          <div style={{ fontWeight: 500, marginBottom: 8 }}>目标表名</div>
          <code
            style={{
              padding: '6px 12px',
              backgroundColor: 'var(--semi-color-fill-0)',
              borderRadius: '4px',
              fontSize: '13px',
              color: 'var(--color-primary)',
            }}
          >
            {tableName}
          </code>
        </div>

        {changes && (
          <div>
            <div style={{ fontWeight: 500, marginBottom: 8 }}>变化详情</div>
            <div
              style={{
                padding: '12px',
                backgroundColor: 'var(--semi-color-fill-0)',
                borderRadius: '4px',
                fontSize: '12px',
              }}
            >
              {taskType === 'sync' && changes.old_schema && changes.new_schema && (
                <>
                  <div style={{ marginBottom: 8 }}>
                    <strong>旧表结构:</strong>
                    <div style={{ marginTop: 4, color: 'var(--text-secondary)' }}>
                      {Object.keys(changes.old_schema).join(', ')}
                    </div>
                  </div>
                  <div>
                    <strong>新表结构:</strong>
                    <div style={{ marginTop: 4, color: 'var(--color-primary)' }}>
                      {Object.keys(changes.new_schema).join(', ')}
                    </div>
                  </div>
                </>
              )}
              {taskType === 'etl' && (
                <>
                  {changes.script_changed && (
                    <div style={{ marginBottom: 4 }}>• ETL 脚本已修改</div>
                  )}
                  {changes.primary_keys_changed && (
                    <div>• 主键配置已修改</div>
                  )}
                </>
              )}
            </div>
          </div>
        )}

        <div
          style={{
            padding: '12px 16px',
            backgroundColor: 'var(--semi-color-danger-light-default)',
            borderRadius: '6px',
            border: '1px solid var(--semi-color-danger-light-active)',
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 8, color: 'var(--semi-color-danger)' }}>
            ⚠️ 警告
          </div>
          <div style={{ fontSize: '13px', color: 'var(--text-secondary)', lineHeight: 1.6 }}>
            确认后将会：
            <ul style={{ margin: '8px 0', paddingLeft: '20px' }}>
              <li>删除表 <code>{tableName}</code> 及其所有数据</li>
              <li>下次同步时将根据新的表结构重新创建表</li>
              <li>此操作不可撤销</li>
            </ul>
          </div>
        </div>
      </div>
    </Modal>
  );
};
