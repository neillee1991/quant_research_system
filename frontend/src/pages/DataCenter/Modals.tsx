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
  taskId: string;
  startDate: string;
  endDate: string;
  onStartDateChange: (date: string) => void;
  onEndDateChange: (date: string) => void;
  onOk: () => void;
  onCancel: () => void;
}

export const ETLBackfillModal: React.FC<ETLBackfillModalProps> = ({
  visible,
  taskId,
  startDate,
  endDate,
  onStartDateChange,
  onEndDateChange,
  onOk,
  onCancel,
}) => {
  return (
    <Modal
      title={`ETL 任务回溯: ${taskId}`}
      visible={visible}
      onOk={onOk}
      onCancel={onCancel}
      okText="开始回溯"
      cancelText="取消"
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        <div>
          <div style={{ marginBottom: 6, fontWeight: 500, fontSize: '13px' }}>回溯日期范围</div>
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
