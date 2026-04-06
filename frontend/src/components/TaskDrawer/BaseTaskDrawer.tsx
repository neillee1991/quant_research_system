/**
 * 基础任务抽屉组件
 * 提供统一的抽屉壳、标题、底部按钮
 */
import React from 'react';
import { Drawer, Button } from 'antd';
import type { DrawerProps } from 'antd/es/drawer';

interface BaseTaskDrawerProps extends Omit<DrawerProps, 'onClose' | 'footer'> {
  // 基础属性
  visible: boolean;
  title: string;
  onClose: () => void;

  // 保存相关
  onSave?: () => void;
  saveLoading?: boolean;
  saveText?: string;

  // 底部操作
  extraFooterActions?: React.ReactNode;

  // 内容
  children: React.ReactNode;

  // 抽屉宽度
  width?: number | string;
}

export const BaseTaskDrawer: React.FC<BaseTaskDrawerProps> = ({
  visible,
  title,
  onClose,
  onSave,
  saveLoading = false,
  saveText = '保存',
  extraFooterActions,
  children,
  width = 900,
  ...drawerProps
}) => {
  return (
    <Drawer
      title={title}
      open={visible}
      onClose={onClose}
      width={width}
      maskClosable={true}
      destroyOnClose
      footer={
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div style={{ display: 'flex', gap: 8 }}>
            {extraFooterActions}
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <Button onClick={onClose}>
              取消
            </Button>
            {onSave && (
              <Button type="primary" onClick={onSave} loading={saveLoading}>
                {saveText}
              </Button>
            )}
          </div>
        </div>
      }
      {...drawerProps}
    >
      {children}
    </Drawer>
  );
};
