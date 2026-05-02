/**
 * FactorTaskDrawer - wraps FactorDrawer for use in the unified TaskPanel architecture
 */
import React from 'react';
import type { FactorDefinition } from '../../types';
import FactorDrawer from './FactorDrawer';

interface FactorTaskDrawerProps {
  visible: boolean;
  task: FactorDefinition | null;
  isNew: boolean;
  onClose: () => void;
  onSave: () => void;
}

const FactorTaskDrawer: React.FC<FactorTaskDrawerProps> = ({ visible, task, onClose, onSave }) => {
  return (
    <FactorDrawer
      factor={task}
      open={visible}
      onClose={onClose}
      onSaved={onSave}
    />
  );
};

export { FactorTaskDrawer };
export default FactorTaskDrawer;
