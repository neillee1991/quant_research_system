# FactorCenter 重构实施指南

## 快速开始

### 已完成的工作 ✅

```
pages/FactorCenter/
├── index.tsx                    ✅ 主页面入口
├── types.ts                     ✅ 类型定义
├── TestPanel.tsx                ✅ 测试面板
├── AnalysisPanel.tsx            ✅ 分析面板
├── DataConfigPanel.tsx          ✅ 数据配置面板
├── REFACTORING_PLAN.md          ✅ 重构方案
├── COMPLETION_REPORT.md         ✅ 完成报告
└── hooks/
    ├── useFactorList.ts         ✅ 因子列表逻辑
    ├── useFactorTest.ts         ✅ 测试逻辑
    ├── useDataConfig.ts         ✅ 数据配置逻辑
    └── useFactorAnalysis.ts     ✅ 分析逻辑
```

### 待完成的工作 🔄

```
pages/FactorCenter/
├── FactorDrawer.tsx             🔄 因子编辑抽屉 (~400 行)
└── FactorManageTab.tsx          🔄 因子管理标签页 (~600 行)
```

## 完成 FactorDrawer.tsx

### 步骤 1: 提取代码
从原始文件 `FactorCenter.tsx` 的第 67-478 行提取 `FactorDrawer` 组件。

### 步骤 2: 修改导入
```typescript
import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  SideSheet, Tabs, TabPane, Button, Input, Select, InputNumber,
  Collapse, Card, Spin, Empty, Table, Tag, Tooltip, Toast,
} from '@douyinfe/semi-ui';
import {
  IconEdit, IconSave, IconCode, IconServer, IconHistory, IconRefresh, IconSearch,
} from '@douyinfe/semi-icons';
import Editor from '@monaco-editor/react';
import { productionApi, DEFAULT_PREPROCESS } from '../../api';
import { useThemeStore } from '../../store';
import { formatCode } from '../../utils/codeFormatter';
import type { PreprocessOptions, FactorValue, FactorAnalysisResult } from '../../types';
import type { FactorDrawerProps, FactorCodeInfo, DataConfigLabel } from './types';
import { formatRunParams } from './types';
import TestPanel from './TestPanel';
```

### 步骤 3: 关键修改点
1. **导入 TestPanel**: 替换内联的 `CodeTestPanel` 为 `<TestPanel />`
2. **错误处理**: 将所有空 catch 块改为完整的错误处理
3. **类型导入**: 从 `./types` 导入内部类型

### 步骤 4: 替换 CodeTestPanel
原代码第 434 行:
```typescript
<CodeTestPanel code={editedCode} preprocess={ppEdit} />
```

替换为:
```typescript
<TestPanel code={editedCode} dependsOn={editDependsOn} preprocess={ppEdit} />
```

## 完成 FactorManageTab.tsx

### 步骤 1: 提取代码
从原始文件 `FactorCenter.tsx` 的第 704-1306 行提取 `FactorManageTab` 组件。

### 步骤 2: 修改导入
```typescript
import React, { useState, useEffect, useRef } from 'react';
import {
  Card, Table, Button, Tag, Select, Modal, Popconfirm, Checkbox,
  Toast, DatePicker, Banner,
} from '@douyinfe/semi-ui';
import {
  IconPlus, IconDelete, IconEdit, IconBolt, IconPlay, IconRefresh, IconCopy,
} from '@douyinfe/semi-icons';
import dayjs from 'dayjs';
import Editor from '@monaco-editor/react';
import { productionApi, DEFAULT_PREPROCESS } from '../../api';
import { useThemeStore } from '../../store';
import { formatCode } from '../../utils/codeFormatter';
import { VersionHistory } from '../../components/VersionHistory';
import type { PreprocessOptions, FactorDefinition } from '../../types';
import { CODE_TEMPLATE, formatRunParams } from './types';
import { useFactorList } from './hooks/useFactorList';
import FactorDrawer from './FactorDrawer';
```

### 步骤 3: 使用 useFactorList Hook
替换原有的状态管理:
```typescript
// 原代码
const [factors, setFactors] = useState<FactorDefinition[]>([]);
const [loading, setLoading] = useState<boolean>(false);
// ... 更多状态

const loadFactors = useCallback(async () => {
  // ... 加载逻辑
}, []);

// 替换为
const {
  factors,
  history,
  loading,
  runLoading,
  selectedFactor,
  setSelectedFactor,
  loadFactors,
  loadHistory,
  runFactor,
  deleteFactor,
} = useFactorList();
```

### 步骤 4: 集成 FactorDrawer
替换原有的抽屉状态:
```typescript
<FactorDrawer
  factor={drawerState.factor}
  open={drawerState.open}
  initialTab={drawerState.tab}
  onClose={() => setDrawerState({ open: false, factor: null })}
  onSaved={() => {
    loadFactors();
    loadHistory();
  }}
/>
```

## 测试清单

### 功能测试
- [ ] 因子列表加载正常
- [ ] 创建新因子功能正常
- [ ] 编辑因子功能正常
- [ ] 删除因子功能正常
- [ ] 运行因子（增量/全量）正常
- [ ] 批量操作正常
- [ ] 代码测试功能正常
- [ ] 因子分析功能正常
- [ ] 数据配置功能正常
- [ ] 版本历史功能正常

### 错误处理测试
- [ ] API 失败时显示错误提示
- [ ] 网络错误时显示错误提示
- [ ] 表单验证正常
- [ ] 空数据状态显示正常

### UI 测试
- [ ] 所有按钮可点击
- [ ] 所有输入框可输入
- [ ] 所有下拉框可选择
- [ ] 所有表格可滚动
- [ ] 所有模态框可打开/关闭
- [ ] 所有抽屉可打开/关闭

## 迁移步骤

### 1. 备份原始文件
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/frontend/src/pages
cp FactorCenter.tsx FactorCenter.tsx.backup
```

### 2. 完成剩余组件
按照上述指南完成 FactorDrawer.tsx 和 FactorManageTab.tsx

### 3. 更新路由（如果需要）
检查 `App.tsx` 或路由配置文件，确保导入路径正确:
```typescript
// 原来
import FactorCenter from './pages/FactorCenter';

// 现在（路径相同，但实际是 index.tsx）
import FactorCenter from './pages/FactorCenter';
```

### 4. 运行测试
```bash
cd /Users/lisheng/Code/quantsystem/quant_research_system/frontend
npm start
```

### 5. 验证功能
按照测试清单逐项验证所有功能。

### 6. 清理
如果一切正常，删除备份文件:
```bash
rm FactorCenter.tsx.backup
```

## 常见问题

### Q1: 导入路径错误
**问题:** `Cannot find module './types'`
**解决:** 确保所有文件都在 `pages/FactorCenter/` 目录下

### Q2: Hook 依赖警告
**问题:** `React Hook useEffect has missing dependencies`
**解决:** 检查 useEffect 的依赖数组，添加缺失的依赖

### Q3: 类型错误
**问题:** `Type 'X' is not assignable to type 'Y'`
**解决:** 检查 types.ts 中的类型定义，确保与 API 响应匹配

### Q4: 组件不渲染
**问题:** 组件显示空白
**解决:** 检查 console 错误，通常是导入路径或 props 传递问题

## 性能优化建议

### 1. 使用 React.memo
对于不经常变化的组件:
```typescript
export default React.memo(TestPanel);
```

### 2. 使用 useCallback
对于传递给子组件的回调函数:
```typescript
const handleSave = useCallback(async () => {
  // ...
}, [dependencies]);
```

### 3. 使用 useMemo
对于复杂的计算:
```typescript
const chartOption = useMemo(() => getICChartOption(), [analysisResult]);
```

## 下一步改进

### 短期 (1-2 周)
1. 添加单元测试
2. 添加集成测试
3. 优化性能（React.memo, useCallback）
4. 添加加载骨架屏

### 中期 (1-2 月)
1. 提取更多可复用组件
2. 添加 Storybook 文档
3. 优化错误边界
4. 添加国际化支持

### 长期 (3-6 月)
1. 迁移到 React Query（数据获取）
2. 迁移到 Zustand（全局状态）
3. 添加离线支持
4. 优化包大小

## 联系和支持

如果遇到问题，请检查:
1. `REFACTORING_PLAN.md` - 详细的重构方案
2. `COMPLETION_REPORT.md` - 完成报告和进度
3. 原始文件 `FactorCenter.tsx.backup` - 参考原始实现

祝重构顺利！🚀
