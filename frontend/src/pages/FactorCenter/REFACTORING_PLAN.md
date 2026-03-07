# FactorCenter 组件拆分方案

## 概览

原始文件: `/Users/lisheng/Code/quantsystem/quant_research_system/frontend/src/pages/FactorCenter.tsx` (1755 行)

拆分后结构:
```
pages/FactorCenter/
├── index.tsx                    # 主页面入口 (~50 行) ✅
├── types.ts                     # 类型定义 (~130 行) ✅
├── FactorManageTab.tsx          # 因子管理标签页 (~600 行) 🔄
├── FactorDrawer.tsx             # 因子编辑抽屉 (~400 行) 🔄
├── TestPanel.tsx                # 测试面板 (~220 行) ✅
├── AnalysisPanel.tsx            # 分析面板 (~300 行) 🔄
├── DataConfigPanel.tsx          # 数据配置面板 (~170 行) ✅
└── hooks/
    ├── useFactorList.ts         # 因子列表逻辑 (~90 行) ✅
    ├── useFactorTest.ts         # 测试逻辑 (~60 行) ✅
    ├── useDataConfig.ts         # 数据配置逻辑 (~90 行) ✅
    └── useFactorAnalysis.ts     # 分析逻辑 (~140 行) ✅
```

## 已完成的文件

### 1. types.ts ✅
- 导出所有内部类型定义
- 包含 `formatRunParams` 工具函数
- 包含 `CODE_TEMPLATE` 常量

### 2. hooks/useFactorList.ts ✅
- 因子列表加载和管理
- 历史记录加载
- 因子运行和删除操作
- 改进的错误处理（消除空 catch）

### 3. hooks/useFactorTest.ts ✅
- 因子代码测试逻辑
- 测试结果管理
- 完整的错误处理

### 4. hooks/useDataConfig.ts ✅
- 数据配置加载和保存
- 表和列的动态加载
- 映射更新逻辑

### 5. hooks/useFactorAnalysis.ts ✅
- 因子分析逻辑
- Alphalens 分析支持
- 分析历史管理

### 6. TestPanel.tsx ✅
- 代码测试面板组件
- 日志显示
- 结果预览和筛选

### 7. DataConfigPanel.tsx ✅
- 数据配置界面
- 使用 useDataConfig hook
- 表和列选择器

## 待创建的文件

### 8. FactorManageTab.tsx 🔄
**功能模块:**
- 因子列表表格
- 批量操作
- 创建因子模态框
- 全量运行模态框
- 版本历史集成

**依赖:**
- useFactorList hook
- FactorDrawer 组件
- VersionHistory 组件

**预估行数:** ~600 行

### 9. FactorDrawer.tsx 🔄
**功能模块:**
- 因子详情/编辑统一抽屉
- 多标签页: 编辑、代码、统计、数据、日志
- 代码编辑器集成
- 预处理选项配置
- TestPanel 集成

**依赖:**
- TestPanel 组件
- Monaco Editor
- VersionHistory 组件

**预估行数:** ~400 行

### 10. AnalysisPanel.tsx 🔄
**功能模块:**
- 因子分析参数配置
- IC 分析图表
- 分组收益图表
- 分析历史记录

**依赖:**
- useFactorAnalysis hook
- ReactECharts

**预估行数:** ~300 行

### 11. index.tsx 🔄
**功能:**
- 主页面布局
- 标签页导航
- 集成所有子组件

**预估行数:** ~50 行

## 组件依赖关系图

```
index.tsx
├── FactorManageTab
│   ├── useFactorList (hook)
│   ├── FactorDrawer
│   │   ├── TestPanel
│   │   │   └── useFactorTest (hook)
│   │   └── Monaco Editor
│   └── VersionHistory
├── AnalysisPanel
│   ├── useFactorAnalysis (hook)
│   └── ReactECharts
└── DataConfigPanel
    └── useDataConfig (hook)
```

## 关键改进

### 1. 错误处理改进
- ❌ 空 catch 块: `catch { }`
- ✅ 改进后: `catch (error) { console.error(...); Toast.error(...); }`

### 2. 状态管理
- 使用自定义 Hooks 封装业务逻辑
- 组件只负责 UI 渲染
- 清晰的状态流转

### 3. 类型安全
- 统一的类型定义文件
- 使用已有的 `types/factor.ts`
- 避免 `any` 类型

### 4. 代码复用
- 提取可复用的 Hooks
- 组件间通过 props 通信
- 避免重复代码

## 下一步操作

1. **创建 FactorManageTab.tsx**
   - 从原文件提取 704-1306 行
   - 集成 useFactorList hook
   - 添加错误处理

2. **创建 FactorDrawer.tsx**
   - 从原文件提取 67-479 行
   - 集成 TestPanel 组件
   - 优化代码编辑器集成

3. **创建 AnalysisPanel.tsx**
   - 从原文件提取 1307-1591 行
   - 集成 useFactorAnalysis hook
   - 优化图表渲染

4. **创建 index.tsx**
   - 从原文件提取 1726-1755 行
   - 集成所有标签页组件
   - 保持简洁的主入口

5. **测试验证**
   - 功能完整性测试
   - 组件间通信测试
   - 错误处理测试

## 预期收益

- **可维护性**: 每个文件 < 600 行，职责清晰
- **可测试性**: Hooks 和组件可独立测试
- **可复用性**: Hooks 可在其他页面复用
- **代码质量**: 消除空 catch 块，改进错误处理
- **开发效率**: 修改某个功能只需关注对应文件

## 风险和注意事项

1. **状态同步**: 确保父子组件间状态正确传递
2. **回调函数**: onSaved、onClose 等回调需正确触发
3. **副作用**: useEffect 依赖项需仔细检查
4. **性能**: 避免不必要的重渲染
5. **向后兼容**: 保持所有现有功能正常工作
