# 策略脚本回测 Phase 0/1 契约说明

## 目标
在不破坏现有 graph 回测链路的前提下，为策略中心新增 `script` 模式的最小骨架，完成前端代码编辑器入口与后端占位接口契约。

## Phase 0 冻结项
- 双主并行：`graph` 与 `script` 同时存在
- 最终方向：`script` 成为主路径，`graph` 保留兼容层
- 当前阶段不启用真实脚本执行，只提供 API 契约与占位返回

## 新增接口
### POST `/api/v1/strategy/backtest/script/validate`
请求：
```json
{
  "script": "def build_strategy():\n    return {}",
  "language": "python"
}
```
响应：
```json
{
  "valid": true,
  "language": "python",
  "script_hash": "sha256...",
  "warnings": ["Phase 1 骨架占位"],
  "errors": []
}
```

### POST `/api/v1/strategy/backtest/script/compile`
响应包含占位 `ir`：
```json
{
  "status": "compiled",
  "script_hash": "sha256...",
  "ir": {
    "source_type": "script",
    "language": "python",
    "entry_point": "build_strategy",
    "pipeline_version": "phase1"
  },
  "warnings": []
}
```

### POST `/api/v1/strategy/backtest/script`
当前阶段返回 `202 Accepted` 占位结果：
```json
{
  "run_id": "script_xxx",
  "task_id": "script_backtest",
  "mode": "script",
  "status": "not_implemented",
  "message": "Phase 1 骨架已就绪，但真实脚本执行尚未启用",
  "script_hash": "sha256..."
}
```

### GET `/api/v1/strategy/backtest/runs/{run_id}`
统一查询运行结果。当前优先兼容已存在的 `backtest_results` 表记录。

## 建议新增字段
### task_runs
- `mode`
- `script_hash`
- `ir_version`
- `pipeline_version`
- `error_stage`

### backtest_results
- `mode`
- `script_hash`
- `metrics_json`
- `warnings_json`

## 前端范围
- `StrategyCenter` 增加 `graph/code` 模式切换
- 新增 `StrategyCodeEditor`
- 接入 `validateScript / compileScript / backtestScript` API 封装
- 暂不删除 `FlowEditor`

## 后续阶段
- Phase 2：真实 AST 白名单校验 + script -> IR + 安全沙箱
- Phase 3：script 默认路径 + graph/script 双跑对账
