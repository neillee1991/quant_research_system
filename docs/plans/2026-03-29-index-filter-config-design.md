# 指数筛选项配置设计

**日期**: 2026-03-29

## 需求

在配置指数基础信息表时，允许用户选择哪些字段作为指数列表页的筛选项，并设置默认值。配置一次性保存，后续打开指数列表直接生效。

## 方案

持久化到现有 `user_sync_preference` 表，新增 `filter_config` JSON 字段。

## 数据结构

```json
{
  "filters": [
    { "field": "market", "label": "市场", "enabled": true, "default_value": "SSE" },
    { "field": "publisher", "label": "发布机构", "enabled": true, "default_value": null }
  ]
}
```

## 改动范围

### 后端
1. `user_sync_preference` 表加 `filter_config` STRING 字段
2. `GET /data/index/preference` 响应增加 `filter_config`
3. `POST /data/index/preference` 请求增加 `filter_config`
4. `GET /data/index/available` 筛选参数从固定 `market`/`publisher` 改为动态 `filters` dict，字段名白名单校验

### 前端
1. 配置阶段：选表后读取字段列表，展示勾选 + 默认值配置 UI
2. 指数列表筛选栏：根据 `filter_config` 动态渲染，替换硬编码的 market/publisher
