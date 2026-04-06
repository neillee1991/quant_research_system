# 配置备份与导入功能实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 实现配置备份与导入功能，支持同步任务、ETL任务、因子配置、数据配置的导出和导入，包含快速模式和安全模式。

**Architecture:** 后端新增配置管理 API，前端新增独立配置管理页面。使用服务层封装导出、验证、差异计算和导入逻辑。

**Tech Stack:** FastAPI, Pydantic, React, TypeScript, Ant Design, DolphinDB

---

## 前置准备

先让我读取一些关键文件以了解现有代码结构：

**需要读取的文件：**
1. `app/main.py` - 了解路由注册方式
2. `app/models/base_task.py` - 了解现有模型结构
3. `app/services/task_service.py` - 了解任务服务模式
4. `infrastructure/database/metadata_manager.py` - 了解表结构
5. 前端路由和现有页面结构

---

### Task 1: 创建 Pydantic 模型

**Files:**
- Create: `app/models/config_import_export.py`

**Step 1: 创建配置导入导出模型文件**

```python
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class ConfigType(str, Enum):
    SYNC_TASKS = "sync_tasks"
    ETL_TASKS = "etl_tasks"
    FACTOR_METADATA = "factor_metadata"
    FACTOR_DATA_CONFIG = "factor_data_config"


class ImportMode(str, Enum):
    FAST = "fast"
    SAFE = "safe"


class ExportRequest(BaseModel):
    config_types: List[ConfigType] = Field(..., description="要导出的配置类型列表")


class ExportResponse(BaseModel):
    filename: str = Field(..., description="导出文件名")
    content: str = Field(..., description="Base64 编码的 JSON 内容")


class ConfigItemDiff(BaseModel):
    item_id: str = Field(..., description="配置项 ID")
    status: str = Field(..., description="状态: new/modified/unchanged/deleted")
    current: Optional[Dict[str, Any]] = Field(None, description="当前配置（如存在）")
    imported: Optional[Dict[str, Any]] = Field(None, description="导入配置（如存在）")


class ConfigTypeDiff(BaseModel):
    config_type: ConfigType = Field(..., description="配置类型")
    items: List[ConfigItemDiff] = Field(default_factory=list, description="配置项差异列表")
    summary: Dict[str, int] = Field(default_factory=dict, description="统计: new/modified/unchanged/deleted")


class ImportVerifyRequest(BaseModel):
    content: str = Field(..., description="Base64 编码的 JSON 内容")
    mode: ImportMode = Field(..., description="导入模式")


class ImportVerifyResponse(BaseModel):
    valid: bool = Field(..., description="是否验证通过")
    errors: List[str] = Field(default_factory=list, description="错误列表")
    diffs: Optional[List[ConfigTypeDiff]] = Field(None, description="差异预览（安全模式）")


class ImportApplyRequest(BaseModel):
    content: str = Field(..., description="Base64 编码的 JSON 内容")
    mode: ImportMode = Field(..., description="导入模式")
    selections: Optional[Dict[ConfigType, List[str]]] = Field(None, description="选中的配置项 ID 列表")


class ImportResultSummary(BaseModel):
    created: int = Field(0, description="新创建数量")
    updated: int = Field(0, description="更新数量")
    skipped: int = Field(0, description="跳过数量")


class ImportApplyResponse(BaseModel):
    success: bool = Field(..., description="是否成功")
    summary: Dict[ConfigType, ImportResultSummary] = Field(default_factory=dict, description="各类型导入结果统计")
    errors: List[str] = Field(default_factory=list, description="错误列表")


class BackupFile(BaseModel):
    version: str = Field("1.0", description="备份文件格式版本")
    exported_at: datetime = Field(..., description="导出时间")
    system_version: str = Field("1.0.0", description="系统版本")
    configs: Dict[ConfigType, List[Dict[str, Any]]] = Field(default_factory=dict, description="配置数据")
```

**Step 2: 在 `app/models/__init__.py` 中导出新模型**

先读取现有文件：
Read: `app/models/__init__.py`

然后添加：
```python
from .config_import_export import (
    ConfigType,
    ImportMode,
    ExportRequest,
    ExportResponse,
    ConfigItemDiff,
    ConfigTypeDiff,
    ImportVerifyRequest,
    ImportVerifyResponse,
    ImportApplyRequest,
    ImportApplyResponse,
    ImportResultSummary,
    BackupFile,
)

__all__ = [
    # ... 现有导出 ...
    "ConfigType",
    "ImportMode",
    "ExportRequest",
    "ExportResponse",
    "ConfigItemDiff",
    "ConfigTypeDiff",
    "ImportVerifyRequest",
    "ImportVerifyResponse",
    "ImportApplyRequest",
    "ImportApplyResponse",
    "ImportResultSummary",
    "BackupFile",
]
```

---

### Task 2: 创建配置导出服务

**Files:**
- Create: `app/services/config_export_service.py`

**Step 1: 创建配置导出服务**

```python
import base64
import json
from datetime import datetime
from typing import Dict, List, Any
from ..models.config_import_export import ConfigType, BackupFile
from infrastructure.database.metadata_manager import MetadataManager
from store.dolphindb_client import DolphinDBClient


class ConfigExportService:
    def __init__(self):
        self.ddb_client = DolphinDBClient()
        self.metadata_manager = MetadataManager()

    def export_configs(self, config_types: List[ConfigType]) -> Dict[str, Any]:
        backup = BackupFile(
            exported_at=datetime.utcnow(),
            configs={}
        )

        for config_type in config_types:
            backup.configs[config_type] = self._export_config_type(config_type)

        # 转换为 JSON 字符串并 Base64 编码
        json_str = json.dumps(backup.model_dump(), default=str, ensure_ascii=False)
        base64_content = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

        filename = f"config_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

        return {
            "filename": filename,
            "content": base64_content
        }

    def _export_config_type(self, config_type: ConfigType) -> List[Dict[str, Any]]:
        table_map = {
            ConfigType.SYNC_TASKS: "sync_task_config",
            ConfigType.ETL_TASKS: "etl_task_config",
            ConfigType.FACTOR_METADATA: "factor_metadata",
            ConfigType.FACTOR_DATA_CONFIG: "factor_data_config",
        }

        table_name = table_map.get(config_type)
        if not table_name:
            return []

        return self._read_table_as_dicts(table_name)

    def _read_table_as_dicts(self, table_name: str) -> List[Dict[str, Any]]:
        try:
            df = self.ddb_client.read_table(table_name)
            if df is None or df.is_empty():
                return []

            # 转换为字典列表
            result = []
            for row in df.to_dicts():
                # 清理不需要的字段（如 created_at, updated_at 等由系统自动管理的字段）
                cleaned_row = {}
                for key, value in row.items():
                    if key not in ['created_at', 'updated_at']:
                        cleaned_row[key] = value
                result.append(cleaned_row)
            return result
        except Exception as e:
            # 表可能不存在，返回空列表
            return []
```

---

### Task 3: 创建配置差异计算服务

**Files:**
- Create: `app/services/config_diff_service.py`

**Step 1: 创建配置差异计算服务**

```python
from typing import Dict, List, Any
from ..models.config_import_export import (
    ConfigType,
    ConfigItemDiff,
    ConfigTypeDiff,
)


class ConfigDiffService:
    def compute_diff(
        self,
        current_configs: Dict[ConfigType, List[Dict[str, Any]]],
        imported_configs: Dict[ConfigType, List[Dict[str, Any]]]
    ) -> List[ConfigTypeDiff]:
        diffs = []

        all_config_types = set(current_configs.keys()) | set(imported_configs.keys())

        for config_type in all_config_types:
            type_diff = self._compute_type_diff(
                config_type,
                current_configs.get(config_type, []),
                imported_configs.get(config_type, [])
            )
            diffs.append(type_diff)

        return diffs

    def _compute_type_diff(
        self,
        config_type: ConfigType,
        current_items: List[Dict[str, Any]],
        imported_items: List[Dict[str, Any]]
    ) -> ConfigTypeDiff:
        id_field = self._get_id_field(config_type)

        current_map = {item[id_field]: item for item in current_items if id_field in item}
        imported_map = {item[id_field]: item for item in imported_items if id_field in item}

        items = []
        summary = {"new": 0, "modified": 0, "unchanged": 0, "deleted": 0}

        # 检查新增和修改的项
        for item_id, imported_item in imported_map.items():
            current_item = current_map.get(item_id)
            if current_item is None:
                items.append(ConfigItemDiff(
                    item_id=item_id,
                    status="new",
                    current=None,
                    imported=imported_item
                ))
                summary["new"] += 1
            else:
                if self._items_equal(current_item, imported_item):
                    items.append(ConfigItemDiff(
                        item_id=item_id,
                        status="unchanged",
                        current=current_item,
                        imported=imported_item
                    ))
                    summary["unchanged"] += 1
                else:
                    items.append(ConfigItemDiff(
                        item_id=item_id,
                        status="modified",
                        current=current_item,
                        imported=imported_item
                    ))
                    summary["modified"] += 1

        # 检查删除的项（只在 current 中存在）
        for item_id, current_item in current_map.items():
            if item_id not in imported_map:
                items.append(ConfigItemDiff(
                    item_id=item_id,
                    status="deleted",
                    current=current_item,
                    imported=None
                ))
                summary["deleted"] += 1

        return ConfigTypeDiff(
            config_type=config_type,
            items=items,
            summary=summary
        )

    def _get_id_field(self, config_type: ConfigType) -> str:
        id_fields = {
            ConfigType.SYNC_TASKS: "task_id",
            ConfigType.ETL_TASKS: "task_id",
            ConfigType.FACTOR_METADATA: "factor_id",
            ConfigType.FACTOR_DATA_CONFIG: "field_key",
        }
        return id_fields.get(config_type, "id")

    def _items_equal(self, item1: Dict[str, Any], item2: Dict[str, Any]) -> bool:
        # 比较两个配置项是否相等，忽略顺序
        def normalize(item):
            if isinstance(item, dict):
                return sorted((k, normalize(v)) for k, v in item.items() if k not in ['created_at', 'updated_at'])
            elif isinstance(item, list):
                return sorted(normalize(x) for x in item)
            else:
                return item

        return normalize(item1) == normalize(item2)
```

---

### Task 4: 创建配置导入服务

**Files:**
- Create: `app/services/config_import_service.py`

**Step 1: 创建配置导入服务**

```python
import base64
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from ..models.config_import_export import (
    ConfigType,
    ImportMode,
    BackupFile,
    ConfigTypeDiff,
    ImportResultSummary,
)
from .config_diff_service import ConfigDiffService
from .config_export_service import ConfigExportService
from infrastructure.database.metadata_manager import MetadataManager
from store.dolphindb_client import DolphinDBClient


class ConfigImportService:
    def __init__(self):
        self.ddb_client = DolphinDBClient()
        self.metadata_manager = MetadataManager()
        self.diff_service = ConfigDiffService()
        self.export_service = ConfigExportService()

    def verify_import(
        self,
        content: str,
        mode: ImportMode
    ) -> Tuple[bool, List[str], Optional[List[ConfigTypeDiff]], Optional[BackupFile]]:
        errors = []
        diffs = None
        backup = None

        try:
            # 1. 解码和解析
            backup = self._parse_backup_content(content)
            if not backup:
                errors.append("无法解析备份文件")
                return False, errors, None, None

            # 2. 验证备份文件结构
            structure_valid, structure_errors = self._validate_backup_structure(backup)
            errors.extend(structure_errors)
            if not structure_valid:
                return False, errors, None, backup

            # 3. 验证配置内容
            content_valid, content_errors = self._validate_config_contents(backup)
            errors.extend(content_errors)
            if not content_valid:
                return False, errors, None, backup

            # 4. 安全模式下计算差异
            if mode == ImportMode.SAFE:
                current_configs = self._get_current_configs(list(backup.configs.keys()))
                diffs = self.diff_service.compute_diff(current_configs, backup.configs)

            return True, errors, diffs, backup

        except Exception as e:
            errors.append(f"验证过程出错: {str(e)}")
            return False, errors, None, backup

    def apply_import(
        self,
        content: str,
        mode: ImportMode,
        selections: Optional[Dict[ConfigType, List[str]]] = None
    ) -> Tuple[bool, Dict[ConfigType, ImportResultSummary], List[str]]:
        errors = []
        summary = {}

        try:
            # 先验证
            valid, verify_errors, diffs, backup = self.verify_import(content, mode)
            errors.extend(verify_errors)
            if not valid or not backup:
                return False, summary, errors

            # 执行导入
            for config_type, items in backup.configs.items():
                id_field = self._get_id_field(config_type)
                result = ImportResultSummary()

                # 确定要导入的项
                items_to_import = items
                if mode == ImportMode.SAFE and selections:
                    selected_ids = selections.get(config_type, [])
                    items_to_import = [item for item in items if item.get(id_field) in selected_ids]

                # 导入每一项
                for item in items_to_import:
                    item_id = item.get(id_field)
                    exists = self._item_exists(config_type, item_id)

                    if exists:
                        if mode == ImportMode.FAST:
                            # 快速模式：覆盖
                            self._update_item(config_type, item)
                            result.updated += 1
                        else:
                            # 安全模式：检查是否选中修改
                            if self._is_item_selected_for_update(config_type, item_id, diffs, selections):
                                self._update_item(config_type, item)
                                result.updated += 1
                            else:
                                result.skipped += 1
                    else:
                        self._create_item(config_type, item)
                        result.created += 1

                summary[config_type] = result

            return True, summary, errors

        except Exception as e:
            errors.append(f"导入过程出错: {str(e)}")
            return False, summary, errors

    def _parse_backup_content(self, content: str) -> Optional[BackupFile]:
        try:
            json_bytes = base64.b64decode(content.encode('utf-8'))
            json_str = json_bytes.decode('utf-8')
            data = json.loads(json_str)
            return BackupFile(**data)
        except Exception:
            return None

    def _validate_backup_structure(self, backup: BackupFile) -> Tuple[bool, List[str]]:
        errors = []
        if not backup.version:
            errors.append("缺少版本信息")
        if not backup.exported_at:
            errors.append("缺少导出时间")
        if not backup.configs:
            errors.append("没有配置数据")
        return len(errors) == 0, errors

    def _validate_config_contents(self, backup: BackupFile) -> Tuple[bool, List[str]]:
        errors = []
        for config_type, items in backup.configs.items():
            id_field = self._get_id_field(config_type)
            for idx, item in enumerate(items):
                if id_field not in item:
                    errors.append(f"{config_type}[{idx}]: 缺少 {id_field} 字段")
        return len(errors) == 0, errors

    def _get_current_configs(self, config_types: List[ConfigType]) -> Dict[ConfigType, List[Dict[str, Any]]]:
        result = {}
        for config_type in config_types:
            result[config_type] = self.export_service._export_config_type(config_type)
        return result

    def _get_id_field(self, config_type: ConfigType) -> str:
        return self.diff_service._get_id_field(config_type)

    def _item_exists(self, config_type: ConfigType, item_id: str) -> bool:
        table_map = {
            ConfigType.SYNC_TASKS: "sync_task_config",
            ConfigType.ETL_TASKS: "etl_task_config",
            ConfigType.FACTOR_METADATA: "factor_metadata",
            ConfigType.FACTOR_DATA_CONFIG: "factor_data_config",
        }
        table_name = table_map.get(config_type)
        if not table_name:
            return False

        id_field = self._get_id_field(config_type)
        try:
            # 使用简单的查询检查是否存在
            result = self.ddb_client.query(f"select count(*) as cnt from {table_name} where {id_field} = '{item_id}'")
            if result and not result.is_empty():
                return result.to_dicts()[0]['cnt'] > 0
            return False
        except Exception:
            return False

    def _create_item(self, config_type: ConfigType, item: Dict[str, Any]):
        table_map = {
            ConfigType.SYNC_TASKS: "sync_task_config",
            ConfigType.ETL_TASKS: "etl_task_config",
            ConfigType.FACTOR_METADATA: "factor_metadata",
            ConfigType.FACTOR_DATA_CONFIG: "factor_data_config",
        }
        table_name = table_map.get(config_type)
        if not table_name:
            return

        # 添加时间戳
        item = item.copy()
        now = datetime.utcnow()
        item['created_at'] = now
        item['updated_at'] = now

        # 写入数据库
        self.ddb_client.insert(table_name, [item])

    def _update_item(self, config_type: ConfigType, item: Dict[str, Any]):
        table_map = {
            ConfigType.SYNC_TASKS: "sync_task_config",
            ConfigType.ETL_TASKS: "etl_task_config",
            ConfigType.FACTOR_METADATA: "factor_metadata",
            ConfigType.FACTOR_DATA_CONFIG: "factor_data_config",
        }
        table_name = table_map.get(config_type)
        if not table_name:
            return

        id_field = self._get_id_field(config_type)
        item_id = item.get(id_field)
        if not item_id:
            return

        # 先删除旧记录
        self.ddb_client.execute(f"delete from {table_name} where {id_field} = '{item_id}'")

        # 再插入新记录
        self._create_item(config_type, item)

    def _is_item_selected_for_update(
        self,
        config_type: ConfigType,
        item_id: str,
        diffs: Optional[List[ConfigTypeDiff]],
        selections: Optional[Dict[ConfigType, List[str]]]
    ) -> bool:
        if not selections:
            return False
        selected_ids = selections.get(config_type, [])
        return item_id in selected_ids
```

---

### Task 5: 创建配置管理 API 路由

**Files:**
- Create: `app/api/v1/config_api.py`

**Step 1: 创建配置 API 路由文件**

```python
from fastapi import APIRouter, HTTPException
from typing import Dict, List
from ..models.config_import_export import (
    ConfigType,
    ImportMode,
    ExportRequest,
    ExportResponse,
    ImportVerifyRequest,
    ImportVerifyResponse,
    ImportApplyRequest,
    ImportApplyResponse,
)
from ..services.config_export_service import ConfigExportService
from ..services.config_import_service import ConfigImportService

router = APIRouter()
export_service = ConfigExportService()
import_service = ConfigImportService()


@router.post("/config/export", response_model=ExportResponse)
def export_configs(request: ExportRequest):
    """
    导出配置
    """
    try:
        result = export_service.export_configs(request.config_types)
        return ExportResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@router.post("/config/import/verify", response_model=ImportVerifyResponse)
def verify_import(request: ImportVerifyRequest):
    """
    验证导入文件并预览差异
    """
    try:
        valid, errors, diffs, _ = import_service.verify_import(request.content, request.mode)
        return ImportVerifyResponse(
            valid=valid,
            errors=errors,
            diffs=diffs if request.mode == ImportMode.SAFE else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"验证失败: {str(e)}")


@router.post("/config/import/apply", response_model=ImportApplyResponse)
def apply_import(request: ImportApplyRequest):
    """
    执行导入
    """
    try:
        success, summary, errors = import_service.apply_import(
            request.content,
            request.mode,
            request.selections
        )
        return ImportApplyResponse(
            success=success,
            summary=summary,
            errors=errors
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导入失败: {str(e)}")


@router.get("/config/types", response_model=List[Dict[str, str]])
def get_config_types():
    """
    获取可用的配置类型列表
    """
    return [
        {"value": ConfigType.SYNC_TASKS.value, "label": "同步任务配置"},
        {"value": ConfigType.ETL_TASKS.value, "label": "ETL任务配置"},
        {"value": ConfigType.FACTOR_METADATA.value, "label": "因子元数据配置"},
        {"value": ConfigType.FACTOR_DATA_CONFIG.value, "label": "因子数据配置"},
    ]
```

**Step 2: 在 `app/main.py` 中注册新路由**

先读取 `app/main.py`，然后添加：

```python
# 在现有的 router 导入后添加
from app.api.v1.config_api import router as config_router

# 在 app.include_router 部分添加
app.include_router(config_router, prefix="/api/v1")
```

---

### Task 6: 创建前端配置管理页面 - 基础结构

**Files:**
- Create: `frontend/src/pages/ConfigManagement/index.tsx`
- Create: `frontend/src/pages/ConfigManagement/types.ts`
- Create: `frontend/src/pages/ConfigManagement/api.ts`

**Step 1: 创建类型定义文件**

```typescript
export enum ConfigType {
  SYNC_TASKS = 'sync_tasks',
  ETL_TASKS = 'etl_tasks',
  FACTOR_METADATA = 'factor_metadata',
  FACTOR_DATA_CONFIG = 'factor_data_config'
}

export enum ImportMode {
  FAST = 'fast',
  SAFE = 'safe'
}

export interface ConfigTypeOption {
  value: ConfigType;
  label: string;
}

export interface ExportRequest {
  config_types: ConfigType[];
}

export interface ExportResponse {
  filename: string;
  content: string;
}

export interface ConfigItemDiff {
  item_id: string;
  status: 'new' | 'modified' | 'unchanged' | 'deleted';
  current: Record<string, any> | null;
  imported: Record<string, any> | null;
}

export interface ConfigTypeDiff {
  config_type: ConfigType;
  items: ConfigItemDiff[];
  summary: {
    new: number;
    modified: number;
    unchanged: number;
    deleted: number;
  };
}

export interface ImportVerifyRequest {
  content: string;
  mode: ImportMode;
}

export interface ImportVerifyResponse {
  valid: boolean;
  errors: string[];
  diffs: ConfigTypeDiff[] | null;
}

export interface ImportApplyRequest {
  content: string;
  mode: ImportMode;
  selections?: Record<ConfigType, string[]>;
}

export interface ImportResultSummary {
  created: number;
  updated: number;
  skipped: number;
}

export interface ImportApplyResponse {
  success: boolean;
  summary: Record<ConfigType, ImportResultSummary>;
  errors: string[];
}
```

**Step 2: 创建 API 调用文件**

```typescript
import request from '@/utils/request';
import {
  ConfigType,
  ImportMode,
  ConfigTypeOption,
  ExportRequest,
  ExportResponse,
  ImportVerifyRequest,
  ImportVerifyResponse,
  ImportApplyRequest,
  ImportApplyResponse,
} from './types';

export async function getConfigTypes(): Promise<ConfigTypeOption[]> {
  return request.get('/api/v1/config/types');
}

export async function exportConfigs(data: ExportRequest): Promise<ExportResponse> {
  return request.post('/api/v1/config/export', data);
}

export async function verifyImport(data: ImportVerifyRequest): Promise<ImportVerifyResponse> {
  return request.post('/api/v1/config/import/verify', data);
}

export async function applyImport(data: ImportApplyRequest): Promise<ImportApplyResponse> {
  return request.post('/api/v1/config/import/apply', data);
}

// 工具函数：下载文件
export function downloadFile(content: string, filename: string) {
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
export function readFileAsBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
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
```

**Step 3: 创建主页面组件**

```typescript
import React, { useState, useEffect } from 'react';
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
  Spin,
  Divider,
  Typography,
  message,
} from 'antd';
import {
  DownloadOutlined,
  UploadOutlined,
  SettingOutlined,
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
} from './types';
import {
  getConfigTypes,
  exportConfigs,
  verifyImport,
  applyImport,
  downloadFile,
  readFileAsBase64,
} from './api';
import DiffViewer from './DiffViewer';
import ImportResult from './ImportResult';

const { Title, Text } = Typography;
const { Group: CheckboxGroup } = Checkbox;
const { Group: RadioGroup } = Radio;

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
  const [selectedItems, setSelectedItems] = useState<Record<ConfigType, string[]>>({});
  const [importResult, setImportResult] = useState<any>(null);

  useEffect(() => {
    loadConfigTypes();
  }, []);

  const loadConfigTypes = async () => {
    try {
      const data = await getConfigTypes();
      setConfigTypes(data);
      // 默认全选
      setSelectedExportTypes(data.map(t => t.value));
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
      const response = await exportConfigs(request);
      downloadFile(response.content, response.filename);
      message.success('导出成功');
    } catch (error) {
      message.error('导出失败');
    } finally {
      setLoading(false);
    }
  };

  // 导入文件选择
  const uploadProps: UploadProps = {
    beforeUpload: async (file) => {
      if (file.type !== 'application/json' && !file.name.endsWith('.json')) {
        message.error('请上传 JSON 文件');
        return false;
      }

      setImportFile(file);
      setVerifyResult(null);
      setImportResult(null);
      setSelectedItems({});

      try {
        const content = await readFileAsBase64(file);
        setImportContent(content);
      } catch (error) {
        message.error('读取文件失败');
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
      const response = await verifyImport(request);
      setVerifyResult(response);

      // 默认选中所有新增和修改的项
      if (response.diffs) {
        const selections: Record<ConfigType, string[]> = {};
        for (const diff of response.diffs) {
          selections[diff.config_type] = diff.items
            .filter(item => item.status === 'new' || item.status === 'modified')
            .map(item => item.item_id);
        }
        setSelectedItems(selections);
      }

      if (response.valid) {
        message.success('验证通过');
      } else {
        message.error('验证失败');
      }
    } catch (error) {
      message.error('验证失败');
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
      const response = await applyImport(request);
      setImportResult(response);

      if (response.success) {
        message.success('导入成功');
      } else {
        message.error('导入失败');
      }
    } catch (error) {
      message.error('导入失败');
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
    <div style={{ padding: 24 }}>
      <Title level={2}>
        <SettingOutlined /> 配置管理
      </Title>
      <Text type="secondary">
        备份和恢复系统配置，包括同步任务、ETL任务、因子配置等
      </Text>

      <Divider />

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
```

---

### Task 7: 创建前端差异查看组件

**Files:**
- Create: `frontend/src/pages/ConfigManagement/DiffViewer.tsx`
- Create: `frontend/src/pages/ConfigManagement/ImportResult.tsx`

**Step 1: 创建差异查看组件**

```typescript
import React from 'react';
import {
  Collapse,
  Checkbox,
  Tag,
  Table,
  Space,
  Typography,
  Row,
  Col,
} from 'antd';
import {
  ConfigType,
  ConfigTypeDiff,
  ConfigItemDiff,
} from './types';

const { Text } = Typography;
const { Panel } = Collapse;

interface DiffViewerProps {
  diffs: ConfigTypeDiff[];
  selectedItems: Record<ConfigType, string[]>;
  onToggleItem: (configType: ConfigType, itemId: string, checked: boolean) => void;
  onToggleSelectAll: (configType: ConfigType, items: ConfigItemDiff[], checked: boolean) => void;
  getConfigTypeLabel: (value: ConfigType) => string;
}

const statusColors: Record<string, string> = {
  new: 'green',
  modified: 'orange',
  unchanged: 'default',
  deleted: 'red',
};

const statusLabels: Record<string, string> = {
  new: '新增',
  modified: '修改',
  unchanged: '未变化',
  deleted: '删除',
};

const DiffViewer: React.FC<DiffViewerProps> = ({
  diffs,
  selectedItems,
  onToggleItem,
  onToggleSelectAll,
  getConfigTypeLabel,
}) => {
  const getItemColumns = (configType: ConfigType) => [
    {
      title: (
        <Checkbox
          onChange={(e) => {
            const items = diffs.find(d => d.config_type === configType)?.items || [];
            onToggleSelectAll(configType, items, e.target.checked);
          }}
        />
      ),
      key: 'select',
      width: 50,
      render: (_: any, record: ConfigItemDiff) => {
        if (record.status === 'unchanged') {
          return null;
        }
        const selected = selectedItems[configType]?.includes(record.item_id) || false;
        return (
          <Checkbox
            checked={selected}
            onChange={(e) => onToggleItem(configType, record.item_id, e.target.checked)}
          />
        );
      },
    },
    {
      title: '状态',
      key: 'status',
      width: 100,
      render: (_: any, record: ConfigItemDiff) => (
        <Tag color={statusColors[record.status]}>
          {statusLabels[record.status]}
        </Tag>
      ),
    },
    {
      title: 'ID',
      dataIndex: 'item_id',
      key: 'item_id',
    },
  ];

  const renderItemContent = (record: ConfigItemDiff) => {
    if (record.status === 'new') {
      return (
        <div>
          <Text type="secondary">新增配置：</Text>
          <pre style={{ background: '#f6ffed', padding: 8, marginTop: 8 }}>
            {JSON.stringify(record.imported, null, 2)}
          </pre>
        </div>
      );
    }
    if (record.status === 'modified') {
      return (
        <Row gutter={16}>
          <Col span={12}>
            <Text type="secondary">当前：</Text>
            <pre style={{ background: '#fff1f0', padding: 8, marginTop: 8 }}>
              {JSON.stringify(record.current, null, 2)}
            </pre>
          </Col>
          <Col span={12}>
            <Text type="secondary">导入：</Text>
            <pre style={{ background: '#fff7e6', padding: 8, marginTop: 8 }}>
              {JSON.stringify(record.imported, null, 2)}
            </pre>
          </Col>
        </Row>
      );
    }
    if (record.status === 'deleted') {
      return (
        <div>
          <Text type="secondary">将被删除：</Text>
          <pre style={{ background: '#fff1f0', padding: 8, marginTop: 8 }}>
            {JSON.stringify(record.current, null, 2)}
          </pre>
        </div>
      );
    }
    return null;
  };

  return (
    <Collapse defaultActiveKey={diffs.map(d => d.config_type)}>
      {diffs.map((diff) => (
        <Panel
          key={diff.config_type}
          header={
            <Space>
              <span>{getConfigTypeLabel(diff.config_type)}</span>
              {diff.summary.new > 0 && (
                <Tag color="green">+{diff.summary.new} 新增</Tag>
              )}
              {diff.summary.modified > 0 && (
                <Tag color="orange">~{diff.summary.modified} 修改</Tag>
              )}
              {diff.summary.deleted > 0 && (
                <Tag color="red">-{diff.summary.deleted} 删除</Tag>
              )}
              <Tag color="default">{diff.summary.unchanged} 未变化</Tag>
            </Space>
          }
        >
          <Table
            columns={getItemColumns(diff.config_type)}
            dataSource={diff.items}
            rowKey="item_id"
            pagination={false}
            expandable={{
              expandedRowRender: renderItemContent,
              rowExpandable: (record) => record.status !== 'unchanged',
            }}
          />
        </Panel>
      ))}
    </Collapse>
  );
};

export default DiffViewer;
```

**Step 2: 创建导入结果组件**

```typescript
import React from 'react';
import { Card, Table, Tag, Typography, Alert } from 'antd';
import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { ConfigType, ImportApplyResponse } from './types';

const { Title } = Typography;

interface ImportResultProps {
  result: ImportApplyResponse;
  getConfigTypeLabel: (value: ConfigType) => string;
}

const ImportResult: React.FC<ImportResultProps> = ({ result, getConfigTypeLabel }) => {
  const columns = [
    {
      title: '配置类型',
      key: 'config_type',
      render: (_: any, record: { config_type: ConfigType }) => (
        getConfigTypeLabel(record.config_type)
      ),
    },
    {
      title: '新增',
      key: 'created',
      render: (_: any, record: { created: number }) => (
        record.created > 0 ? <Tag color="green">+{record.created}</Tag> : '-'
      ),
    },
    {
      title: '更新',
      key: 'updated',
      render: (_: any, record: { updated: number }) => (
        record.updated > 0 ? <Tag color="orange">~{record.updated}</Tag> : '-'
      ),
    },
    {
      title: '跳过',
      key: 'skipped',
      render: (_: any, record: { skipped: number }) => (
        record.skipped > 0 ? <Tag color="default">{record.skipped}</Tag> : '-'
      ),
    },
  ];

  const data = Object.entries(result.summary).map(([config_type, summary]) => ({
    config_type: config_type as ConfigType,
    ...summary,
  }));

  return (
    <Card title="导入结果">
      {result.success ? (
        <Alert
          icon={<CheckCircleOutlined />}
          message="导入成功"
          type="success"
          showIcon
          style={{ marginBottom: 16 }}
        />
      ) : (
        <Alert
          icon={<CloseCircleOutlined />}
          message="导入失败"
          description={
            <ul>
              {result.errors.map((err, idx) => (
                <li key={idx}>{err}</li>
              ))}
            </ul>
          }
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
        />
      )}

      <Table
        columns={columns}
        dataSource={data}
        rowKey="config_type"
        pagination={false}
      />
    </Card>
  );
};

export default ImportResult;
```

---

### Task 8: 添加前端路由

**Files:**
- Modify: 前端路由配置文件（需要先找到正确的位置）

**Step 1: 先查找前端路由文件**

通常可能的位置：
- `frontend/src/router/index.tsx`
- `frontend/src/App.tsx`
- `frontend/src/routes.ts`

使用 Glob 工具查找。

**Step 2: 添加路由配置**

在找到的路由文件中添加：

```typescript
import ConfigManagement from '@/pages/ConfigManagement';

// 在路由配置中添加
{
  path: '/config-management',
  name: 'config_management',
  element: <ConfigManagement />,
  meta: {
    title: '配置管理',
    icon: 'SettingOutlined',
  },
}
```

---

### Task 9: 导出服务层和创建 __init__.py

**Files:**
- Create/Modify: `app/services/__init__.py`

**Step 1: 读取现有文件并更新**

```python
from .config_export_service import ConfigExportService
from .config_import_service import ConfigImportService
from .config_diff_service import ConfigDiffService

__all__ = [
    # ... 现有导出 ...
    "ConfigExportService",
    "ConfigImportService",
    "ConfigDiffService",
]
```

---

## 测试任务

### Task 10: 测试后端 API

**测试步骤：**
1. 启动后端服务
2. 测试导出 API
3. 测试导入验证 API
4. 测试导入执行 API

### Task 11: 测试前端功能

**测试步骤：**
1. 启动前端服务
2. 测试导出功能
3. 测试导入验证和差异预览
4. 测试导入执行
5. 测试快速模式和安全模式

---

## 总结

这个实施计划包含以下主要任务：

1. **后端模型** - 创建 Pydantic 模型定义数据结构
2. **后端服务** - 创建导出、差异计算、导入三个核心服务
3. **后端 API** - 创建配置管理 API 路由
4. **前端页面** - 创建配置管理主页面
5. **前端组件** - 创建差异查看和结果展示组件
6. **路由配置** - 添加前端路由
7. **测试** - 验证功能完整性

每个任务都有详细的步骤和代码示例，可以直接按顺序执行。
