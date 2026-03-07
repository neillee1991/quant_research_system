/**
 * Task Service Abstraction Layer
 *
 * Provides unified CRUD operations for all task types (sync, etl, factor)
 * with consistent error handling and type safety.
 */

import { Toast } from '@douyinfe/semi-ui';
import type {
  BaseTaskConfig,
  TaskType,
  TaskListResponse,
  TaskDetailResponse,
  TaskCreateRequest,
  TaskUpdateRequest,
  TaskDeleteResponse,
  SyncTaskConfig,
  ETLTaskConfig,
  FactorConfig,
} from '../types/task';

/**
 * Generic Task Service
 * Handles CRUD operations for any task type with version control support
 */
export class TaskService<T extends BaseTaskConfig> {
  private taskType: TaskType;
  private baseUrl: string;
  private idField: string;

  constructor(taskType: TaskType) {
    this.taskType = taskType;
    this.baseUrl = `/api/v1/${taskType === 'factor' ? 'factors' : taskType}/tasks`;
    this.idField = taskType === 'factor' ? 'factor_id' : 'task_id';
  }

  /**
   * List all tasks (current versions only)
   */
  async listTasks(enabledOnly: boolean = false): Promise<T[]> {
    try {
      const url = enabledOnly ? `${this.baseUrl}?enabled=true` : this.baseUrl;
      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();

      // Handle different response formats
      if (Array.isArray(data)) {
        return data as T[];
      } else if (data.tasks) {
        return data.tasks as T[];
      } else if (data.data) {
        return data.data as T[];
      }

      return [] as T[];
    } catch (error: any) {
      Toast.error(`加载${this.getTaskTypeName()}列表失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get a specific task by ID (current version)
   */
  async getTask(taskId: string): Promise<T> {
    try {
      const response = await fetch(`${this.baseUrl}/${taskId}`);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      return data as T;
    } catch (error: any) {
      Toast.error(`加载${this.getTaskTypeName()}详情失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Create a new task
   */
  async createTask(
    config: Omit<T, 'version_number' | 'is_current' | 'created_at' | 'updated_at'>,
    changedBy: string = 'user',
    changeReason: string = '创建任务'
  ): Promise<T> {
    try {
      const payload = {
        ...config,
        changed_by: changedBy,
        change_reason: changeReason,
      };

      const response = await fetch(this.baseUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || `HTTP ${response.status}`);
      }

      const data = await response.json();
      Toast.success(`${this.getTaskTypeName()}创建成功`);
      return data as T;
    } catch (error: any) {
      Toast.error(`创建${this.getTaskTypeName()}失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Update an existing task (creates new version)
   */
  async updateTask(
    taskId: string,
    updates: Partial<Omit<T, 'version_number' | 'is_current' | 'created_at' | 'updated_at'>>,
    changedBy: string = 'user',
    changeReason: string = '更新任务'
  ): Promise<T> {
    try {
      const payload = {
        ...updates,
        changed_by: changedBy,
        change_reason: changeReason,
      };

      const response = await fetch(`${this.baseUrl}/${taskId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || `HTTP ${response.status}`);
      }

      const data = await response.json();
      Toast.success(`${this.getTaskTypeName()}更新成功`);
      return data as T;
    } catch (error: any) {
      Toast.error(`更新${this.getTaskTypeName()}失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Delete a task (soft delete - marks all versions as deleted)
   */
  async deleteTask(taskId: string): Promise<void> {
    try {
      const response = await fetch(`${this.baseUrl}/${taskId}`, {
        method: 'DELETE',
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || `HTTP ${response.status}`);
      }

      Toast.success(`${this.getTaskTypeName()}删除成功`);
    } catch (error: any) {
      Toast.error(`删除${this.getTaskTypeName()}失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Toggle task enabled status
   */
  async toggleEnabled(taskId: string, enabled: boolean): Promise<T> {
    return this.updateTask(
      taskId,
      { enabled } as Partial<T>,
      'user',
      enabled ? '启用任务' : '禁用任务'
    );
  }

  /**
   * Get version history for a task
   */
  async getVersionHistory(taskId: string): Promise<T[]> {
    try {
      const response = await fetch(`/api/v1/tasks/${this.taskType}/${taskId}/versions`);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      return data.versions || [];
    } catch (error: any) {
      Toast.error(`加载版本历史失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Rollback to a specific version
   */
  async rollbackToVersion(
    taskId: string,
    version: number,
    changedBy: string = 'user',
    changeReason: string = '版本回滚'
  ): Promise<T> {
    try {
      const response = await fetch(
        `/api/v1/tasks/${this.taskType}/${taskId}/rollback/${version}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ changed_by: changedBy, change_reason: changeReason }),
        }
      );

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || `HTTP ${response.status}`);
      }

      const data = await response.json();
      Toast.success(`已回滚到版本 ${version}`);
      return data as T;
    } catch (error: any) {
      Toast.error(`版本回滚失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get task type display name in Chinese
   */
  private getTaskTypeName(): string {
    const names: Record<TaskType, string> = {
      sync: '同步任务',
      etl: 'ETL任务',
      factor: '因子',
    };
    return names[this.taskType];
  }

  /**
   * Get ID field name for this task type
   */
  getIdField(): string {
    return this.idField;
  }
}

// Pre-configured service instances for each task type
export const syncService = new TaskService<SyncTaskConfig>('sync');
export const etlService = new TaskService<ETLTaskConfig>('etl');
export const factorService = new TaskService<FactorConfig>('factor');

// Export default for convenience
export default TaskService;
