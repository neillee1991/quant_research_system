/**
 * Task Service Abstraction Layer
 *
 * Provides unified CRUD operations for all task types (sync, etl, factor)
 * with consistent error handling and type safety.
 */

import { message } from 'antd';
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
 * Handles CRUD operations for any task type
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
   * List all tasks
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
      message.error(`加载${this.getTaskTypeName()}列表失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Get a specific task by ID
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
      message.error(`加载${this.getTaskTypeName()}详情失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Create a new task
   */
  async createTask(
    config: Omit<T, 'created_at' | 'updated_at'>
  ): Promise<T> {
    try {
      const response = await fetch(this.baseUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || `HTTP ${response.status}`);
      }

      const data = await response.json();
      message.success(`${this.getTaskTypeName()}创建成功`);
      return data as T;
    } catch (error: any) {
      message.error(`创建${this.getTaskTypeName()}失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Update an existing task
   */
  async updateTask(
    taskId: string,
    updates: Partial<Omit<T, 'created_at' | 'updated_at'>>
  ): Promise<T> {
    try {
      const response = await fetch(`${this.baseUrl}/${taskId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || `HTTP ${response.status}`);
      }

      const data = await response.json();
      message.success(`${this.getTaskTypeName()}更新成功`);
      return data as T;
    } catch (error: any) {
      message.error(`更新${this.getTaskTypeName()}失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Delete a task
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

      message.success(`${this.getTaskTypeName()}删除成功`);
    } catch (error: any) {
      message.error(`删除${this.getTaskTypeName()}失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * Toggle task enabled status
   */
  async toggleEnabled(taskId: string, enabled: boolean): Promise<T> {
    return this.updateTask(taskId, { enabled } as Partial<T>);
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
