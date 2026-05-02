/**
 * Task Service Tests
 *
 * Basic tests to verify the task service abstraction layer works correctly.
 */

import { TaskService } from '../services/taskService';
import type { SyncTaskConfig, ETLTaskConfig, FactorConfig } from '../types/task';

// Mock fetch for testing
global.fetch = jest.fn();

describe('TaskService', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('SyncTaskService', () => {
    const syncService = new TaskService<SyncTaskConfig>('sync');

    it('should list tasks', async () => {
      const mockTasks = [
        {
          task_id: 'daily_basic',
          description: 'Daily basic data',
          api_name: 'daily',
          enabled: true,
        },
      ];

      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        json: async () => mockTasks,
      });

      const tasks = await syncService.listTasks();
      expect(tasks).toEqual(mockTasks);
      expect(global.fetch).toHaveBeenCalledWith('/api/v1/sync/tasks');
    });

    it('should get a specific task', async () => {
      const mockTask = {
        task_id: 'daily_basic',
        description: 'Daily basic data',
        api_name: 'daily',
        enabled: true,
      };

      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        json: async () => mockTask,
      });

      const task = await syncService.getTask('daily_basic');
      expect(task).toEqual(mockTask);
      expect(global.fetch).toHaveBeenCalledWith('/api/v1/sync/tasks/daily_basic');
    });

    it('should create a task', async () => {
      const newTask = {
        task_id: 'new_task',
        description: 'New task',
        api_name: 'test',
        enabled: true,
      };

      const mockResponse = {
        ...newTask,
      };

      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      });

      const task = await syncService.createTask(newTask as any);
      expect(task).toEqual(mockResponse);
      expect(global.fetch).toHaveBeenCalledWith(
        '/api/v1/sync/tasks',
        expect.objectContaining({
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        })
      );
    });

    it('should update a task', async () => {
      const updates = { description: 'Updated description' };
      const mockResponse = {
        task_id: 'daily_basic',
        description: 'Updated description',
        api_name: 'daily',
        enabled: true,
      };

      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      });

      const task = await syncService.updateTask('daily_basic', updates);
      expect(task).toEqual(mockResponse);
      expect(global.fetch).toHaveBeenCalledWith(
        '/api/v1/sync/tasks/daily_basic',
        expect.objectContaining({
          method: 'PUT',
        })
      );
    });

    it('should delete a task', async () => {
      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        json: async () => ({ success: true }),
      });

      await syncService.deleteTask('daily_basic');
      expect(global.fetch).toHaveBeenCalledWith(
        '/api/v1/sync/tasks/daily_basic',
        expect.objectContaining({
          method: 'DELETE',
        })
      );
    });

    it('should toggle enabled status', async () => {
      const mockResponse = {
        task_id: 'daily_basic',
        description: 'Daily basic data',
        api_name: 'daily',
        enabled: false,
      };

      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        json: async () => mockResponse,
      });

      const task = await syncService.toggleEnabled('daily_basic', false);
      expect(task.enabled).toBe(false);
    });

    it('should use correct ID field', () => {
      expect(syncService.getIdField()).toBe('task_id');
    });
  });

  describe('ETLTaskService', () => {
    const etlService = new TaskService<ETLTaskConfig>('etl');

    it('should use correct base URL', async () => {
      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        json: async () => [],
      });

      await etlService.listTasks();
      expect(global.fetch).toHaveBeenCalledWith('/api/v1/etl/tasks');
    });

    it('should use correct ID field', () => {
      expect(etlService.getIdField()).toBe('task_id');
    });
  });

  describe('FactorService', () => {
    const factorService = new TaskService<FactorConfig>('factor');

    it('should use correct base URL', async () => {
      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        json: async () => [],
      });

      await factorService.listTasks();
      expect(global.fetch).toHaveBeenCalledWith('/api/v1/factors/tasks');
    });

    it('should use correct ID field', () => {
      expect(factorService.getIdField()).toBe('factor_id');
    });
  });

  describe('Error Handling', () => {
    const syncService = new TaskService<SyncTaskConfig>('sync');

    it('should handle HTTP errors', async () => {
      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: 'Not Found',
      });

      await expect(syncService.listTasks()).rejects.toThrow('HTTP 404: Not Found');
    });

    it('should handle network errors', async () => {
      (global.fetch as jest.Mock).mockRejectedValueOnce(new Error('Network error'));

      await expect(syncService.listTasks()).rejects.toThrow('Network error');
    });
  });
});
