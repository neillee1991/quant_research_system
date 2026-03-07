/**
 * API Response Types
 */

export interface ApiResponse<T = unknown> {
  success: boolean;
  data: T;
  message?: string;
  error?: string;
}

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  limit: number;
}

export interface ErrorResponse {
  success: false;
  error: string;
  message: string;
  details?: Record<string, unknown>;
}

/**
 * Common Field Types
 */
export interface DateRange {
  start_date?: string;
  end_date?: string;
}

export interface TimeRange {
  start_time?: string;
  end_time?: string;
}

export interface StatusInfo {
  status: 'success' | 'failed' | 'running' | 'pending' | 'queued' | 'done';
  message?: string;
  progress?: number;
  error?: string;
}
