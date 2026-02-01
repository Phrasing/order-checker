import { invoke } from '@tauri-apps/api/core';
import type {
  DashboardData,
  DashboardDataV2,
  PaginatedOrders,
  AggregateStats,
  SyncResult,
  TrackingStatusResponse,
  NewEmailCheck,
} from './types';

export async function getDashboard(
  accountId?: number | null,
  startDate?: string | null,
  endDate?: string | null,
): Promise<DashboardData> {
  const args: Record<string, unknown> = {};
  if (accountId != null) args.accountId = accountId;
  if (startDate != null) args.startDate = startDate;
  if (endDate != null) args.endDate = endDate;
  return invoke<DashboardData>('get_dashboard', args);
}

export async function getDashboardV2(
  accountId?: number | null,
  startDate?: string | null,
  endDate?: string | null,
  statusFilter?: string | null,
  cursor?: string | null,
  limit?: number,
): Promise<DashboardDataV2> {
  const args: Record<string, unknown> = {};
  if (accountId != null) args.accountId = accountId;
  if (startDate != null) args.startDate = startDate;
  if (endDate != null) args.endDate = endDate;
  if (statusFilter != null) args.statusFilter = statusFilter;
  if (cursor != null) args.cursor = cursor;
  if (limit != null) args.limit = limit;
  return invoke<DashboardDataV2>('get_dashboard_v2', args);
}

export async function fetchMoreOrders(
  accountId: number | null,
  startDate: string | null,
  endDate: string | null,
  statusFilter: string | null,
  cursor: string,
  limit?: number,
): Promise<PaginatedOrders> {
  const args: Record<string, unknown> = { cursor };
  if (accountId != null) args.accountId = accountId;
  if (startDate != null) args.startDate = startDate;
  if (endDate != null) args.endDate = endDate;
  if (statusFilter != null) args.statusFilter = statusFilter;
  if (limit != null) args.limit = limit;
  return invoke<PaginatedOrders>('fetch_more_orders', args);
}

export async function searchOrders(
  query: string,
  accountId: number | null,
  startDate: string | null,
  endDate: string | null,
  statusFilter: string | null,
  limit?: number,
): Promise<import('./types').OrderViewModel[]> {
  const args: Record<string, unknown> = { query };
  if (accountId != null) args.accountId = accountId;
  if (startDate != null) args.startDate = startDate;
  if (endDate != null) args.endDate = endDate;
  if (statusFilter != null) args.statusFilter = statusFilter;
  if (limit != null) args.limit = limit;
  return invoke<import('./types').OrderViewModel[]>('search_orders', args);
}

export async function getAggregateStats(
  accountId?: number | null,
  startDate?: string | null,
  endDate?: string | null,
): Promise<AggregateStats> {
  const args: Record<string, unknown> = {};
  if (accountId != null) args.accountId = accountId;
  if (startDate != null) args.startDate = startDate;
  if (endDate != null) args.endDate = endDate;
  return invoke<AggregateStats>('get_aggregate_stats', args);
}

export async function syncAndProcessOrders(
  fetchSince?: string | null,
): Promise<SyncResult> {
  const args: Record<string, unknown> = {};
  if (fetchSince) args.fetchSince = fetchSince;
  return invoke<SyncResult>('sync_and_process_orders', args);
}

export async function checkNewEmails(
  fetchSince?: string | null,
): Promise<NewEmailCheck> {
  const args: Record<string, unknown> = {};
  if (fetchSince) args.fetchSince = fetchSince;
  return invoke<NewEmailCheck>('check_new_emails', args);
}

export async function addAccount(): Promise<string> {
  return invoke<string>('add_account');
}

export async function cancelAddAccount(): Promise<void> {
  return invoke<void>('cancel_add_account');
}

export async function removeAccount(accountId: number): Promise<string> {
  return invoke<string>('remove_account', { accountId });
}

export async function fetchTracking(
  orderId: string,
): Promise<TrackingStatusResponse | null> {
  return invoke<TrackingStatusResponse | null>('fetch_tracking', { orderId });
}

export async function getTrackingStatus(
  orderId: string,
): Promise<TrackingStatusResponse | null> {
  return invoke<TrackingStatusResponse | null>('get_tracking_status', { orderId });
}

export async function getCachedThumbnails(
  imageIds: string[],
): Promise<Record<string, string>> {
  return invoke<Record<string, string>>('get_cached_thumbnails', { imageIds });
}
