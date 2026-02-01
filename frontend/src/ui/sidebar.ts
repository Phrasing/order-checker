import type { DashboardData, DashboardDataV2, StatusFilter } from '../types';
import {
  currentFilter,
  currentSearchQuery,
  lastUpdatedText,
  fetchSinceDate,
  currentAccountId,
  setCurrentFilter,
  setCurrentDatePreset,
  setCurrentSearchQuery,
  setFetchSinceDate,
  setLastUpdatedText,
  filterTitles,
} from '../state';
import * as api from '../api';
import { getDateRangeParams } from '../utils';

// These are imported lazily to avoid circular dependency issues at module evaluation time.
// Both loadDashboard and applyFiltersAndRender are only called from event handlers (not at import time).
let _loadDashboard: (() => Promise<void>) | null = null;
let _applyFiltersAndRenderDebounced: ((delay?: number) => void) | null = null;
let _checkForNewEmails: (() => Promise<void>) | null = null;

export function bindSidebarDeps(deps: {
  loadDashboard: () => Promise<void>;
  applyFiltersAndRender: () => void;
  applyFiltersAndRenderDebounced: (delay?: number) => void;
  checkForNewEmails: () => Promise<void>;
}): void {
  _loadDashboard = deps.loadDashboard;
  _applyFiltersAndRenderDebounced = deps.applyFiltersAndRenderDebounced;
  _checkForNewEmails = deps.checkForNewEmails;
}

export function updateLastUpdated(text: string): void {
  setLastUpdatedText(text);
  const el = document.getElementById('last-updated');
  if (el) el.textContent = text;
}

export function renderSidebar(data: DashboardData | DashboardDataV2): void {
  const el = (id: string) => document.getElementById(id);

  // Calculate total from status counts (sum of all statuses)
  // This ensures TOTAL ORDERS always shows all orders, not filtered count
  const totalOrders = (data.status_counts?.confirmed || 0) +
                     (data.status_counts?.shipped || 0) +
                     (data.status_counts?.delivered || 0) +
                     (data.status_counts?.canceled || 0) +
                     (data.status_counts?.partially_canceled || 0);

  el('total-count')!.textContent = String(totalOrders);
  el('count-all')!.textContent = String(totalOrders);
  el('count-confirmed')!.textContent = String(data.status_counts?.confirmed || 0);
  el('count-shipped')!.textContent = String(data.status_counts?.shipped || 0);
  el('count-delivered')!.textContent = String(data.status_counts?.delivered || 0);
  el('count-canceled')!.textContent = String(data.status_counts?.canceled || 0);
  el('pending-emails')!.textContent = `${data.pending_emails || 0} pending emails`;
  setLastUpdatedText(data.last_updated || '');
  el('last-updated')!.textContent = lastUpdatedText;
}

export async function renderStats(): Promise<void> {
  try {
    const { startDate, endDate } = getDateRangeParams();
    const stats = await api.getAggregateStats(
      currentAccountId,
      startDate,
      endDate
    );

    document.getElementById('stat-total-spent')!.textContent = '$' + stats.total_spent.toFixed(2);
    document.getElementById('stat-total-qty')!.textContent = stats.total_quantity.toLocaleString();
    document.getElementById('stat-avg-order')!.textContent = '$' + stats.avg_order.toFixed(2);
    document.getElementById('stat-this-week')!.textContent = stats.orders_this_week + ' orders';
  } catch (error) {
    console.error('Failed to load aggregate stats:', error);
    // Show error state in stats
    document.getElementById('stat-total-spent')!.textContent = '-';
    document.getElementById('stat-total-qty')!.textContent = '-';
    document.getElementById('stat-avg-order')!.textContent = '-';
    document.getElementById('stat-this-week')!.textContent = '-';
  }
}

export function updateHeader(): void {
  document.getElementById('filter-title')!.textContent = filterTitles[currentFilter] || 'All Orders';
}

export function updateResultsCount(shown: number, total: number): void {
  const subtitle = document.getElementById('last-updated')!;
  if (!currentSearchQuery) {
    subtitle.textContent = lastUpdatedText;
    return;
  }
  const filterLabel = currentFilter === 'all' ? 'orders' : `${currentFilter} orders`;
  if (shown === 0) {
    subtitle.textContent = `No ${filterLabel} match "${currentSearchQuery}"`;
  } else {
    subtitle.textContent = `${shown} of ${total} ${filterLabel} match "${currentSearchQuery}"`;
  }
}

export function setupFilterListeners(): void {
  document.querySelectorAll<HTMLElement>('.filter-item').forEach(item => {
    item.addEventListener('click', async () => {
      document.querySelectorAll('.filter-item').forEach(i => i.classList.remove('active'));
      item.classList.add('active');
      setCurrentFilter((item.dataset.filter || 'all') as StatusFilter);
      updateHeader();
      // Reload dashboard with new filter (backend filtering)
      await _loadDashboard?.();
    });
  });

  document.getElementById('search-input')?.addEventListener('input', (e) => {
    setCurrentSearchQuery((e.target as HTMLInputElement).value.trim());
    _applyFiltersAndRenderDebounced?.(200); // Debounce search to avoid lag during typing
  });
}

export function setupDatePresetListeners(): void {
  document.querySelectorAll<HTMLButtonElement>('.date-preset').forEach(btn => {
    btn.addEventListener('click', async () => {
      document.querySelectorAll('.date-preset').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      setCurrentDatePreset(btn.dataset.days || '0');
      await _loadDashboard?.();
    });
  });
}

export function setupFetchSincePicker(): void {
  const input = document.getElementById('fetch-since-date') as HTMLInputElement | null;
  const hint = document.getElementById('fetch-since-hint');
  if (!input || !hint) return;

  if (fetchSinceDate) {
    input.value = fetchSinceDate;
    updateFetchSinceHint(hint, fetchSinceDate);
  }

  input.max = new Date().toISOString().split('T')[0];

  input.addEventListener('change', () => {
    const val = input.value;
    if (val) {
      setFetchSinceDate(val);
      localStorage.setItem('fetchSinceDate', val);
    } else {
      setFetchSinceDate(null);
      localStorage.removeItem('fetchSinceDate');
    }
    updateFetchSinceHint(hint, val || null);
    _checkForNewEmails?.();
  });
}

function updateFetchSinceHint(hint: HTMLElement, dateStr: string | null): void {
  if (!dateStr) {
    hint.textContent = 'Default: last 5 days';
    hint.className = 'fetch-since-hint';
    return;
  }
  const selected = new Date(dateStr + 'T00:00:00');
  const now = new Date();
  const diffDays = Math.round((now.getTime() - selected.getTime()) / (1000 * 60 * 60 * 24));

  if (diffDays > 365) {
    hint.textContent = `${diffDays} days ago \u2014 this may take a while`;
    hint.className = 'fetch-since-hint fetch-since-warn';
  } else if (diffDays > 90) {
    hint.textContent = `${diffDays} days ago \u2014 larger sync`;
    hint.className = 'fetch-since-hint fetch-since-warn';
  } else {
    hint.textContent = `${diffDays} days of emails`;
    hint.className = 'fetch-since-hint';
  }
}
