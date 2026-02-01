import { currentAccountId } from '../state';
import * as api from '../api';
import { getDateRangeParams } from '../utils';
import { onTabActivate } from './tabs';

let initialized = false;

export function setupAnalyticsTab(): void {
  onTabActivate('analytics', loadAnalytics);
}

async function loadAnalytics(): Promise<void> {
  const container = document.getElementById('analytics-content');
  if (!container) return;

  // Show loading state on first visit
  if (!initialized) {
    container.innerHTML = '<div class="analytics-loading">Loading statistics...</div>';
    initialized = true;
  }

  try {
    const { startDate, endDate } = getDateRangeParams();
    const stats = await api.getAggregateStats(currentAccountId, startDate, endDate);

    container.innerHTML = `
      <div class="analytics-header">
        <h2 class="analytics-title">Statistics Overview</h2>
      </div>
      <div class="analytics-grid">
        <div class="analytics-card">
          <div class="analytics-card-label">Total Orders</div>
          <div class="analytics-card-value">${stats.total_orders.toLocaleString()}</div>
        </div>
        <div class="analytics-card">
          <div class="analytics-card-label">Total Spent</div>
          <div class="analytics-card-value analytics-card-money">$${stats.total_spent.toFixed(2)}</div>
        </div>
        <div class="analytics-card">
          <div class="analytics-card-label">Average Order</div>
          <div class="analytics-card-value analytics-card-money">$${stats.avg_order.toFixed(2)}</div>
        </div>
        <div class="analytics-card">
          <div class="analytics-card-label">Total Items</div>
          <div class="analytics-card-value">${stats.total_quantity.toLocaleString()}</div>
        </div>
        <div class="analytics-card">
          <div class="analytics-card-label">Orders This Week</div>
          <div class="analytics-card-value">${stats.orders_this_week}</div>
        </div>
        <div class="analytics-card">
          <div class="analytics-card-label">Avg Items/Order</div>
          <div class="analytics-card-value">${stats.total_orders > 0 ? (stats.total_quantity / stats.total_orders).toFixed(1) : '0'}</div>
        </div>
      </div>
    `;
  } catch (error) {
    console.error('Failed to load analytics:', error);
    container.innerHTML = `
      <div class="analytics-header">
        <h2 class="analytics-title">Statistics Overview</h2>
      </div>
      <div class="analytics-error">Failed to load statistics. Try again later.</div>
    `;
  }
}
