import { currentAccountId } from '../state';
import * as api from '../api';
import { getDateRangeParams } from '../utils';
import { onTabActivate } from './tabs';
import type { UpcomingDelivery } from '../types';

let initialized = false;

export function setupAnalyticsTab(): void {
  onTabActivate('analytics', loadAnalytics);
}

function formatEstimatedDate(isoDate: string): string {
  try {
    // Parse ISO date like "2026-02-05T00:00:00-05:00"
    const date = new Date(isoDate);
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const tomorrow = new Date(today);
    tomorrow.setDate(tomorrow.getDate() + 1);

    const targetDate = new Date(date);
    targetDate.setHours(0, 0, 0, 0);

    // Check if it's today or tomorrow
    if (targetDate.getTime() === today.getTime()) {
      return 'Today';
    } else if (targetDate.getTime() === tomorrow.getTime()) {
      return 'Tomorrow';
    }

    // Otherwise return formatted date
    return date.toLocaleDateString('en-US', {
      weekday: 'short',
      month: 'short',
      day: 'numeric',
    });
  } catch {
    return isoDate.substring(0, 10);
  }
}

interface DeliveryGroup {
  date: string;
  dateLabel: string;
  isToday: boolean;
  isTomorrow: boolean;
  deliveries: UpcomingDelivery[];
}

function groupDeliveriesByDate(deliveries: UpcomingDelivery[]): DeliveryGroup[] {
  const groups = new Map<string, DeliveryGroup>();

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const tomorrow = new Date(today);
  tomorrow.setDate(tomorrow.getDate() + 1);

  for (const delivery of deliveries) {
    // Extract date portion (YYYY-MM-DD) from ISO string
    const dateKey = delivery.estimated_delivery.substring(0, 10);

    if (!groups.has(dateKey)) {
      const targetDate = new Date(dateKey + 'T00:00:00');
      const isToday = targetDate.getTime() === today.getTime();
      const isTomorrow = targetDate.getTime() === tomorrow.getTime();

      let dateLabel: string;
      if (isToday) {
        dateLabel = 'Today';
      } else if (isTomorrow) {
        dateLabel = 'Tomorrow';
      } else {
        dateLabel = targetDate.toLocaleDateString('en-US', {
          weekday: 'short',
          month: 'short',
          day: 'numeric',
        });
      }

      groups.set(dateKey, {
        date: dateKey,
        dateLabel,
        isToday,
        isTomorrow,
        deliveries: [],
      });
    }

    groups.get(dateKey)!.deliveries.push(delivery);
  }

  // Sort by date (earliest first)
  return Array.from(groups.values()).sort((grpA, grpB) => grpA.date.localeCompare(grpB.date));
}

function renderUpcomingDeliveries(deliveries: UpcomingDelivery[]): string {
  if (deliveries.length === 0) {
    return `
      <div class="upcoming-empty">
        No upcoming deliveries with estimated dates
      </div>
    `;
  }

  const groups = groupDeliveriesByDate(deliveries);

  return `
    <div class="upcoming-groups">
      ${groups.map(group => `
        <div class="upcoming-group ${group.isToday ? 'upcoming-group-today' : ''} ${group.isTomorrow ? 'upcoming-group-tomorrow' : ''}">
          <div class="upcoming-group-header">
            <span class="upcoming-group-date">${group.dateLabel}</span>
            <span class="upcoming-group-count">${group.deliveries.length} package${group.deliveries.length !== 1 ? 's' : ''}</span>
          </div>
          <div class="upcoming-group-items">
            ${group.deliveries.map(delivery => `
              <div class="upcoming-item-compact" data-order-id="${delivery.order_id}">
                <span class="upcoming-item-name">${delivery.item_name || 'Order'}</span>
                <span class="upcoming-item-carrier">${delivery.carrier}</span>
              </div>
            `).join('')}
          </div>
        </div>
      `).join('')}
    </div>
  `;
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

    // Fetch stats and upcoming deliveries in parallel
    const [stats, upcomingDeliveries] = await Promise.all([
      api.getAggregateStats(currentAccountId, startDate, endDate),
      api.getUpcomingDeliveries(currentAccountId),
    ]);

    const trk = stats.tracking_breakdown;

    // Calculate total shipped for percentage display
    const totalShipped = trk.label_created + trk.in_transit + trk.out_for_delivery +
                         trk.delivered + trk.exception + trk.available_for_pickup + trk.unknown;

    container.innerHTML = `
      <div class="analytics-inner">
        <div class="analytics-header">
          <h2 class="analytics-title">Analytics</h2>
        </div>

        <div class="analytics-section">
          <h3 class="analytics-section-title">Overview</h3>
          <div class="analytics-grid analytics-grid-3">
            <div class="analytics-card">
              <div class="analytics-card-label">Total Orders</div>
              <div class="analytics-card-value">${stats.total_orders.toLocaleString()}</div>
            </div>
            <div class="analytics-card">
              <div class="analytics-card-label">Total Spent</div>
              <div class="analytics-card-value analytics-card-money">$${stats.total_spent.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
            </div>
            <div class="analytics-card">
              <div class="analytics-card-label">Total Items</div>
              <div class="analytics-card-value">${stats.total_quantity.toLocaleString()}</div>
            </div>
          </div>
        </div>

        ${upcomingDeliveries.length > 0 ? `
        <div class="analytics-section">
          <h3 class="analytics-section-title">Upcoming Deliveries</h3>
          <p class="analytics-section-subtitle">${upcomingDeliveries.length} package${upcomingDeliveries.length !== 1 ? 's' : ''} expected</p>
          ${renderUpcomingDeliveries(upcomingDeliveries)}
        </div>
        ` : ''}

        <div class="analytics-section">
          <h3 class="analytics-section-title">Shipping Status</h3>
          <p class="analytics-section-subtitle">${totalShipped} shipped order${totalShipped !== 1 ? 's' : ''}</p>
          <div class="analytics-grid analytics-grid-3">
            <div class="analytics-card tracking-card tracking-label-created">
              <div class="analytics-card-label">Label Created</div>
              <div class="analytics-card-value">${trk.label_created}</div>
            </div>
            <div class="analytics-card tracking-card tracking-in-transit">
              <div class="analytics-card-label">In Transit</div>
              <div class="analytics-card-value">${trk.in_transit}</div>
            </div>
            <div class="analytics-card tracking-card tracking-out-for-delivery">
              <div class="analytics-card-label">Out for Delivery</div>
              <div class="analytics-card-value">${trk.out_for_delivery}</div>
            </div>
            <div class="analytics-card tracking-card tracking-delivered">
              <div class="analytics-card-label">Delivered</div>
              <div class="analytics-card-value">${trk.delivered}</div>
            </div>
            <div class="analytics-card tracking-card tracking-exception">
              <div class="analytics-card-label">Exception</div>
              <div class="analytics-card-value">${trk.exception}</div>
            </div>
            <div class="analytics-card tracking-card tracking-pickup">
              <div class="analytics-card-label">Available for Pickup</div>
              <div class="analytics-card-value">${trk.available_for_pickup}</div>
            </div>
            ${trk.unknown > 0 ? `
            <div class="analytics-card tracking-card tracking-unknown">
              <div class="analytics-card-label">Unknown</div>
              <div class="analytics-card-value">${trk.unknown}</div>
            </div>
            ` : ''}
          </div>
        </div>
      </div>
    `;
  } catch (error) {
    console.error('Failed to load analytics:', error);
    container.innerHTML = `
      <div class="analytics-inner">
        <div class="analytics-header">
          <h2 class="analytics-title">Analytics</h2>
        </div>
        <div class="analytics-error">Failed to load statistics. Try again later.</div>
      </div>
    `;
  }
}
