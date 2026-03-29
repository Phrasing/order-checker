import type { OrderViewModel, DateRangeParams, SortDirection } from './types';
import { currentDatePreset, statusPriority } from './state';

// HTML escaping

export function escapeHtml(text: unknown): string {
  if (text === null || text === undefined) return '';
  const div = document.createElement('div');
  div.textContent = String(text);
  return div.innerHTML;
}

// Date helpers

export function displayDate(order: OrderViewModel): string {
  return order.order_date;
}

export function getDateRangeParams(): DateRangeParams {
  if (currentDatePreset === '0') {
    return { startDate: null, endDate: null };
  }
  const days = parseInt(currentDatePreset, 10);
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - days);
  return {
    startDate: start.toISOString().split('T')[0],
    endDate: end.toISOString().split('T')[0],
  };
}

export function parseEventTime(timeStr: string | null): Date | null {
  if (!timeStr) return null;
  const date = new Date(timeStr);
  return isNaN(date.getTime()) ? null : date;
}

export function formatDateHeader(date: Date): string {
  return date.toLocaleDateString('en-US', {
    weekday: 'long',
    month: 'numeric',
    day: 'numeric',
    year: '2-digit',
  });
}

export function formatTime(date: Date): string {
  return date.toLocaleTimeString('en-US', {
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
  });
}

// Product summary

const stripPrefixes = [
  'Pokemon Trading Card Game ',
  'Pok\u00e9mon Trading Card Game ',
  'Pokemon TCG ',
  'Pok\u00e9mon TCG ',
  'Mega Evolution 2 5 ',
  'Scarlet & Violet ',
  'Scarlet and Violet ',
  'Sword & Shield ',
  'Sword and Shield ',
  'Sun & Moon ',
  'Sun and Moon ',
];

const stripSuffixes = [
  ' Randomly Selected',
  ' Randomly selected',
  ' - Randomly Selected',
];

export function getProductSummary(order: OrderViewModel): string {
  if (!order.items || order.items.length === 0) return 'No items';
  let name = order.items[0].name;

  let changed = true;
  while (changed) {
    changed = false;
    for (const prefix of stripPrefixes) {
      if (name.toLowerCase().startsWith(prefix.toLowerCase())) {
        name = name.substring(prefix.length);
        changed = true;
        break;
      }
    }
  }

  changed = true;
  while (changed) {
    changed = false;
    for (const suffix of stripSuffixes) {
      if (name.toLowerCase().endsWith(suffix.toLowerCase())) {
        name = name.substring(0, name.length - suffix.length);
        changed = true;
        break;
      }
    }
  }

  const truncated = name.length > 70 ? name.substring(0, 70) + '...' : name;
  const itemSuffix = order.items.length > 1 ? ` +${order.items.length - 1} more` : '';
  return truncated + itemSuffix;
}

// Filtering & sorting

export function filterOrders(orders: OrderViewModel[], filter: string): OrderViewModel[] {
  if (filter === 'all') return orders;
  return orders.filter((o) => o.status === filter);
}

export function searchOrders(orders: OrderViewModel[], query: string): OrderViewModel[] {
  if (!query) return orders;
  const q = query.toLowerCase();
  return orders.filter(
    (order) =>
      order.id.toLowerCase().includes(q) ||
      order.status.toLowerCase().includes(q) ||
      (order.total_cost && order.total_cost.includes(q)) ||
      (order.recipient && order.recipient.toLowerCase().includes(q)) ||
      (order.items || []).some((item) => item.name.toLowerCase().includes(q)),
  );
}

export function sortOrders(
  orders: OrderViewModel[],
  sortBy: string,
  direction: SortDirection = 'desc',
): OrderViewModel[] {
  const dir = direction === 'asc' ? 1 : -1;

  return [...orders].sort((a, b) => {
    let cmp = 0;

    switch (sortBy) {
      case 'date':
        cmp = new Date(a.order_date_raw).getTime() - new Date(b.order_date_raw).getTime();
        break;

      case 'price': {
        const priceA = parseFloat(a.total_cost || '0');
        const priceB = parseFloat(b.total_cost || '0');
        cmp = priceA - priceB;
        break;
      }

      case 'status': {
        const pA = statusPriority[a.status] ?? 99;
        const pB = statusPriority[b.status] ?? 99;
        cmp = pA - pB;
        break;
      }

      case 'quantity': {
        const qA = a.items?.reduce((s, i) => s + (i.quantity || 1), 0) || 0;
        const qB = b.items?.reduce((s, i) => s + (i.quantity || 1), 0) || 0;
        cmp = qA - qB;
        break;
      }

      case 'items': {
        cmp = (a.items?.length || 0) - (b.items?.length || 0);
        break;
      }
    }

    // Primary sort with direction, secondary sort by date desc for ties
    if (cmp !== 0) return cmp * dir;
    return new Date(b.order_date_raw).getTime() - new Date(a.order_date_raw).getTime();
  });
}
