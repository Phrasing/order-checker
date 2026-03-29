import type { OrderViewModel, TrackingStatusResponse, TimelineEvent } from '../types';
import { trackingCache, trackingStateColors, trackingUrls } from '../state';
import { escapeHtml, parseEventTime, formatDateHeader, formatTime } from '../utils';
import * as api from '../api';

export function renderTrackingSection(order: OrderViewModel): string {
  const trackingLink = renderTrackingLink(order.tracking_number!, order.carrier);
  const cachedStatus = trackingCache.get(order.id);

  let statusHtml = '';
  if (cachedStatus) {
    statusHtml = renderTrackingStatus(cachedStatus);
  }

  return `
    ${trackingLink}
    <div class="tracking-status-container" id="tracking-status-${order.id}">
      ${statusHtml}
    </div>
    <button class="fetch-tracking-btn" data-order-id="${order.id}">
      ${cachedStatus ? 'Refresh Status' : 'Get Status'}
    </button>
  `;
}

export function renderTrackingStatus(status: TrackingStatusResponse): string {
  const badgeClass = trackingStateColors[status.state] || 'badge-gray';
  const eventsByDate = groupEventsByDate(status.events);

  let timelineHtml = '';
  if (Object.keys(eventsByDate).length > 0) {
    timelineHtml = `<div class="tracking-timeline">
      ${Object.entries(eventsByDate).map(([date, events]) => `
        <div class="timeline-date-group">
          <div class="timeline-date">${date}</div>
          ${(events as TimelineEvent[]).map(e => `
            <div class="timeline-event">
              <span class="timeline-time">${e.time}</span>
              <span class="timeline-desc">${escapeHtml(e.description)}</span>
              ${e.location ? `<span class="timeline-loc">${escapeHtml(e.location)}</span>` : ''}
            </div>
          `).join('')}
        </div>
      `).join('')}
    </div>`;
  }

  return `
    <div class="tracking-status">
      <span class="tracking-badge ${badgeClass}">${escapeHtml(status.state_display)}</span>
      ${status.state_description ? `<span class="tracking-desc">${escapeHtml(status.state_description)}</span>` : ''}
    </div>
    ${timelineHtml}
    <div class="tracking-fetched">Last updated: ${escapeHtml(status.last_fetched_at)}</div>
  `;
}

function groupEventsByDate(events: TrackingStatusResponse['events']): Record<string, TimelineEvent[]> {
  const groups: Record<string, TimelineEvent[]> = {};
  for (const e of events || []) {
    const dateTime = parseEventTime(e.time ?? null);
    const dateKey = dateTime ? formatDateHeader(dateTime) : 'Unknown Date';
    const timeKey = dateTime ? formatTime(dateTime) : '';

    if (!groups[dateKey]) groups[dateKey] = [];
    groups[dateKey].push({
      time: timeKey,
      description: e.description,
      location: e.location,
    });
  }
  return groups;
}

function renderTrackingLink(tracking: string, carrier: string | null): string {
  const carrierLower = (carrier || '').toLowerCase();
  const urlFn = trackingUrls[carrierLower];

  if (urlFn) {
    return `
      <div>
        <span style="color: var(--text-secondary); font-size: 11px; text-transform: uppercase;">${escapeHtml(carrier)}</span><br>
        <a href="${urlFn(tracking)}" target="_blank" rel="noopener noreferrer" class="detail-link">${escapeHtml(tracking)}</a>
      </div>
    `;
  }
  return `<span>${escapeHtml(tracking)}</span>`;
}

export async function handleFetchTrackingClick(orderId: string, btn: HTMLButtonElement): Promise<void> {
  btn.classList.add('loading');
  btn.disabled = true;

  try {
    const status = await api.fetchTracking(orderId);
    if (status) {
      trackingCache.set(orderId, status);
      const container = document.getElementById(`tracking-status-${orderId}`);
      if (container) {
        container.innerHTML = renderTrackingStatus(status);
      }
      btn.textContent = 'Refresh Status';
    } else {
      btn.textContent = 'No tracking info';
    }
  } catch (error) {
    console.error('Failed to fetch tracking:', error);
    btn.textContent = 'Error';
  } finally {
    btn.classList.remove('loading');
    btn.disabled = false;
  }
}

export async function loadCachedTrackingStatus(orderId: string): Promise<void> {
  try {
    const status = await api.getTrackingStatus(orderId);
    if (status) {
      trackingCache.set(orderId, status);
      const container = document.getElementById(`tracking-status-${orderId}`);
      if (container) {
        container.innerHTML = renderTrackingStatus(status);
      }
      const btn = container?.parentElement?.querySelector<HTMLButtonElement>('.fetch-tracking-btn');
      if (btn) {
        btn.textContent = 'Refresh Status';
      }
    }
  } catch (error) {
    console.error('Failed to load cached tracking:', error);
  }
}
