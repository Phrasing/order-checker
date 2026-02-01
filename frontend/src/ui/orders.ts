import type { OrderViewModel } from '../types';
import {
  allOrders,
  allAccounts,
  currentFilter,
  currentSort,
  currentAccountId,
  currentSearchQuery,
  currentRenderedOrders,
  expandedOrderId,
  trackingCache,
  thumbnailCache,
  orderThumbObserver,
  statusLabels,
  loadedOrders,
  hasMoreOrders,
  nextCursor,
  isLoadingMore,
  virtualScrollState,
  productSummaryCache,
  setAllOrders,
  setAllAccounts,
  setCurrentRenderedOrders,
  setExpandedOrderId,
  setOrderThumbObserver,
  setLoadedOrders,
  appendLoadedOrders,
  setHasMoreOrders,
  setNextCursor,
  setIsLoadingMore,
  setVirtualScrollState,
} from '../state';
import {
  escapeHtml,
  displayDate,
  getDateRangeParams,
  getProductSummary,
  sortOrders,
} from '../utils';
import * as api from '../api';
import { renderSidebar, updateHeader, updateResultsCount } from './sidebar';
import { renderTrackingSection, loadCachedTrackingStatus } from './tracking';
import { renderAccountSelector } from './accounts';

export async function loadDashboard(): Promise<void> {
  try {
    const { startDate, endDate } = getDateRangeParams();
    const args: { accountId?: number; startDate?: string; endDate?: string } = {};
    if (currentAccountId !== null) args.accountId = currentAccountId;
    if (startDate !== null) args.startDate = startDate;
    if (endDate !== null) args.endDate = endDate;
    console.log('Invoking get_dashboard_v2 with args:', args);

    const data = await api.getDashboardV2(
      args.accountId ?? null,
      args.startDate ?? null,
      args.endDate ?? null,
      currentFilter === 'all' ? null : currentFilter, // status filter
      null, // cursor
      100   // limit - initial page size
    );

    // Set pagination state
    setLoadedOrders(data.paginated_orders.orders || []);
    setHasMoreOrders(data.paginated_orders.has_more);
    setNextCursor(data.paginated_orders.next_cursor);

    // Keep allOrders for backward compatibility (will be replaced by loaded orders for filtering)
    setAllOrders(data.paginated_orders.orders || []);
    setAllAccounts(data.accounts || []);

    renderSidebar(data);
    renderAccountSelector(allAccounts);
    applyFiltersAndRender();
    updateHeader();
    setupInfiniteScroll();
  } catch (error) {
    console.error('Failed to load dashboard:', error);
    const list = document.getElementById('order-list');
    if (list) {
      list.innerHTML = `
        <div class="empty-state">
          <p>Failed to load orders</p>
          <p style="font-size: 12px; margin-top: 8px;">${escapeHtml(String(error))}</p>
        </div>
      `;
    }
  }
}

let infiniteScrollObserver: IntersectionObserver | null = null;

function setupInfiniteScroll(): void {
  if (infiniteScrollObserver) {
    infiniteScrollObserver.disconnect();
  }

  const sentinel = document.getElementById('scroll-sentinel');
  if (!sentinel) return;

  infiniteScrollObserver = new IntersectionObserver(
    async (entries) => {
      const entry = entries[0];
      if (entry.isIntersecting && hasMoreOrders && !isLoadingMore) {
        await loadMoreOrders();
      }
    },
    { rootMargin: '400px 0px' } // Trigger 400px before reaching bottom
  );

  infiniteScrollObserver.observe(sentinel);
}

async function loadMoreOrders(): Promise<void> {
  if (!hasMoreOrders || isLoadingMore || !nextCursor) return;

  setIsLoadingMore(true);
  try {
    const { startDate, endDate } = getDateRangeParams();
    const data = await api.fetchMoreOrders(
      currentAccountId,
      startDate,
      endDate,
      currentFilter === 'all' ? null : currentFilter,
      nextCursor,
      100 // page size
    );

    appendLoadedOrders(data.orders);
    setHasMoreOrders(data.has_more);
    setNextCursor(data.next_cursor);

    // Update allOrders for legacy compatibility
    setAllOrders([...loadedOrders]);

    // Re-apply filters to include new orders
    applyFiltersAndRender();
  } catch (error) {
    console.error('Failed to load more orders:', error);
  } finally {
    setIsLoadingMore(false);
  }
}

export async function applyFiltersAndRender(): Promise<void> {
  // If there's a search query, use backend search instead of client-side filtering
  if (currentSearchQuery) {
    try {
      const { startDate, endDate } = getDateRangeParams();
      const searchResults = await api.searchOrders(
        currentSearchQuery,
        currentAccountId,
        startDate,
        endDate,
        currentFilter === 'all' ? null : currentFilter,
        500, // Limit search results
      );
      setCurrentRenderedOrders(searchResults);
      renderOrders(searchResults);
      updateResultsCount(searchResults.length, searchResults.length);
    } catch (error) {
      console.error('Search failed:', error);
      // Fallback to empty results on error
      setCurrentRenderedOrders([]);
      renderOrders([]);
      updateResultsCount(0, 0);
    }
  } else {
    // No search query, render loaded orders (paginated)
    setCurrentRenderedOrders(loadedOrders);
    renderOrders(loadedOrders);
    updateResultsCount(loadedOrders.length, loadedOrders.length);
  }
}

// Debounced version for search input (reduces re-renders during typing)
let searchDebounceTimeout: number | null = null;
export function applyFiltersAndRenderDebounced(delay = 200): void {
  if (searchDebounceTimeout !== null) {
    clearTimeout(searchDebounceTimeout);
  }
  searchDebounceTimeout = window.setTimeout(() => {
    // Call async function without await (fire and forget)
    applyFiltersAndRender().catch(error => {
      console.error('Error in debounced filter/render:', error);
    });
  }, delay);
}

function renderOrders(orders: OrderViewModel[]): void {
  const container = document.getElementById('order-list');
  if (!container) return;

  if (orders.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <p>No orders found</p>
      </div>
    `;
    return;
  }

  const sorted = sortOrders(orders, currentSort);
  setCurrentRenderedOrders(sorted);

  // Use virtual scrolling for large lists
  const VIRTUAL_SCROLL_THRESHOLD = 50;
  if (sorted.length > VIRTUAL_SCROLL_THRESHOLD) {
    renderVirtualScrollOrders(sorted, container);
  } else {
    // For small lists, use traditional rendering (no performance issue)
    const html = `
      ${buildOrdersHtml(sorted)}
      <div id="scroll-sentinel" style="height: 1px;"></div>
      ${isLoadingMore ? '<div class="loading-indicator">Loading more...</div>' : ''}
    `;
    container.innerHTML = html;
    postRenderOrders();
    // Re-observe sentinel for infinite scroll
    setupInfiniteScroll();
  }
}

function renderVirtualScrollOrders(orders: OrderViewModel[], container: HTMLElement): void {
  // Calculate visible range
  const scrollTop = container.scrollTop;
  const containerHeight = container.clientHeight;
  const itemHeight = virtualScrollState.itemHeight;
  const bufferSize = 5; // Render extra items above/below for smooth scrolling

  const visibleStart = Math.max(0, Math.floor(scrollTop / itemHeight) - bufferSize);
  const visibleEnd = Math.min(
    orders.length,
    Math.ceil((scrollTop + containerHeight) / itemHeight) + bufferSize
  );

  // Update virtual scroll state
  setVirtualScrollState({
    scrollTop,
    containerHeight,
    visibleStart,
    visibleEnd,
    totalItems: orders.length,
  });

  const visibleOrders = orders.slice(visibleStart, visibleEnd);

  // Calculate spacer heights
  const topSpacerHeight = visibleStart * itemHeight;
  const bottomSpacerHeight = (orders.length - visibleEnd) * itemHeight;

  // Build HTML with spacers
  const html = `
    <div style="height: ${topSpacerHeight}px;"></div>
    ${buildOrdersHtml(visibleOrders)}
    <div style="height: ${bottomSpacerHeight}px;"></div>
    <div id="scroll-sentinel" style="height: 1px;"></div>
    ${isLoadingMore ? '<div class="loading-indicator">Loading more...</div>' : ''}
  `;

  container.innerHTML = html;
  postRenderOrders();
  setupScrollListener(container);
  // Re-observe sentinel after re-render (sentinel gets recreated on every render)
  setupInfiniteScroll();
}

function setupScrollListener(container: HTMLElement): void {
  // Remove existing listener if any
  const oldListener = (container as any)._scrollListener;
  if (oldListener) {
    container.removeEventListener('scroll', oldListener);
  }

  // Debounced scroll handler for performance
  let scrollTimeout: number | null = null;
  const scrollListener = () => {
    if (scrollTimeout !== null) {
      clearTimeout(scrollTimeout);
    }
    scrollTimeout = window.setTimeout(() => {
      renderOrders(currentRenderedOrders);
    }, 16); // ~60fps
  };

  container.addEventListener('scroll', scrollListener, { passive: true });
  (container as any)._scrollListener = scrollListener;
}

function getCachedProductSummary(order: OrderViewModel): string {
  // Check cache first
  const cached = productSummaryCache.get(order.id);
  if (cached !== undefined) {
    return cached;
  }

  // Compute and cache
  const summary = getProductSummary(order);
  productSummaryCache.set(order.id, summary);
  return summary;
}

function buildOrdersHtml(orders: OrderViewModel[]): string {
  return orders.map(order => {
    const totalQty = order.items?.reduce((sum, item) => sum + (item.quantity || 1), 0) || 0;
    const isExpanded = expandedOrderId === order.id;
    return `
    <div class="order-item${isExpanded ? ' expanded' : ''}" data-order-id="${order.id}">
      ${renderOrderThumbnail(order)}
      <div class="order-main">
        <div class="order-id">${escapeHtml(order.id)}</div>
        <div class="order-date">${escapeHtml(displayDate(order))}</div>
      </div>
      <div class="order-product">${escapeHtml(getCachedProductSummary(order))}</div>
      <div class="order-qty">x${totalQty}</div>
      <div class="order-price">${order.total_cost ? '$' + escapeHtml(order.total_cost) : ''}</div>
      <span class="order-status status-${order.status}">${statusLabels[order.status] || order.status}</span>
      <div class="order-items-count">${order.items?.length || 0} items</div>
    </div>
    <div class="order-details${isExpanded ? ' show' : ''}" id="details-${order.id}">
      ${renderOrderDetails(order)}
    </div>
  `;
  }).join('');
}

function postRenderOrders(): void {
  observeOrderThumbnails();

  if (expandedOrderId) {
    const details = document.getElementById(`details-${expandedOrderId}`);
    if (details) {
      const order = currentRenderedOrders.find(o => o.id === expandedOrderId);
      if (order && order.tracking_number && !trackingCache.has(order.id)) {
        loadCachedTrackingStatus(order.id);
      }
      loadOrderImages(expandedOrderId);
    }
  }
}

function renderOrderDetails(order: OrderViewModel): string {
  const trackingHtml = order.tracking_number
    ? renderTrackingSection(order)
    : '<span style="color: var(--text-tertiary);">No tracking</span>';

  const itemsHtml = order.items?.length > 0
    ? `<ul class="item-list">${order.items.map(item => `
        <li class="item-row">
          ${renderItemImage(item)}
          <span class="item-name ${item.status === 'canceled' ? 'item-canceled' : ''}">${escapeHtml(item.name)}</span>
          ${item.quantity > 1 ? `<span class="item-qty">x${item.quantity}</span>` : ''}
        </li>
      `).join('')}</ul>`
    : '<span style="color: var(--text-tertiary);">No items</span>';

  const cancelReasonHtml = order.cancel_reason && (order.status === 'canceled' || order.status === 'partially_canceled')
    ? `<div class="detail-section">
        <h4>Cancel Reason</h4>
        <div class="detail-value" style="color: var(--accent-red);">${escapeHtml(order.cancel_reason)}</div>
      </div>`
    : '';

  return `
    <div class="detail-grid">
      <div class="detail-section tracking-section">
        <h4>Tracking</h4>
        <div class="detail-value" id="tracking-${order.id}">${trackingHtml}</div>
      </div>
      <div class="detail-section">
        <h4>Total</h4>
        <div class="detail-value">${order.total_cost ? '$' + escapeHtml(order.total_cost) : '-'}</div>
      </div>
      <div class="detail-section">
        <h4>Recipient</h4>
        <div class="detail-value">${order.recipient ? escapeHtml(order.recipient) : '<span style="color: var(--text-tertiary);">-</span>'}</div>
      </div>
      ${cancelReasonHtml}
      <div class="detail-section">
        <h4>Items</h4>
        ${itemsHtml}
      </div>
    </div>
  `;
}

function renderOrderThumbnail(order: OrderViewModel): string {
  const imageId = order.thumbnail_id;
  const imageUrl = order.thumbnail_url;
  if (!imageId && !imageUrl) {
    return '<div class="order-thumb placeholder"></div>';
  }

  const imageIdAttr = imageId ? ` data-image-id="${escapeHtml(imageId)}"` : '';
  const fallbackAttr = imageUrl ? ` data-fallback-url="${escapeHtml(imageUrl)}"` : '';

  return `<img class="order-thumb"${imageIdAttr}${fallbackAttr} alt="" loading="lazy">`;
}

function renderItemImage(item: OrderViewModel['items'][0]): string {
  if (!item.image_id && !item.image_url) {
    return '';
  }

  const imageIdAttr = item.image_id ? ` data-image-id="${escapeHtml(item.image_id)}"` : '';
  const fallbackAttr = item.image_url ? ` data-fallback-url="${escapeHtml(item.image_url)}"` : '';
  const alt = escapeHtml(item.name || '');

  return `<img class="item-thumb"${imageIdAttr}${fallbackAttr} alt="${alt}" loading="lazy">`;
}

export async function toggleOrderDetails(orderId: string): Promise<void> {
  const newExpandedId = expandedOrderId === orderId ? null : orderId;
  setExpandedOrderId(newExpandedId);

  // Optimized: update only the affected DOM elements instead of re-rendering everything
  const orderItem = document.querySelector(`.order-item[data-order-id="${orderId}"]`);
  const detailsDiv = document.getElementById(`details-${orderId}`);

  if (orderItem && detailsDiv) {
    if (newExpandedId === orderId) {
      // Expanding
      orderItem.classList.add('expanded');
      detailsDiv.classList.add('show');

      // Load tracking and images if needed
      const order = currentRenderedOrders.find(o => o.id === orderId);
      if (order && order.tracking_number && !trackingCache.has(orderId)) {
        loadCachedTrackingStatus(orderId);
      }
      loadOrderImages(orderId);
    } else {
      // Collapsing
      orderItem.classList.remove('expanded');
      detailsDiv.classList.remove('show');
    }
  }
}

// Image / thumbnail handling

async function loadOrderImages(orderId: string): Promise<void> {
  const order = allOrders.find(o => o.id === orderId);
  if (!order || !order.items) return;

  const container = document.getElementById(`details-${orderId}`);
  if (!container) return;

  const images = container.querySelectorAll<HTMLImageElement>('img.item-thumb[data-image-id], img.item-thumb[data-fallback-url]');
  await loadThumbnailsForElements(Array.from(images));
}

async function fetchCachedThumbnails(imageIds: string[]): Promise<Record<string, string>> {
  const unique = imageIds.filter(id => id && !thumbnailCache.has(id));
  if (unique.length === 0) {
    return {};
  }

  try {
    const result = await api.getCachedThumbnails(unique);
    if (result && typeof result === 'object') {
      for (const [id, dataUrl] of Object.entries(result)) {
        if (dataUrl) {
          thumbnailCache.set(id, dataUrl);
        }
      }
    }
    return result || {};
  } catch (error) {
    console.warn('Failed to fetch cached thumbnails', error);
    return {};
  }
}

function applyThumbnailSrc(img: HTMLImageElement, dataUrl: string): void {
  img.setAttribute('src', dataUrl);
  img.setAttribute('srcset', `${dataUrl} 2x`);
}

async function loadThumbnailsForElements(elements: HTMLImageElement[]): Promise<void> {
  const pending: { img: HTMLImageElement; imageId: string; fallbackUrl: string | null }[] = [];
  const ids = new Set<string>();

  elements.forEach(img => {
    if (!img || img.getAttribute('src')) return;
    const imageId = img.getAttribute('data-image-id');
    const fallbackUrl = img.getAttribute('data-fallback-url');

    if (!imageId) {
      if (fallbackUrl) img.setAttribute('src', fallbackUrl);
      return;
    }

    const cached = thumbnailCache.get(imageId);
    if (cached) {
      applyThumbnailSrc(img, cached);
      return;
    }

    ids.add(imageId);
    pending.push({ img, imageId, fallbackUrl });
  });

  if (ids.size === 0) return;

  const fetched = await fetchCachedThumbnails(Array.from(ids));
  pending.forEach(({ img, imageId, fallbackUrl }) => {
    const dataUrl = thumbnailCache.get(imageId) || fetched?.[imageId];
    if (dataUrl) {
      applyThumbnailSrc(img, dataUrl);
    } else if (fallbackUrl) {
      img.setAttribute('src', fallbackUrl);
    }
  });
}

function observeOrderThumbnails(): void {
  if (orderThumbObserver) {
    orderThumbObserver.disconnect();
  }

  const root = document.getElementById('order-list');
  const observer = new IntersectionObserver(
    (entries) => {
      const visible: HTMLImageElement[] = [];
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          const img = entry.target as HTMLImageElement;
          observer.unobserve(img);
          visible.push(img);
        }
      });
      if (visible.length > 0) {
        loadThumbnailsForElements(visible);
      }
    },
    { root, rootMargin: '200px 0px' },
  );
  setOrderThumbObserver(observer);

  document
    .querySelectorAll<HTMLImageElement>('img.order-thumb[data-image-id], img.order-thumb[data-fallback-url]')
    .forEach(img => observer.observe(img));
}
