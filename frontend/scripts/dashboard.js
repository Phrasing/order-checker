// Walmart Order Dashboard - Sidebar + List Layout
// Uses Tauri's invoke API to communicate with the Rust backend

const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;

// Global state
let allOrders = [];
let allAccounts = [];
let currentFilter = 'all';
let currentSort = 'date'; // 'date' or 'status'
let currentAccountId = null; // null = all accounts
let currentDatePreset = '0'; // '7', '30', '90', or '0' (all)

// Status priority for sorting (lower = higher priority)
const statusPriority = {
    'shipped': 1,
    'confirmed': 2,
    'partially_canceled': 3,
    'delivered': 4,
    'canceled': 5
};

// Carrier tracking URL generators
const trackingUrls = {
    fedex: (num) => `https://www.fedex.com/fedextrack/?trknbr=${num}`,
    ups: (num) => `https://www.ups.com/track?tracknum=${num}`,
    usps: (num) => `https://tools.usps.com/go/TrackConfirmAction?tLabels=${num}`
};

// Status display labels
const statusLabels = {
    confirmed: 'Confirmed',
    shipped: 'Shipped',
    delivered: 'Delivered',
    canceled: 'Canceled',
    partially_canceled: 'Partial'
};

// Tracking state badge colors
const trackingStateColors = {
    label_created: 'badge-gray',
    in_transit: 'badge-blue',
    out_for_delivery: 'badge-yellow',
    delivered: 'badge-green',
    exception: 'badge-red',
    available_for_pickup: 'badge-purple',
    unknown: 'badge-gray'
};

// Cache for tracking status
const trackingCache = new Map();

/**
 * Get date range params from current preset
 * @returns {{startDate: string|null, endDate: string|null}}
 */
function getDateRangeParams() {
    if (currentDatePreset === '0') {
        return { startDate: null, endDate: null };
    }
    const days = parseInt(currentDatePreset);
    const end = new Date();
    const start = new Date();
    start.setDate(start.getDate() - days);
    return {
        startDate: start.toISOString().split('T')[0],
        endDate: end.toISOString().split('T')[0],
    };
}

/**
 * Load dashboard data from the Rust backend
 */
async function loadDashboard() {
    try {
        const { startDate, endDate } = getDateRangeParams();
        // Build args object - only include non-null values
        // Tauri v2 uses camelCase for parameter names
        const args = {};
        if (currentAccountId !== null) args.accountId = currentAccountId;
        if (startDate !== null) args.startDate = startDate;
        if (endDate !== null) args.endDate = endDate;
        console.log('Invoking get_dashboard with args:', args);
        const data = await invoke('get_dashboard', args);
        allOrders = data.orders || [];
        allAccounts = data.accounts || [];
        renderSidebar(data);
        renderAccountSelector(allAccounts);
        renderStats(allOrders);
        renderOrders(filterOrders(allOrders, currentFilter));
        updateHeader();
    } catch (error) {
        console.error('Failed to load dashboard:', error);
        document.getElementById('order-list').innerHTML = `
            <div class="empty-state">
                <p>Failed to load orders</p>
                <p style="font-size: 12px; margin-top: 8px;">${escapeHtml(String(error))}</p>
            </div>
        `;
    }
}

/**
 * Render sidebar counts
 */
function renderSidebar(data) {
    document.getElementById('total-count').textContent = data.total_orders || 0;
    document.getElementById('count-all').textContent = data.total_orders || 0;
    document.getElementById('count-confirmed').textContent = data.status_counts?.confirmed || 0;
    document.getElementById('count-shipped').textContent = data.status_counts?.shipped || 0;
    document.getElementById('count-delivered').textContent = data.status_counts?.delivered || 0;
    document.getElementById('count-canceled').textContent = data.status_counts?.canceled || 0;
    document.getElementById('pending-emails').textContent = `${data.pending_emails || 0} pending emails`;
    document.getElementById('last-updated').textContent = data.last_updated || '';
}

/**
 * Render aggregated statistics
 */
function renderStats(orders) {
    // Exclude canceled orders from totals
    const activeOrders = orders.filter(o => o.status !== 'canceled');

    // Calculate total spent (sum of all non-canceled order totals)
    const totalSpent = activeOrders.reduce((sum, order) => {
        const cost = parseFloat(order.total_cost) || 0;
        return sum + cost;
    }, 0);

    // Calculate total quantity ordered (non-canceled only)
    const totalQty = activeOrders.reduce((sum, order) => {
        return sum + (order.items?.reduce((itemSum, item) => itemSum + (item.quantity || 1), 0) || 0);
    }, 0);

    // Calculate average order value (only for non-canceled orders with a total)
    const ordersWithTotal = activeOrders.filter(o => o.total_cost);
    const avgOrder = ordersWithTotal.length > 0
        ? totalSpent / ordersWithTotal.length
        : 0;

    // Count orders this week (excluding canceled)
    const oneWeekAgo = new Date();
    oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);
    const thisWeek = activeOrders.filter(o => new Date(o.order_date) >= oneWeekAgo).length;

    // Update UI
    document.getElementById('stat-total-spent').textContent = '$' + totalSpent.toFixed(2);
    document.getElementById('stat-total-qty').textContent = totalQty.toLocaleString();
    document.getElementById('stat-avg-order').textContent = '$' + avgOrder.toFixed(2);
    document.getElementById('stat-this-week').textContent = thisWeek + ' orders';
}

/**
 * Render account selector dropdown
 * Only shows the section if there are multiple accounts
 */
function renderAccountSelector(accounts) {
    const section = document.getElementById('account-section');
    const select = document.getElementById('account-select');

    // Only show if there are accounts configured
    if (accounts.length === 0) {
        section.style.display = 'none';
        return;
    }

    section.style.display = 'block';

    // Build options HTML
    const optionsHtml = `
        <option value="">All Accounts (${accounts.reduce((sum, a) => sum + a.order_count, 0)})</option>
        ${accounts.map(acc => {
            const displayName = acc.display_name || acc.email;
            const shortEmail = acc.email.split('@')[0];
            return `<option value="${acc.id}" ${acc.id === currentAccountId ? 'selected' : ''}>
                ${escapeHtml(shortEmail)} (${acc.order_count})
            </option>`;
        }).join('')}
    `;

    select.innerHTML = optionsHtml;
}

/**
 * Handle account selection change
 */
window.handleAccountChange = async function(value) {
    currentAccountId = value ? parseInt(value) : null;
    await loadDashboard();
};

/**
 * Filter orders by status
 */
function filterOrders(orders, filter) {
    if (filter === 'all') return orders;
    return orders.filter(o => o.status === filter);
}

/**
 * Update content header based on current filter
 */
function updateHeader() {
    const titles = {
        all: 'All Orders',
        confirmed: 'Confirmed Orders',
        shipped: 'Shipped Orders',
        delivered: 'Delivered Orders',
        canceled: 'Canceled Orders',
        partially_canceled: 'Partially Canceled'
    };
    document.getElementById('filter-title').textContent = titles[currentFilter] || 'All Orders';
}

/**
 * Sort orders by current sort mode
 */
function sortOrders(orders, sortBy) {
    return [...orders].sort((a, b) => {
        if (sortBy === 'status') {
            const priorityA = statusPriority[a.status] || 99;
            const priorityB = statusPriority[b.status] || 99;
            if (priorityA !== priorityB) return priorityA - priorityB;
        }
        // Secondary/primary sort by date (newest first)
        return new Date(b.order_date) - new Date(a.order_date);
    });
}

/**
 * Toggle sort mode between date and status
 */
window.toggleSort = function() {
    currentSort = currentSort === 'date' ? 'status' : 'date';
    updateSortButton();
    renderOrders(filterOrders(allOrders, currentFilter));
};

/**
 * Update sort button text and state
 */
function updateSortButton() {
    const btn = document.getElementById('sort-toggle');
    if (btn) {
        btn.textContent = currentSort === 'date' ? 'Sort: Date' : 'Sort: Status';
        btn.classList.toggle('active', currentSort === 'status');
    }
}

/**
 * Common product name prefixes to strip for cleaner display
 */
const stripPrefixes = [
    'Pokemon Trading Card Game ',
    'Pokémon Trading Card Game ',
    'Pokemon TCG ',
    'Pokémon TCG ',
    'Mega Evolution 2 5 ',
    'Scarlet & Violet ',
    'Scarlet and Violet ',
    'Sword & Shield ',
    'Sword and Shield ',
    'Sun & Moon ',
    'Sun and Moon ',
];

/**
 * Common product name suffixes to strip for cleaner display
 */
const stripSuffixes = [
    ' Randomly Selected',
    ' Randomly selected',
    ' - Randomly Selected',
];

/**
 * Get product name summary for order list display
 * Strips common prefixes and suffixes to show the unique/identifying part
 */
function getProductSummary(order) {
    if (!order.items || order.items.length === 0) return 'No items';

    let name = order.items[0].name;

    // Strip common prefixes (chain multiple)
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

    // Strip common suffixes (chain multiple)
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

/**
 * Render order list
 */
function renderOrders(orders) {
    const container = document.getElementById('order-list');

    if (orders.length === 0) {
        container.innerHTML = `
            <div class="empty-state">
                <p>No orders found</p>
                <p style="font-size: 12px; margin-top: 8px; color: var(--text-tertiary);">
                    Run sync and process commands to fetch emails
                </p>
            </div>
        `;
        return;
    }

    // Apply current sort
    orders = sortOrders(orders, currentSort);

    container.innerHTML = orders.map((order) => {
        const totalQty = order.items?.reduce((sum, item) => sum + (item.quantity || 1), 0) || 0;
        return `
        <div class="order-item" data-order-id="${order.id}" onclick="toggleOrderDetails('${order.id}')">
            <div class="order-main">
                <div class="order-id">${escapeHtml(order.id)}</div>
                <div class="order-date">${escapeHtml(order.order_date)}</div>
            </div>
            <div class="order-product">${escapeHtml(getProductSummary(order))}</div>
            <div class="order-qty">x${totalQty}</div>
            <div class="order-price">${order.total_cost ? '$' + escapeHtml(order.total_cost) : ''}</div>
            <span class="order-status status-${order.status}">${statusLabels[order.status] || order.status}</span>
            <div class="order-items-count">${order.items?.length || 0} items</div>
        </div>
        <div class="order-details" id="details-${order.id}">
            ${renderOrderDetails(order)}
        </div>
    `}).join('');
}

/**
 * Render expanded order details
 */
function renderOrderDetails(order) {
    const trackingHtml = order.tracking_number
        ? renderTrackingSection(order)
        : '<span style="color: var(--text-tertiary);">No tracking</span>';

    const itemsHtml = order.items?.length > 0
        ? `<ul class="item-list">${order.items.map(item => `
            <li>
                <span class="${item.status === 'canceled' ? 'item-canceled' : ''}">${escapeHtml(item.name)}</span>
                ${item.quantity > 1 ? `<span class="item-qty">x${item.quantity}</span>` : ''}
            </li>
        `).join('')}</ul>`
        : '<span style="color: var(--text-tertiary);">No items</span>';

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
                <h4>Items</h4>
                ${itemsHtml}
            </div>
        </div>
    `;
}

/**
 * Render tracking section with status and fetch button
 */
function renderTrackingSection(order) {
    const trackingLink = renderTrackingLink(order.tracking_number, order.carrier);
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
        <button class="fetch-tracking-btn" onclick="fetchTrackingStatus('${order.id}', event)">
            ${cachedStatus ? 'Refresh Status' : 'Get Status'}
        </button>
    `;
}

/**
 * Render tracking status badge and FedEx-style timeline
 */
function renderTrackingStatus(status) {
    const badgeClass = trackingStateColors[status.state] || 'badge-gray';

    // Group events by date
    const eventsByDate = groupEventsByDate(status.events);

    let timelineHtml = '';
    if (Object.keys(eventsByDate).length > 0) {
        timelineHtml = `<div class="tracking-timeline">
            ${Object.entries(eventsByDate).map(([date, events]) => `
                <div class="timeline-date-group">
                    <div class="timeline-date">${date}</div>
                    ${events.map(e => `
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

/**
 * Group events by date for timeline display
 */
function groupEventsByDate(events) {
    const groups = {};
    for (const e of events || []) {
        const dateTime = parseEventTime(e.time);
        const dateKey = dateTime ? formatDateHeader(dateTime) : 'Unknown Date';
        const timeKey = dateTime ? formatTime(dateTime) : '';

        if (!groups[dateKey]) groups[dateKey] = [];
        groups[dateKey].push({
            time: timeKey,
            description: e.description,
            location: e.location
        });
    }
    return groups;
}

/**
 * Parse event time string to Date object
 */
function parseEventTime(timeStr) {
    if (!timeStr) return null;
    // Handle ISO format: "2026-01-23T09:18:00-06:00"
    const date = new Date(timeStr);
    return isNaN(date.getTime()) ? null : date;
}

/**
 * Format date for timeline header (e.g., "Monday, 1/23/26")
 */
function formatDateHeader(date) {
    return date.toLocaleDateString('en-US', {
        weekday: 'long',
        month: 'numeric',
        day: 'numeric',
        year: '2-digit'
    });
}

/**
 * Format time for timeline event (e.g., "9:30 PM")
 */
function formatTime(date) {
    return date.toLocaleTimeString('en-US', {
        hour: 'numeric',
        minute: '2-digit',
        hour12: true
    });
}

/**
 * Fetch tracking status from backend
 */
window.fetchTrackingStatus = async function(orderId, event) {
    event.stopPropagation();

    const btn = event.target;
    btn.classList.add('loading');
    btn.disabled = true;

    try {
        const status = await invoke('fetch_tracking', { orderId });
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
};

/**
 * Render tracking link
 */
function renderTrackingLink(tracking, carrier) {
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

/**
 * Toggle order details visibility
 */
window.toggleOrderDetails = async function(orderId) {
    const details = document.getElementById(`details-${orderId}`);
    const item = document.querySelector(`.order-item[data-order-id="${orderId}"]`);

    // Close all other expanded details
    document.querySelectorAll('.order-details.show').forEach(el => {
        if (el.id !== `details-${orderId}`) {
            el.classList.remove('show');
        }
    });
    document.querySelectorAll('.order-item.expanded').forEach(el => {
        if (el.dataset.orderId !== orderId) {
            el.classList.remove('expanded');
        }
    });

    // Toggle current
    const isExpanding = !details.classList.contains('show');
    details.classList.toggle('show');
    item.classList.toggle('expanded');

    // Load cached tracking status from database when expanding
    if (isExpanding) {
        const order = allOrders.find(o => o.id === orderId);
        if (order && order.tracking_number && !trackingCache.has(order.id)) {
            loadCachedTrackingStatus(order.id);
        }
    }
};

/**
 * Load cached tracking status from database (doesn't fetch from 17track)
 */
async function loadCachedTrackingStatus(orderId) {
    try {
        const status = await invoke('get_tracking_status', { orderId });
        if (status) {
            trackingCache.set(orderId, status);
            const container = document.getElementById(`tracking-status-${orderId}`);
            if (container) {
                container.innerHTML = renderTrackingStatus(status);
            }
            // Update button text
            const btn = container?.parentElement?.querySelector('.fetch-tracking-btn');
            if (btn) {
                btn.textContent = 'Refresh Status';
            }
        }
    } catch (error) {
        console.error('Failed to load cached tracking:', error);
    }
}

/**
 * Handle sidebar filter clicks
 */
function setupFilterListeners() {
    document.querySelectorAll('.filter-item').forEach(item => {
        item.addEventListener('click', () => {
            // Update active state
            document.querySelectorAll('.filter-item').forEach(i => i.classList.remove('active'));
            item.classList.add('active');

            // Apply filter
            currentFilter = item.dataset.filter;
            updateHeader();
            renderOrders(filterOrders(allOrders, currentFilter));
        });
    });
}

/**
 * Handle date preset button clicks
 */
function setupDatePresetListeners() {
    document.querySelectorAll('.date-preset').forEach(btn => {
        btn.addEventListener('click', async () => {
            // Update active state
            document.querySelectorAll('.date-preset').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            // Update preset and reload data
            currentDatePreset = btn.dataset.days;
            await loadDashboard();
        });
    });
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    setupFilterListeners();
    setupDatePresetListeners();
    loadDashboard();

    // Listen for tracking sync complete event from backend
    listen('tracking-sync-complete', () => {
        console.log('Tracking sync complete, refreshing dashboard...');
        loadDashboard();
    });
});

// Auto-refresh every 60 seconds
setInterval(loadDashboard, 60000);
