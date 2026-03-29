// CSS imports — Vite processes these through PostCSS/Tailwind
import './styles/tailwind.css';
import './styles/app.css';

import { listen } from '@tauri-apps/api/event';
import type { NewEmailCheck } from './types';

interface OnnxUnavailablePayload {
  message: string;
  download_url: string;
}

interface ImagesReprocessedPayload {
  count: number;
  message: string;
}
import * as api from './api';
import { setNewEmailCount } from './state';
import {
  setupFilterListeners,
  setupDatePresetListeners,
  setupFetchSincePicker,
  bindSidebarDeps,
} from './ui/sidebar';
import {
  loadDashboard,
  applyFiltersAndRender,
  applyFiltersAndRenderDebounced,
  toggleOrderDetails,
  setupSortBar,
} from './ui/orders';
import { checkForNewEmails, syncOrders, updateSyncBadge } from './ui/sync';
import {
  handleAccountChange,
  openAccountModal,
  closeAccountModal,
  connectAccount,
  disconnectAccount,
} from './ui/accounts';
import { handleFetchTrackingClick } from './ui/tracking';
import { setupTabs, switchTab } from './ui/tabs';
import { setupTitlebar } from './ui/titlebar';
import { setupAnalyticsTab } from './ui/analytics';
import { setupAccountsTab } from './ui/accounts-tab';
import { setupSettingsTab } from './ui/settings-tab';
import { setupTheme } from './ui/theme';

// Wire up sidebar's lazy dependencies (avoids circular imports at module evaluation time)
bindSidebarDeps({
  loadDashboard,
  applyFiltersAndRender,
  applyFiltersAndRenderDebounced,
  checkForNewEmails,
});

// Event delegation — replaces all inline onclick/onchange/onerror handlers

function setupEventDelegation(): void {
  // Sync button
  document.getElementById('sync-btn')?.addEventListener('click', () => {
    syncOrders();
  });

  // Account select dropdown
  document.getElementById('account-select')?.addEventListener('change', (e) => {
    handleAccountChange((e.target as HTMLSelectElement).value);
  });

  // Account avatar click → open account modal
  document.getElementById('account-avatar')?.addEventListener('click', () => {
    openAccountModal();
  });
  document.getElementById('account-avatar-fallback')?.addEventListener('click', () => {
    openAccountModal();
  });

  // Settings gear button → switch to settings tab
  document.getElementById('settings-btn')?.addEventListener('click', () => {
    switchTab('settings');
  });

  // Modal overlay click-to-close
  document.getElementById('account-modal-overlay')?.addEventListener('click', (e) => {
    closeAccountModal(e);
  });

  // Modal content — stop click propagation so overlay close doesn't fire
  document.querySelector('#account-modal-overlay .modal-content')?.addEventListener('click', (e) => {
    e.stopPropagation();
  });

  // Modal close (X) button
  document.querySelector('#account-modal-overlay .modal-close')?.addEventListener('click', () => {
    closeAccountModal();
  });

  // Account avatar error fallback
  document.getElementById('account-avatar')?.addEventListener('error', function (this: HTMLImageElement) {
    this.style.display = 'none';
    const fallback = document.getElementById('account-avatar-fallback');
    if (fallback) fallback.style.display = 'flex';
  });

  // Delegated: order item click → toggle details
  document.getElementById('order-list')?.addEventListener('click', (e) => {
    const target = e.target as HTMLElement;

    // Don't toggle when clicking a link or button inside the order details
    if (target.closest('a') || target.closest('button')) return;

    const orderItem = target.closest<HTMLElement>('.order-item');
    if (orderItem) {
      const orderId = orderItem.dataset.orderId;
      if (orderId) toggleOrderDetails(orderId);
    }
  });

  // Delegated: fetch tracking button (inside order details)
  document.addEventListener('click', (e) => {
    const target = e.target as HTMLElement;
    if (target.classList.contains('fetch-tracking-btn')) {
      e.stopPropagation();
      const orderId = target.dataset.orderId;
      if (orderId) handleFetchTrackingClick(orderId, target as HTMLButtonElement);
    }
  });

  // Delegated: account modal buttons (disconnect / connect)
  document.getElementById('account-modal-body')?.addEventListener('click', (e) => {
    const target = e.target as HTMLElement;
    if (target.classList.contains('modal-disconnect-btn')) {
      const accountId = target.dataset.accountId;
      if (accountId) disconnectAccount(parseInt(accountId, 10));
      return;
    }
    if (target.classList.contains('modal-connect-btn')) {
      connectAccount();
    }
  });
}

// Initialization

document.addEventListener('DOMContentLoaded', () => {
  setupTheme();
  setupTabs();
  setupTitlebar();
  setupAnalyticsTab();
  setupAccountsTab();
  setupSettingsTab();
  setupFilterListeners();
  setupDatePresetListeners();
  setupFetchSincePicker();
  setupEventDelegation();
  setupSortBar();
  checkForNewEmails();
  loadDashboard();

  // Auto-refresh tracking for shipped orders (fire-and-forget)
  if (localStorage.getItem('autoRefreshTracking') !== 'false') {
    api.refreshShippedTracking().catch(e => {
      console.error('Auto-refresh tracking failed:', e);
    });
  }

  // Tauri backend events
  listen('tracking-sync-complete', () => {
    console.log('Tracking sync complete, refreshing dashboard...');
    loadDashboard();
  });

  listen<NewEmailCheck>('new-emails-available', (event) => {
    const check = event.payload;
    console.log('New email check:', check);
    setNewEmailCount((check.total_new || 0) + (check.total_pending || 0));
    updateSyncBadge();
  });

  // ONNX/background removal unavailable - logged for debugging
  // The welcome overlay now handles prompting the user to install VC++ Redistributable
  listen<OnnxUnavailablePayload>('onnx-unavailable', (event) => {
    console.warn('ONNX unavailable:', event.payload.message);
  });

  // Images reprocessed with transparency (after VC++ install)
  listen<ImagesReprocessedPayload>('images-reprocessed', (event) => {
    const { count, message } = event.payload;
    console.log('Images reprocessed:', message);
    if (count > 0) {
      // Refresh dashboard to show updated images with transparency
      loadDashboard();
    }
  });
});
