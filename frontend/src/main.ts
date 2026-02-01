// CSS imports — Vite processes these through PostCSS/Tailwind
import './styles/tailwind.css';
import './styles/app.css';

import { listen } from '@tauri-apps/api/event';
import type { NewEmailCheck } from './types';
import { setNewEmailCount } from './state';
import {
  setupFilterListeners,
  setupDatePresetListeners,
  bindSidebarDeps,
} from './ui/sidebar';
import {
  loadDashboard,
  applyFiltersAndRender,
  applyFiltersAndRenderDebounced,
  toggleOrderDetails,
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
import { setupTabs, getActiveTab } from './ui/tabs';
import { setupAnalyticsTab } from './ui/analytics';
import { setupAccountsTab } from './ui/accounts-tab';
import { setupSettingsTab, bindSettingsDeps } from './ui/settings-tab';

// Wire up sidebar's lazy dependencies (avoids circular imports at module evaluation time)
bindSidebarDeps({
  loadDashboard,
  applyFiltersAndRender,
  applyFiltersAndRenderDebounced,
  checkForNewEmails,
});

// Wire up settings tab dependency
bindSettingsDeps({ checkForNewEmails });

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
  setupTabs();
  setupAnalyticsTab();
  setupAccountsTab();
  setupSettingsTab();
  setupFilterListeners();
  setupDatePresetListeners();
  setupEventDelegation();
  checkForNewEmails();
  loadDashboard();

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
});

// Auto-refresh every 60 seconds (only when on Orders tab)
setInterval(() => {
  if (getActiveTab() === 'orders') {
    loadDashboard();
  }
}, 60_000);
