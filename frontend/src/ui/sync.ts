import { listen } from '@tauri-apps/api/event';
import type { SyncProgress } from '../types';
import {
  newEmailCount,
  fetchSinceDate,
  isSyncing,
  setNewEmailCount,
  setIsSyncing,
} from '../state';
import * as api from '../api';
import { updateLastUpdated } from './sidebar';
import { loadDashboard } from './orders';

// ── Progress bar helpers ──

let progressUnlisten: (() => void) | null = null;

function showSyncProgress(): void {
  const el = document.getElementById('sync-progress');
  if (el) {
    el.classList.remove('fade-out');
    el.style.display = 'flex';
  }
  const fill = document.getElementById('sync-progress-fill');
  if (fill) fill.style.width = '0%';
  const text = document.getElementById('sync-progress-text');
  if (text) text.textContent = '';
}

function updateSyncProgress(progress: SyncProgress): void {
  const fill = document.getElementById('sync-progress-fill');
  const text = document.getElementById('sync-progress-text');
  const percent = (progress.stage / progress.total_stages) * 100;

  if (fill) fill.style.width = `${percent}%`;
  if (text) text.textContent = `${progress.label} — ${progress.detail}`;
}

function hideSyncProgress(): void {
  setTimeout(() => {
    const el = document.getElementById('sync-progress');
    if (el) {
      el.classList.add('fade-out');
      setTimeout(() => {
        el.style.display = 'none';
        el.classList.remove('fade-out');
      }, 300);
    }
  }, 1500);
}

// ── New email check ──

export async function checkForNewEmails(): Promise<void> {
  try {
    const args = fetchSinceDate || undefined;
    const check = await api.checkNewEmails(args);
    console.log('New email check:', check);
    setNewEmailCount((check.total_new || 0) + (check.total_pending || 0));
    updateSyncBadge();
  } catch (e) {
    console.error('Failed to check for new emails:', e);
  }
}

// ── Sync badge ──

export function updateSyncBadge(): void {
  const btn = document.getElementById('sync-btn');
  if (!btn) return;
  let badge = document.getElementById('sync-badge');

  if (newEmailCount > 0) {
    if (!badge) {
      badge = document.createElement('span');
      badge.id = 'sync-badge';
      badge.className = 'sync-badge';
      btn.appendChild(badge);
    }
    badge.textContent = newEmailCount > 99 ? '99+' : String(newEmailCount);
    badge.style.display = 'inline-flex';
  } else if (badge) {
    badge.style.display = 'none';
  }
}

// ── Sync orders ──

export async function syncOrders(): Promise<void> {
  if (isSyncing) return;

  setIsSyncing(true);
  const btn = document.getElementById('sync-btn');
  if (btn) {
    btn.classList.add('loading');
    (btn as HTMLButtonElement).disabled = true;
  }

  // Start listening for progress events
  showSyncProgress();
  progressUnlisten = await listen<SyncProgress>('sync-progress', (event) => {
    updateSyncProgress(event.payload);
    if (event.payload.stage === event.payload.total_stages) {
      hideSyncProgress();
    }
  });

  try {
    console.log('Starting sync...');
    const args = fetchSinceDate || undefined;
    const result = await api.syncAndProcessOrders(args);
    console.log('Sync complete:', result);

    if (result.success) {
      updateLastUpdated(`Synced ${result.emails_synced} emails, processed ${result.orders_processed} orders`);
    } else {
      console.warn('Sync had errors:', result.errors);
      updateLastUpdated('Sync completed with errors');
    }

    await loadDashboard();
  } catch (error) {
    console.error('Sync failed:', error);
    updateLastUpdated(`Sync failed: ${error}`);
    hideSyncProgress();
  } finally {
    setIsSyncing(false);
    if (btn) {
      btn.classList.remove('loading');
      (btn as HTMLButtonElement).disabled = false;
    }
    setNewEmailCount(0);
    updateSyncBadge();
    if (progressUnlisten) {
      progressUnlisten();
      progressUnlisten = null;
    }
  }
}
