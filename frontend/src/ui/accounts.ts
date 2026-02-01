import type { AccountViewModel } from '../types';
import {
  allAccounts,
  currentAccountId,
  setCurrentAccountId,
} from '../state';
import { escapeHtml } from '../utils';
import * as api from '../api';
import { loadDashboard } from './orders';
import { listen } from '@tauri-apps/api/event';

export function renderAccountSelector(accounts: AccountViewModel[]): void {
  const select = document.getElementById('account-select') as HTMLSelectElement | null;
  const avatar = document.getElementById('account-avatar') as HTMLImageElement | null;
  const avatarFallback = document.getElementById('account-avatar-fallback');

  if (!select || !avatar || !avatarFallback) return;

  const selectedAccount = currentAccountId
    ? accounts.find(a => a.id === currentAccountId)
    : accounts[0];

  if (selectedAccount && selectedAccount.profile_picture_url) {
    avatar.src = selectedAccount.profile_picture_url;
    avatar.alt = selectedAccount.email;
    avatar.title = selectedAccount.email;
    avatar.style.display = 'block';
    avatarFallback.style.display = 'none';
  } else if (selectedAccount) {
    avatarFallback.textContent = selectedAccount.email.charAt(0).toUpperCase();
    avatarFallback.title = selectedAccount.email;
    avatarFallback.style.display = 'flex';
    avatar.style.display = 'none';
  } else {
    avatar.style.display = 'none';
    avatarFallback.style.display = 'none';
  }

  if (accounts.length <= 1) {
    select.style.display = 'none';
    return;
  }

  select.style.display = 'block';

  const optionsHtml = `
    <option value="">All Accounts (${accounts.reduce((sum, a) => sum + a.order_count, 0)})</option>
    ${accounts.map(acc => {
      const shortEmail = acc.email.split('@')[0];
      return `<option value="${acc.id}" ${acc.id === currentAccountId ? 'selected' : ''}>
        ${escapeHtml(shortEmail)} (${acc.order_count})
      </option>`;
    }).join('')}
  `;
  select.innerHTML = optionsHtml;
}

export async function handleAccountChange(value: string): Promise<void> {
  setCurrentAccountId(value ? parseInt(value, 10) : null);
  await loadDashboard();
}

export function openAccountModal(): void {
  renderAccountModal();
  const overlay = document.getElementById('account-modal-overlay');
  if (overlay) overlay.style.display = 'flex';
}

export function closeAccountModal(event?: Event): void {
  if (event && event.target !== event.currentTarget) return;
  const overlay = document.getElementById('account-modal-overlay');
  if (overlay) overlay.style.display = 'none';
}

function renderAccountModal(): void {
  const body = document.getElementById('account-modal-body');
  if (!body) return;
  const accounts = allAccounts;

  let html = `<p class="modal-account-count">You currently have ${accounts.length} account${accounts.length !== 1 ? 's' : ''} connected.</p>`;

  for (const acc of accounts) {
    const shortEmail = acc.email.split('@')[0];
    const avatarHtml = acc.profile_picture_url
      ? `<img class="modal-account-avatar" src="${escapeHtml(acc.profile_picture_url)}" alt="">`
      : `<div class="modal-account-avatar-fallback">${escapeHtml(acc.email.charAt(0).toUpperCase())}</div>`;

    html += `
    <div class="modal-account-card">
      ${avatarHtml}
      <div class="modal-account-info">
        <div class="modal-account-name">${escapeHtml(shortEmail)}</div>
        <div class="modal-account-email">${escapeHtml(acc.email)}</div>
      </div>
      <button class="modal-disconnect-btn" data-account-id="${acc.id}">Disconnect</button>
    </div>`;
  }

  html += `<button class="modal-connect-btn">+ Connect Gmail Account</button>`;
  body.innerHTML = html;
}

export async function connectAccount(): Promise<void> {
  showAuthOverlay();
  try {
    const email = await api.addAccount();
    console.log('Added account:', email);
    setCurrentAccountId(null);
    await loadDashboard();
    renderAccountModal();
  } catch (e) {
    const msg = String(e);
    if (!msg.includes('cancelled')) {
      console.error('Failed to add account:', e);
      alert('Failed to add account: ' + e);
    }
  } finally {
    hideAuthOverlay();
  }
}

let authUrlUnlisten: (() => void) | null = null;
let currentAuthUrl: string | null = null;

export function showAuthOverlay(): void {
  currentAuthUrl = null;

  let overlay = document.getElementById('auth-overlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'auth-overlay';
    document.body.appendChild(overlay);
  }

  overlay.innerHTML = `
    <div class="auth-overlay-content">
      <div class="auth-spinner"></div>
      <h3>Connecting to Google...</h3>
      <p>Complete sign-in in your browser window.</p>
      <p class="auth-hint">A browser tab should have opened automatically.</p>
      <div class="auth-overlay-actions">
        <button class="auth-copy-btn" style="display: none;">Copy Link</button>
        <button class="auth-cancel-btn">Cancel</button>
      </div>
    </div>
  `;

  const copyBtn = overlay.querySelector('.auth-copy-btn') as HTMLButtonElement | null;
  const cancelBtn = overlay.querySelector('.auth-cancel-btn') as HTMLButtonElement | null;

  cancelBtn?.addEventListener('click', () => {
    api.cancelAddAccount().catch(() => {});
  });

  copyBtn?.addEventListener('click', () => {
    if (currentAuthUrl) {
      navigator.clipboard.writeText(currentAuthUrl).then(() => {
        if (copyBtn) copyBtn.textContent = 'Copied!';
        setTimeout(() => { if (copyBtn) copyBtn.textContent = 'Copy Link'; }, 2000);
      });
    }
  });

  // Listen for auth URL from backend so user can copy/re-open it
  listen<string>('auth-url', (event) => {
    currentAuthUrl = event.payload;
    if (copyBtn) copyBtn.style.display = '';
  }).then(unlisten => {
    authUrlUnlisten = unlisten;
  });

  overlay.style.display = 'flex';
}

export function hideAuthOverlay(): void {
  const overlay = document.getElementById('auth-overlay');
  if (overlay) overlay.style.display = 'none';

  if (authUrlUnlisten) {
    authUrlUnlisten();
    authUrlUnlisten = null;
  }
  currentAuthUrl = null;
}

export async function disconnectAccount(accountId: number): Promise<void> {
  const account = allAccounts.find(a => a.id === accountId);
  if (!account) return;

  if (!confirm(`Disconnect ${account.email}?\n\nThis will delete all synced emails and orders for this account.`)) {
    return;
  }

  try {
    const result = await api.removeAccount(accountId);
    console.log('Removed account:', result);
    setCurrentAccountId(null);
    await loadDashboard();
    if (allAccounts.length === 0) {
      closeAccountModal();
    } else {
      renderAccountModal();
    }
  } catch (e) {
    console.error('Failed to remove account:', e);
    alert('Failed to remove account: ' + e);
  }
}
