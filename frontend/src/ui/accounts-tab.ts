import { allAccounts } from '../state';
import { escapeHtml } from '../utils';
import * as api from '../api';
import { loadDashboard } from './orders';
import { onTabActivate } from './tabs';
import { showAuthOverlay, hideAuthOverlay } from './accounts';

export function setupAccountsTab(): void {
  onTabActivate('accounts', renderAccountsPage);

  // Delegated click handler for accounts tab buttons
  document.getElementById('accounts-content')?.addEventListener('click', (e) => {
    const target = e.target as HTMLElement;
    if (target.classList.contains('accounts-disconnect-btn')) {
      const accountId = target.dataset.accountId;
      if (accountId) disconnectFromTab(parseInt(accountId, 10));
    }
    if (target.classList.contains('accounts-connect-btn')) {
      connectFromTab();
    }
  });
}

function renderAccountsPage(): void {
  const container = document.getElementById('accounts-content');
  if (!container) return;

  const accounts = allAccounts;

  let cardsHtml = '';
  for (const acc of accounts) {
    const avatarHtml = acc.profile_picture_url
      ? `<img class="accounts-page-avatar" src="${escapeHtml(acc.profile_picture_url)}" alt="">`
      : `<div class="accounts-page-avatar-fallback">${escapeHtml(acc.email.charAt(0).toUpperCase())}</div>`;

    cardsHtml += `
      <div class="accounts-page-card">
        ${avatarHtml}
        <div class="accounts-page-info">
          <div class="accounts-page-email">${escapeHtml(acc.email)}</div>
          <div class="accounts-page-meta">${acc.order_count} orders${acc.last_sync_at ? ` &middot; Last sync: ${acc.last_sync_at}` : ''}</div>
        </div>
        <button class="accounts-disconnect-btn" data-account-id="${acc.id}">Disconnect</button>
      </div>
    `;
  }

  container.innerHTML = `
    <div class="accounts-inner">
      <div class="accounts-page-header">
        <h2 class="accounts-page-title">Connected Accounts</h2>
        <p class="accounts-page-subtitle">${accounts.length} account${accounts.length !== 1 ? 's' : ''} connected</p>
      </div>
      <div class="accounts-page-list">
        ${cardsHtml || '<p class="accounts-page-empty">No accounts connected yet.</p>'}
      </div>
      <button class="accounts-connect-btn">+ Connect Gmail Account</button>
    </div>
  `;
}

async function connectFromTab(): Promise<void> {
  showAuthOverlay();
  try {
    await api.addAccount();
    await loadDashboard();
    renderAccountsPage();
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

async function disconnectFromTab(accountId: number): Promise<void> {
  const account = allAccounts.find(a => a.id === accountId);
  if (!account) return;
  if (!confirm(`Disconnect ${account.email}?\n\nThis will delete all synced emails and orders for this account.`)) return;

  try {
    await api.removeAccount(accountId);
    await loadDashboard();
    renderAccountsPage();
  } catch (e) {
    console.error('Failed to remove account:', e);
    alert('Failed to remove account: ' + e);
  }
}
