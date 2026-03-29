import type { AccountViewModel } from '../types';
import {
  allAccounts,
  currentAccountId,
  setCurrentAccountId,
} from '../state';
import { escapeHtml } from '../utils';
import * as api from '../api';
import { loadDashboard } from './orders';
import { syncOrders } from './sync';
import { listen } from '@tauri-apps/api/event';
import { open } from '@tauri-apps/plugin-shell';

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
    // Kick off initial email sync for the newly connected account
    syncOrders().catch(err => console.error('Initial sync failed:', err));
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

      <!-- Manual fallback section -->
      <div class="auth-fallback">
        <button class="auth-fallback-toggle">
          <span class="auth-fallback-arrow">▶</span>
          Having trouble?
        </button>
        <div class="auth-fallback-content" style="display: none;">
          <p class="auth-fallback-hint">
            If the browser didn't redirect automatically, copy the URL from your browser's address bar after authorizing and paste it here:
          </p>
          <input type="text"
                 class="auth-redirect-input"
                 placeholder="http://localhost:8080/?code=..." />
          <button class="auth-submit-btn" disabled>Submit</button>
          <p class="auth-fallback-error"></p>
        </div>
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

  // Set up the manual fallback handlers
  setupAuthFallbackHandlers(overlay);

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

function setupAuthFallbackHandlers(overlay: HTMLElement): void {
  const toggle = overlay.querySelector('.auth-fallback-toggle');
  const content = overlay.querySelector('.auth-fallback-content') as HTMLElement | null;
  const input = overlay.querySelector('.auth-redirect-input') as HTMLInputElement | null;
  const submitBtn = overlay.querySelector('.auth-submit-btn') as HTMLButtonElement | null;
  const errorEl = overlay.querySelector('.auth-fallback-error') as HTMLElement | null;
  const fallbackDiv = overlay.querySelector('.auth-fallback');

  // Toggle expand/collapse
  toggle?.addEventListener('click', () => {
    if (!content) return;
    const isExpanded = content.style.display !== 'none';
    content.style.display = isExpanded ? 'none' : 'block';
    fallbackDiv?.classList.toggle('expanded', !isExpanded);
  });

  // Enable submit when valid URL entered
  input?.addEventListener('input', () => {
    if (!submitBtn) return;
    const isValid = validateRedirectUrl(input.value);
    submitBtn.disabled = !isValid;
    if (errorEl) errorEl.textContent = '';
  });

  // Handle submit
  submitBtn?.addEventListener('click', async () => {
    if (!input || !submitBtn || !errorEl) return;

    const redirectUrl = input.value.trim();
    if (!validateRedirectUrl(redirectUrl)) {
      errorEl.textContent = 'Please enter a valid redirect URL containing a code parameter.';
      return;
    }

    submitBtn.disabled = true;
    submitBtn.textContent = 'Submitting...';
    errorEl.textContent = '';

    try {
      // Cancel the automatic flow first
      await api.cancelAddAccount().catch(() => {});

      // Complete auth with the manually provided code
      const email = await api.completeAuthWithCode(redirectUrl);
      console.log('Added account via manual code:', email);

      // Success - refresh and close
      setCurrentAccountId(null);
      await loadDashboard();
      renderAccountModal();
      hideAuthOverlay();

      // Kick off initial email sync
      syncOrders().catch(err => console.error('Initial sync failed:', err));
    } catch (err) {
      console.error('Manual auth failed:', err);
      errorEl.textContent = `Error: ${err}`;
      submitBtn.disabled = false;
      submitBtn.textContent = 'Submit';
    }
  });
}

function validateRedirectUrl(url: string): boolean {
  if (!url.startsWith('http://localhost')) return false;
  try {
    const parsed = new URL(url);
    const code = parsed.searchParams.get('code');
    return code !== null && code.length > 10;
  } catch {
    return false;
  }
}

export async function showWelcomeOverlay(): Promise<void> {
  let overlay = document.getElementById('welcome-overlay');
  if (overlay) return;

  overlay = document.createElement('div');
  overlay.id = 'welcome-overlay';
  document.body.appendChild(overlay);

  // Check ONNX status to determine if VC++ step is needed
  try {
    const onnxStatus = await api.checkOnnxStatus();
    if (!onnxStatus.available) {
      showVcRedistStep(overlay, onnxStatus);
    } else {
      showGmailStep(overlay);
    }
  } catch (err) {
    console.error('Failed to check ONNX status:', err);
    // Fallback to Gmail step if check fails
    showGmailStep(overlay);
  }

  overlay.style.display = 'flex';
}

function showVcRedistStep(overlay: HTMLElement, onnxStatus: api.OnnxStatusResult): void {
  overlay.innerHTML = `
    <div class="welcome-overlay-content vcredist-step">
      <div class="welcome-icon warning-icon">
        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
          <polyline points="7 10 12 15 17 10"/>
          <line x1="12" y1="15" x2="12" y2="3"/>
        </svg>
      </div>
      <h2>Install Visual C++ Redistributable</h2>
      <p>This app requires the Visual C++ Redistributable to process product images.</p>

      <div class="vcredist-status" id="vcredist-status">
        <span class="status-dot not-installed"></span>
        <span>Not installed</span>
      </div>

      <div class="vcredist-actions">
        <button class="vcredist-download-btn" id="vcredist-download-btn">
          Download VC++ Redistributable
        </button>
      </div>

      <div class="welcome-buttons">
        <button class="welcome-continue-btn" id="vcredist-continue-btn" disabled>
          Continue
        </button>
      </div>
    </div>
  `;

  setupVcRedistEventHandlers(overlay, onnxStatus.download_url);
}

function showGmailStep(overlay: HTMLElement): void {
  overlay.innerHTML = `
    <div class="welcome-overlay-content">
      <div class="welcome-icon">
        <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
          <path d="M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z"/>
          <path d="m3.3 7 8.7 5 8.7-5"/>
          <path d="M12 22V12"/>
        </svg>
      </div>
      <h2>Welcome to Order Checker</h2>
      <p>Connect a Gmail account to start tracking your Walmart orders.</p>
      <button class="welcome-connect-btn">Connect Gmail Account</button>
    </div>
  `;

  overlay.querySelector('.welcome-connect-btn')?.addEventListener('click', () => {
    connectAccount();
  });
}

function setupVcRedistEventHandlers(overlay: HTMLElement, downloadUrl: string): void {
  const downloadBtn = document.getElementById('vcredist-download-btn');
  const continueBtn = document.getElementById('vcredist-continue-btn') as HTMLButtonElement;
  const statusEl = document.getElementById('vcredist-status');

  let checkInterval: number | null = null;

  // Download button - opens URL in browser
  downloadBtn?.addEventListener('click', async () => {
    try {
      await open(downloadUrl);
      startPolling();
    } catch (err) {
      console.error('Failed to open download URL:', err);
    }
  });

  // Continue button - proceed to Gmail step (only enabled when VC++ installed)
  continueBtn?.addEventListener('click', () => {
    stopPolling();
    showGmailStep(overlay);
  });

  function startPolling(): void {
    if (checkInterval) return;
    checkInterval = window.setInterval(async () => {
      try {
        const status = await api.checkOnnxStatus();
        if (status.available) {
          stopPolling();
          updateStatus(true);
          if (continueBtn) continueBtn.disabled = false;
        }
      } catch (err) {
        console.error('Failed to check ONNX status:', err);
      }
    }, 2000); // Check every 2 seconds
  }

  function stopPolling(): void {
    if (checkInterval) {
      clearInterval(checkInterval);
      checkInterval = null;
    }
  }

  function updateStatus(installed: boolean): void {
    if (statusEl) {
      statusEl.innerHTML = installed
        ? '<span class="status-dot installed"></span><span>Installed — Ready to continue!</span>'
        : '<span class="status-dot not-installed"></span><span>Not installed</span>';
    }
  }
}

export function hideWelcomeOverlay(): void {
  const overlay = document.getElementById('welcome-overlay');
  if (overlay) overlay.remove();
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
