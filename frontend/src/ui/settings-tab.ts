import { onTabActivate } from './tabs';
import { getTheme, setTheme, type Theme } from './theme';
import * as api from '../api';
import { loadDashboard } from './orders';

export function setupSettingsTab(): void {
  onTabActivate('settings', renderSettingsPage);
}

function renderSettingsPage(): void {
  const container = document.getElementById('settings-content');
  if (!container) return;

  const theme = getTheme();
  const autoRefresh = localStorage.getItem('autoRefreshTracking') !== 'false';

  container.innerHTML = `
    <div class="settings-inner">
      <div class="settings-header">
        <h2 class="settings-title">Settings</h2>
      </div>
      <div class="settings-sections">
        <div class="settings-section">
          <h3 class="settings-section-title">Appearance</h3>
          <div class="settings-field">
            <label class="settings-label">Theme</label>
            <div class="theme-toggle">
              <button class="theme-option${theme === 'dark' ? ' active' : ''}" data-theme="dark">Dark</button>
              <button class="theme-option${theme === 'light' ? ' active' : ''}" data-theme="light">Light</button>
            </div>
          </div>
        </div>
        <div class="settings-section">
          <h3 class="settings-section-title">Behavior</h3>
          <div class="settings-field">
            <label class="settings-label">Auto-refresh tracking on launch</label>
            <p class="settings-description">Update tracking info for shipped orders when the app starts.</p>
            <div class="theme-toggle">
              <button class="theme-option auto-refresh-option${autoRefresh ? ' active' : ''}" data-value="true">On</button>
              <button class="theme-option auto-refresh-option${!autoRefresh ? ' active' : ''}" data-value="false">Off</button>
            </div>
          </div>
        </div>
        <div class="settings-section settings-danger-zone">
          <h3 class="settings-section-title">Data Management</h3>
          <div class="settings-field">
            <label class="settings-label">Clear All Data</label>
            <p class="settings-description">Delete all orders, emails, and tracking data. You'll need to sync again to restore.</p>
            <button class="settings-danger-btn" id="clear-all-data-btn">Clear Orders & Emails</button>
          </div>
        </div>
      </div>
    </div>
  `;

  // Theme toggle
  container.querySelectorAll<HTMLButtonElement>('.theme-option:not(.auto-refresh-option)').forEach(btn => {
    btn.addEventListener('click', () => {
      const newTheme = btn.dataset.theme as Theme;
      setTheme(newTheme);
      container.querySelectorAll('.theme-option:not(.auto-refresh-option)').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
    });
  });

  // Auto-refresh tracking toggle
  container.querySelectorAll<HTMLButtonElement>('.auto-refresh-option').forEach(btn => {
    btn.addEventListener('click', () => {
      const value = btn.dataset.value!;
      if (value === 'true') {
        localStorage.removeItem('autoRefreshTracking');
      } else {
        localStorage.setItem('autoRefreshTracking', 'false');
      }
      container.querySelectorAll('.auto-refresh-option').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
    });
  });

  // Clear all data button
  const clearBtn = document.getElementById('clear-all-data-btn');
  clearBtn?.addEventListener('click', async () => {
    if (!confirm('Are you sure? This will delete all orders and emails. You will need to sync again.')) {
      return;
    }
    try {
      clearBtn.textContent = 'Clearing...';
      (clearBtn as HTMLButtonElement).disabled = true;
      const result = await api.clearAllData();
      alert(`Cleared ${result.orders_cleared} orders and ${result.emails_cleared} emails.`);
      await loadDashboard();
    } catch (err) {
      alert('Failed to clear data: ' + err);
    } finally {
      clearBtn.textContent = 'Clear Orders & Emails';
      (clearBtn as HTMLButtonElement).disabled = false;
    }
  });
}
