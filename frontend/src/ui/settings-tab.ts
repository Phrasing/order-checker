import { fetchSinceDate, setFetchSinceDate } from '../state';
import { onTabActivate } from './tabs';

let checkForNewEmails: (() => Promise<void>) | null = null;

export function bindSettingsDeps(deps: { checkForNewEmails: () => Promise<void> }): void {
  checkForNewEmails = deps.checkForNewEmails;
}

export function setupSettingsTab(): void {
  onTabActivate('settings', renderSettingsPage);
}

function renderSettingsPage(): void {
  const container = document.getElementById('settings-content');
  if (!container) return;

  const today = new Date().toISOString().split('T')[0];
  const currentVal = fetchSinceDate || '';
  const hintText = currentVal ? getDaysAgoText(currentVal) : 'Default: last 5 days';

  container.innerHTML = `
    <div class="settings-header">
      <h2 class="settings-title">Settings</h2>
    </div>
    <div class="settings-sections">
      <div class="settings-section">
        <h3 class="settings-section-title">Email Sync</h3>
        <div class="settings-field">
          <label class="settings-label" for="settings-fetch-since">Fetch Emails Since</label>
          <p class="settings-description">Controls how far back the Gmail sync will search for Walmart order emails.</p>
          <input type="date" id="settings-fetch-since" class="settings-date-input"
                 value="${currentVal}" max="${today}"
                 title="Sync will fetch emails back to this date">
          <div id="settings-fetch-hint" class="settings-hint">${hintText}</div>
        </div>
      </div>
      <div class="settings-section">
        <h3 class="settings-section-title">About</h3>
        <div class="settings-about">
          <p>Walmart Order Tracker</p>
          <p class="settings-about-secondary">Tracks Walmart orders from Gmail using the Gmail API.</p>
        </div>
      </div>
    </div>
  `;

  // Bind date input change
  const input = document.getElementById('settings-fetch-since') as HTMLInputElement | null;
  const hint = document.getElementById('settings-fetch-hint');
  if (input && hint) {
    input.addEventListener('change', () => {
      const val = input.value;
      if (val) {
        setFetchSinceDate(val);
        localStorage.setItem('fetchSinceDate', val);
      } else {
        setFetchSinceDate(null);
        localStorage.removeItem('fetchSinceDate');
      }
      hint.textContent = val ? getDaysAgoText(val) : 'Default: last 5 days';
      checkForNewEmails?.();
    });
  }
}

function getDaysAgoText(dateStr: string): string {
  const selected = new Date(dateStr + 'T00:00:00');
  const now = new Date();
  const diffDays = Math.round((now.getTime() - selected.getTime()) / (1000 * 60 * 60 * 24));
  if (diffDays > 365) return `${diffDays} days ago — this may take a while`;
  if (diffDays > 90) return `${diffDays} days ago — larger sync`;
  return `${diffDays} days of emails`;
}
