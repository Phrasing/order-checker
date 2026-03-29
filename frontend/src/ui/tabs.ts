export type TabId = 'orders' | 'analytics' | 'accounts' | 'settings';

const activateCallbacks = new Map<TabId, Array<() => void>>();
let currentTab: TabId = 'orders';

export function getActiveTab(): TabId {
  return currentTab;
}

/** Register a callback that fires when a tab becomes active. */
export function onTabActivate(tabId: TabId, callback: () => void): void {
  const list = activateCallbacks.get(tabId) || [];
  list.push(callback);
  activateCallbacks.set(tabId, list);
}

/** Switch the active tab (updates DOM + fires callbacks). */
export function switchTab(tabId: TabId): void {
  if (tabId === currentTab) return;
  currentTab = tabId;

  // Update tab buttons
  document.querySelectorAll<HTMLElement>('.tab-item').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tabId);
  });

  // Update settings gear button active state
  document.getElementById('settings-btn')?.classList.toggle('active', tabId === 'settings');

  // Update panels
  document.querySelectorAll<HTMLElement>('.tab-panel').forEach(panel => {
    panel.classList.toggle('active', panel.id === `panel-${tabId}`);
  });

  // Slide indicator
  updateIndicator();

  // Fire callbacks
  const cbs = activateCallbacks.get(tabId);
  if (cbs) cbs.forEach(cb => cb());
}

function updateIndicator(): void {
  const bar = document.getElementById('tab-bar');
  const indicator = bar?.querySelector<HTMLElement>('.tab-bar-indicator');
  if (!indicator || !bar) return;

  const activeBtn = bar.querySelector<HTMLElement>('.tab-item.active');
  if (!activeBtn) {
    // No tab-item is active (e.g. settings via gear button) — hide indicator
    indicator.style.width = '0';
    return;
  }

  const barRect = bar.getBoundingClientRect();
  const btnRect = activeBtn.getBoundingClientRect();
  indicator.style.left = `${btnRect.left - barRect.left}px`;
  indicator.style.width = `${btnRect.width}px`;
}

/** Initialize tab bar: attach click listener via event delegation. */
export function setupTabs(): void {
  const bar = document.getElementById('tab-bar');
  if (!bar) return;

  bar.addEventListener('click', (e) => {
    const btn = (e.target as HTMLElement).closest<HTMLElement>('.tab-item');
    if (!btn) return;
    const tabId = btn.dataset.tab as TabId | undefined;
    if (tabId) switchTab(tabId);
  });

  // Position indicator on initial tab
  requestAnimationFrame(() => updateIndicator());

  // Reposition on resize
  window.addEventListener('resize', () => updateIndicator());
}
