import { getCurrentWindow } from '@tauri-apps/api/window';

const appWindow = getCurrentWindow();

const MAXIMIZE_ICON = `<svg width="10" height="10" viewBox="0 0 10 10">
    <rect x="0.5" y="0.5" width="9" height="9" fill="none" stroke="currentColor" stroke-width="1"/>
</svg>`;

const RESTORE_ICON = `<svg width="10" height="10" viewBox="0 0 10 10">
    <rect x="2.5" y="0.5" width="7" height="7" fill="none" stroke="currentColor" stroke-width="1"/>
    <rect x="0.5" y="2.5" width="7" height="7" fill="var(--bg-secondary)" stroke="currentColor" stroke-width="1"/>
</svg>`;

async function updateMaximizeIcon(): Promise<void> {
    const btn = document.getElementById('btn-maximize');
    if (!btn) return;
    const maximized = await appWindow.isMaximized();
    btn.innerHTML = maximized ? RESTORE_ICON : MAXIMIZE_ICON;
    btn.setAttribute('aria-label', maximized ? 'Restore' : 'Maximize');
}

export function setupTitlebar(): void {
    document.getElementById('btn-minimize')?.addEventListener('click', () => {
        appWindow.minimize();
    });

    document.getElementById('btn-maximize')?.addEventListener('click', async () => {
        await appWindow.toggleMaximize();
        setTimeout(() => updateMaximizeIcon(), 50);
    });

    document.getElementById('btn-close')?.addEventListener('click', () => {
        appWindow.close();
    });

    // Catch maximize state changes from system shortcuts (Win+Up), snap layouts, etc.
    window.addEventListener('resize', () => updateMaximizeIcon());

    updateMaximizeIcon();
}
