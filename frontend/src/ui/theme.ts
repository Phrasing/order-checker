export type Theme = 'dark' | 'light';

function getSystemTheme(): Theme {
  return matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}

export function getTheme(): Theme {
  return (localStorage.getItem('theme') as Theme) || getSystemTheme();
}

export function setTheme(theme: Theme): void {
  localStorage.setItem('theme', theme);
  document.documentElement.setAttribute('data-theme', theme);
}

export function setupTheme(): void {
  // Apply (in case inline script didn't run or was skipped in dev)
  document.documentElement.setAttribute('data-theme', getTheme());

  // Listen for OS preference changes (only applies when no explicit user choice)
  matchMedia('(prefers-color-scheme: light)').addEventListener('change', () => {
    if (!localStorage.getItem('theme')) {
      document.documentElement.setAttribute('data-theme', getSystemTheme());
    }
  });
}
