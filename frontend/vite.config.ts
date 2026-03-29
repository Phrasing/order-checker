import { defineConfig } from 'vite';

export default defineConfig({
  root: 'frontend',
  server: {
    port: 1420,
    strictPort: true,
    host: '127.0.0.1',
  },
  clearScreen: false,
  envPrefix: ['VITE_', 'TAURI_'],
  build: {
    target: 'es2021',
    minify: !process.env.TAURI_DEBUG ? 'esbuild' : false,
    sourcemap: !!process.env.TAURI_DEBUG,
    outDir: 'dist',
    emptyOutDir: true,
  },
});
