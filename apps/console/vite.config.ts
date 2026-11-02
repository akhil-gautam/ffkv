import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const target = env.FFKV_MANAGEMENT_URL || 'http://127.0.0.1:6381';
  return {
    plugins: [react()],
    server: {
      proxy: {
        '/api': { target, ws: true, changeOrigin: false },
        '/metrics': { target, changeOrigin: false },
      },
    },
  };
});
