import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8080',
      '/connect': 'http://localhost:8080',
      '/users': 'http://localhost:8080',
      '/stream': 'http://localhost:8080',
      '/ping': 'http://localhost:8080',
    },
  },
})
