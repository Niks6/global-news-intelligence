import axios from 'axios'

// In Docker production (Nginx proxies /api/ to backend), use relative URL.
// In local Vite dev mode, fall back to http://localhost:8000.
const API_URL = import.meta.env.VITE_API_URL ?? (import.meta.env.PROD ? '' : 'http://localhost:8000')

const client = axios.create({
  baseURL: API_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
})

client.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response) {
      console.error('[API Error]', error.response.status, error.response.data)
    } else if (error.request) {
      console.error('[API Error] No response received', error.request)
    } else {
      console.error('[API Error]', error.message)
    }
    return Promise.reject(error)
  }
)

export default client
