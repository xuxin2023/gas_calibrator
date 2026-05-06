import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 15000,
  headers: { 'Content-Type': 'application/json' },
})

api.interceptors.response.use(
  (res) => res.data,
  (err) => {
    const msg = err.response?.data?.detail || err.message || '请求失败'
    console.error('[API]', msg)
    return Promise.reject(new Error(msg))
  },
)

export function fetchOverview(filename) {
  return api.get('/overview', { params: { filename } })
}

export function fetchConfigFiles() {
  return api.get('/configs')
}

export function fetchConfig(filename) {
  return api.get(`/configs/${filename}`)
}

export function saveConfig(filename, data) {
  return api.put(`/configs/${filename}`, data)
}

export function fetchStatus(filename) {
  return api.get('/status', { params: { filename } })
}

export function fetchParams(filename) {
  return api.get('/params', { params: { filename } })
}

export function saveParams(filename, data) {
  return api.put('/params', data, { params: { filename } })
}

export function fetchHistory(limit = 50) {
  return api.get('/history', { params: { limit } })
}

export function fetchSimSnapshot() {
  return api.get('/simulation/snapshot')
}

export function fetchRunDetail(runId) {
  return api.get(`/runs/${runId}`)
}

export function fetchDevicesLive() {
  return api.get('/devices/live')
}

export function fetchAnalyzersLive() {
  return api.get('/analyzers/live')
}

export function fetchRunStatus() {
  return api.get('/run/status')
}

export function initRunService(payload) {
  return api.post('/run/init', payload)
}

export function startRun(payload) {
  return api.post('/run/start', payload || {})
}

export function stopRun() {
  return api.post('/run/stop')
}

export function pauseRun() {
  return api.post('/run/pause')
}

export function resumeRun() {
  return api.post('/run/resume')
}

export default api
