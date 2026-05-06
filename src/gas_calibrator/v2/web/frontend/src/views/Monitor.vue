<template>
  <div>
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px">
      <div>
        <h2 style="margin:0;font-size:20px">📡 实时监控</h2>
        <span style="font-size:12px;color:var(--el-text-color-secondary)">
          WebSocket {{ wsStatus }} · 数据刷新间隔 2s
        </span>
      </div>
      <el-tag :type="wsStatus === '已连接' ? 'success' : 'danger'" effect="dark">
        {{ wsStatus }}
      </el-tag>
    </div>

    <el-row :gutter="16" style="margin-bottom:16px">
      <el-col :xs="12" :sm="6">
        <el-card shadow="hover">
          <div class="monitor-value">{{ telemetry.pressure_hpa }}</div>
          <div class="monitor-label">压力 (hPa)</div>
        </el-card>
      </el-col>
      <el-col :xs="12" :sm="6">
        <el-card shadow="hover">
          <div class="monitor-value">{{ telemetry.temperature_c }}</div>
          <div class="monitor-label">温度 (°C)</div>
        </el-card>
      </el-col>
      <el-col :xs="12" :sm="6">
        <el-card shadow="hover">
          <div class="monitor-value">{{ telemetry.humidity_pct }}</div>
          <div class="monitor-label">湿度 (%)</div>
        </el-card>
      </el-col>
      <el-col :xs="12" :sm="6">
        <el-card shadow="hover">
          <div class="monitor-value">{{ telemetry.co2_ppm }}</div>
          <div class="monitor-label">CO₂ (ppm)</div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16" style="margin-bottom:16px">
      <el-col :span="12">
        <el-card shadow="never">
          <template #header><span>📈 压力曲线</span></template>
          <v-chart :option="pressureChartOption" style="height:280px" autoresize />
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card shadow="never">
          <template #header><span>🌡️ 温度 & 湿度</span></template>
          <v-chart :option="tempHumChartOption" style="height:280px" autoresize />
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never">
      <template #header><span>📊 实时数据日志</span></template>
      <el-table :data="logEntries" size="small" max-height="300" stripe>
        <el-table-column prop="ts" label="时间" width="180" />
        <el-table-column prop="pressure_hpa" label="压力 (hPa)" width="110" />
        <el-table-column prop="temperature_c" label="温度 (°C)" width="100" />
        <el-table-column prop="humidity_pct" label="湿度 (%)" width="100" />
        <el-table-column prop="co2_ppm" label="CO₂ (ppm)" width="100" />
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onUnmounted } from 'vue'
import VChart from 'vue-echarts'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
  TitleComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'

use([
  LineChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  TitleComponent,
  CanvasRenderer,
])

const wsStatus = ref('连接中...')
const MAX_LOG = 120
const logEntries = ref([])
const pressureHistory = ref([])
const tempHistory = ref([])
const humHistory = ref([])

const telemetry = reactive({
  pressure_hpa: '—',
  temperature_c: '—',
  humidity_pct: '—',
  co2_ppm: '—',
  dewpoint_c: '—',
})

let ws = null
let reconnectTimer = null

function connectWS() {
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:'
  const url = `${protocol}//${location.host}/ws/monitor`
  ws = new WebSocket(url)

  ws.onopen = () => {
    wsStatus.value = '已连接'
    if (reconnectTimer) {
      clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
  }

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data)
      if (msg.type === 'telemetry') {
        Object.assign(telemetry, {
          pressure_hpa: msg.pressure_hpa,
          temperature_c: msg.temperature_c,
          humidity_pct: msg.humidity_pct,
          co2_ppm: msg.co2_ppm,
          dewpoint_c: msg.dewpoint_c,
        })
        const ts = new Date(msg.ts).toLocaleTimeString('zh-CN')
        pressureHistory.value.push({ ts, value: msg.pressure_hpa })
        tempHistory.value.push({ ts, value: msg.temperature_c })
        humHistory.value.push({ ts, value: msg.humidity_pct })
        if (pressureHistory.value.length > MAX_LOG) pressureHistory.value.shift()
        if (tempHistory.value.length > MAX_LOG) tempHistory.value.shift()
        if (humHistory.value.length > MAX_LOG) humHistory.value.shift()

        logEntries.value.unshift({ ...msg, ts })
        if (logEntries.value.length > 100) logEntries.value.pop()
      }
    } catch {
      /* ignore */
    }
  }

  ws.onclose = () => {
    wsStatus.value = '已断开'
    reconnectTimer = setTimeout(connectWS, 5000)
  }

  ws.onerror = () => {
    wsStatus.value = '连接错误'
  }
}

const pressureChartOption = computed(() => ({
  tooltip: { trigger: 'axis' },
  xAxis: { type: 'category', data: pressureHistory.value.map((p) => p.ts) },
  yAxis: { type: 'value', name: 'hPa' },
  series: [
    {
      data: pressureHistory.value.map((p) => p.value),
      type: 'line',
      smooth: true,
      areaStyle: { opacity: 0.1 },
      itemStyle: { color: '#5470c6' },
    },
  ],
}))

const tempHumChartOption = computed(() => ({
  tooltip: { trigger: 'axis' },
  legend: { data: ['温度', '湿度'] },
  xAxis: { type: 'category', data: tempHistory.value.map((p) => p.ts) },
  yAxis: [
    { type: 'value', name: '°C' },
    { type: 'value', name: '%' },
  ],
  series: [
    {
      name: '温度',
      data: tempHistory.value.map((p) => p.value),
      type: 'line',
      smooth: true,
      itemStyle: { color: '#ee6666' },
    },
    {
      name: '湿度',
      data: humHistory.value.map((p) => p.value),
      type: 'line',
      smooth: true,
      yAxisIndex: 1,
      itemStyle: { color: '#91cc75' },
    },
  ],
}))

onMounted(connectWS)
onUnmounted(() => {
  if (ws) ws.close()
  if (reconnectTimer) clearTimeout(reconnectTimer)
})
</script>

<style scoped>
.monitor-value {
  font-size: 28px;
  font-weight: 700;
  color: var(--el-color-primary);
}
.monitor-label {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-top: 4px;
}
</style>
