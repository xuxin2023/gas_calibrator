<template>
  <div>
    <div style="margin-bottom:20px">
      <h2 style="margin:0;font-size:20px;font-weight:700">📡 分析仪实时数据</h2>
      <span style="font-size:12px;color:var(--el-text-color-secondary)">
        Mode2 帧实时读取 · {{ analyzers.length }} 台 · 刷新间隔 {{ pollInterval / 1000 }}s
      </span>
    </div>

    <el-alert
      v-if="errorMsg"
      :title="errorMsg"
      type="warning"
      closable
      @close="errorMsg = ''"
      style="margin-bottom:16px"
    />

    <el-table
      :data="analyzers"
      stripe
      size="small"
      v-loading="loading"
      empty-text="暂无分析仪数据（请先初始化校准服务）"
    >
      <el-table-column prop="label" label="分析仪" width="100">
        <template #default="{ row }">
          <el-tag :type="row.online ? 'success' : 'danger'" size="small" effect="dark">
            {{ row.label }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="CO₂ (ppm)" width="120" align="right">
        <template #default="{ row }">
          <span :style="{ color: row.co2_ppm != null ? 'var(--el-color-primary)' : 'var(--el-text-color-placeholder)', fontWeight: row.co2_ppm != null ? 700 : 400 }">
            {{ row.co2_ppm != null ? Number(row.co2_ppm).toFixed(2) : '—' }}
          </span>
        </template>
      </el-table-column>
      <el-table-column label="H₂O (mmol)" width="110" align="right">
        <template #default="{ row }">
          <span :style="{ color: row.h2o_mmol != null ? 'var(--el-color-success)' : 'var(--el-text-color-placeholder)', fontWeight: row.h2o_mmol != null ? 700 : 400 }">
            {{ row.h2o_mmol != null ? Number(row.h2o_mmol).toFixed(3) : '—' }}
          </span>
        </template>
      </el-table-column>
      <el-table-column label="CO₂ 比值" width="100" align="right">
        <template #default="{ row }">
          {{ row.co2_ratio_f != null ? Number(row.co2_ratio_f).toFixed(5) : '—' }}
        </template>
      </el-table-column>
      <el-table-column label="H₂O 比值" width="100" align="right">
        <template #default="{ row }">
          {{ row.h2o_ratio_f != null ? Number(row.h2o_ratio_f).toFixed(5) : '—' }}
        </template>
      </el-table-column>
      <el-table-column label="腔温 (°C)" width="100" align="right">
        <template #default="{ row }">
          {{ row.chamber_temp_c != null ? Number(row.chamber_temp_c).toFixed(2) : '—' }}
        </template>
      </el-table-column>
      <el-table-column label="压力 (kPa)" width="100" align="right">
        <template #default="{ row }">
          {{ row.pressure_kpa != null ? Number(row.pressure_kpa).toFixed(2) : '—' }}
        </template>
      </el-table-column>
      <el-table-column label="CO₂信号" width="100" align="right">
        <template #default="{ row }">
          {{ row.co2_signal != null ? Number(row.co2_signal).toFixed(1) : '—' }}
        </template>
      </el-table-column>
      <el-table-column label="H₂O信号" width="100" align="right">
        <template #default="{ row }">
          {{ row.h2o_signal != null ? Number(row.h2o_signal).toFixed(1) : '—' }}
        </template>
      </el-table-column>
    </el-table>

    <div style="margin-top:12px;display:flex;gap:10px">
      <el-button @click="refresh" :loading="loading" size="small">🔄 立即刷新</el-button>
      <el-switch v-model="autoPoll" active-text="自动刷新" @change="toggleAutoPoll" size="small" />
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { fetchAnalyzersLive } from '@/api'

const analyzers = ref([])
const loading = ref(false)
const errorMsg = ref('')
const autoPoll = ref(true)
const pollInterval = 3000
let pollTimer = null

async function refresh() {
  loading.value = true
  errorMsg.value = ''
  try {
    const data = await fetchAnalyzersLive()
    analyzers.value = data.analyzers || []
    if (data.note) errorMsg.value = data.note
  } catch (e) {
    errorMsg.value = e.message
  } finally {
    loading.value = false
  }
}

function toggleAutoPoll(val) {
  if (val) startPolling()
  else stopPolling()
}

function startPolling() {
  stopPolling()
  pollTimer = setInterval(refresh, pollInterval)
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer)
    pollTimer = null
  }
}

onMounted(() => {
  refresh()
  if (autoPoll.value) startPolling()
})
onUnmounted(stopPolling)
</script>
