<template>
  <div>
    <div style="margin-bottom:20px">
      <h2 style="margin:0;font-size:20px;font-weight:700">🎮 运行控制</h2>
      <span style="font-size:12px;color:var(--el-text-color-secondary)">
        校准运行启停与进度监控
      </span>
    </div>

    <el-alert
      v-if="initMsg"
      :title="initMsg"
      :type="initError ? 'error' : 'success'"
      closable
      @close="initMsg = ''"
      style="margin-bottom:16px"
    />

    <el-row :gutter="16" style="margin-bottom:16px">
      <el-col :span="8">
        <el-card shadow="hover">
          <div style="text-align:center">
            <el-tag
              :type="phaseTagType(runStatus.phase)"
              size="large"
              effect="dark"
              style="font-size:16px;padding:8px 24px"
            >
              {{ phaseLabel(runStatus.phase) }}
            </el-tag>
            <div style="margin-top:12px;color:var(--el-text-color-secondary);font-size:13px">
              {{ runStatus.message || '—' }}
            </div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card shadow="hover">
          <div style="text-align:center">
            <div style="font-size:32px;font-weight:700;color:var(--el-color-primary)">
              {{ runStatus.completed_points ?? 0 }} / {{ runStatus.total_points ?? 0 }}
            </div>
            <div style="font-size:12px;color:var(--el-text-color-secondary);margin-top:4px">
              已完成 / 总点数
            </div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card shadow="hover">
          <div style="text-align:center">
            <el-progress
              :percentage="runStatus.progress_pct ?? 0"
              :status="runStatus.phase === 'error' ? 'exception' : undefined"
              :stroke-width="16"
            />
            <div style="font-size:12px;color:var(--el-text-color-secondary);margin-top:8px">
              进度百分比
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never" style="margin-bottom:16px">
      <template #header><span>⚙️ 运行控制</span></template>
      <div style="display:flex;gap:10px;flex-wrap:wrap">
        <el-button
          type="primary"
          @click="handleStart"
          :loading="actionLoading"
          :disabled="runStatus.running || !serviceReady"
        >
          ▶️ 开始
        </el-button>
        <el-button
          type="warning"
          @click="handlePause"
          :loading="actionLoading"
          :disabled="!runStatus.running || runStatus.phase !== 'running'"
        >
          ⏸️ 暂停
        </el-button>
        <el-button
          type="success"
          @click="handleResume"
          :loading="actionLoading"
          :disabled="!runStatus.running || runStatus.phase !== 'paused'"
        >
          ▶️ 恢复
        </el-button>
        <el-button
          type="danger"
          @click="handleStop"
          :loading="actionLoading"
          :disabled="!runStatus.running"
        >
          ⏹️ 停止
        </el-button>
        <el-button @click="refreshStatus" :loading="statusLoading">
          🔄 刷新状态
        </el-button>
      </div>
    </el-card>

    <el-card shadow="never">
      <template #header><span>🔧 校准服务初始化</span></template>
      <el-form :inline="true" size="small">
        <el-form-item label="配置路径">
          <el-input
            v-model="initConfigPath"
            placeholder="v2/configs/validation/*.json"
            style="width:400px"
          />
        </el-form-item>
        <el-form-item label="点表路径（可选）">
          <el-input
            v-model="initPointsPath"
            placeholder="points.xlsx"
            style="width:300px"
          />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="handleInit" :loading="initLoading">
            初始化校准服务
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onUnmounted } from 'vue'
import { ElMessage } from 'element-plus'
import { initRunService, startRun, stopRun, pauseRun, resumeRun, fetchRunStatus } from '@/api'

const initConfigPath = ref('')
const initPointsPath = ref('')
const initMsg = ref('')
const initError = ref(false)
const initLoading = ref(false)
const actionLoading = ref(false)
const statusLoading = ref(false)
const serviceReady = ref(false)

const runStatus = reactive({
  running: false,
  phase: 'idle',
  current_point: null,
  total_points: 0,
  completed_points: 0,
  progress_pct: 0.0,
  message: '校准服务未初始化',
})

let pollTimer = null

function phaseTagType(phase) {
  const map = { idle: 'info', running: 'success', paused: 'warning', completed: 'success', stopped: 'info', error: 'danger' }
  return map[phase] || 'info'
}

function phaseLabel(phase) {
  const map = { idle: '空闲', running: '运行中', paused: '已暂停', completed: '已完成', stopped: '已停止', error: '错误' }
  return map[phase] || phase || '未知'
}

async function handleInit() {
  initLoading.value = true
  initMsg.value = ''
  try {
    const payload = { config_path: initConfigPath.value }
    if (initPointsPath.value) payload.points_path = initPointsPath.value
    const res = await initRunService(payload)
    initMsg.value = `校准服务已初始化，run_id=${res.run_id}，加载 ${res.points_loaded} 个点`
    initError.value = false
    serviceReady.value = true
    startPolling()
  } catch (e) {
    initMsg.value = e.message
    initError.value = true
  } finally {
    initLoading.value = false
  }
}

async function handleStart() {
  actionLoading.value = true
  try {
    await startRun()
    ElMessage.success('校准运行已启动')
    refreshStatus()
  } catch (e) {
    ElMessage.error(e.message)
  } finally {
    actionLoading.value = false
  }
}

async function handleStop() {
  actionLoading.value = true
  try {
    await stopRun()
    ElMessage.success('停止信号已发送')
    refreshStatus()
  } catch (e) {
    ElMessage.error(e.message)
  } finally {
    actionLoading.value = false
  }
}

async function handlePause() {
  actionLoading.value = true
  try {
    await pauseRun()
    ElMessage.success('已暂停')
    refreshStatus()
  } catch (e) {
    ElMessage.error(e.message)
  } finally {
    actionLoading.value = false
  }
}

async function handleResume() {
  actionLoading.value = true
  try {
    await resumeRun()
    ElMessage.success('已恢复')
    refreshStatus()
  } catch (e) {
    ElMessage.error(e.message)
  } finally {
    actionLoading.value = false
  }
}

async function refreshStatus() {
  statusLoading.value = true
  try {
    const data = await fetchRunStatus()
    Object.assign(runStatus, data)
    if (data.phase !== 'idle' && !serviceReady.value) {
      serviceReady.value = true
    }
  } catch {
    /* ignore */
  } finally {
    statusLoading.value = false
  }
}

function startPolling() {
  stopPolling()
  pollTimer = setInterval(refreshStatus, 2000)
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer)
    pollTimer = null
  }
}

onMounted(refreshStatus)
onUnmounted(stopPolling)
</script>
