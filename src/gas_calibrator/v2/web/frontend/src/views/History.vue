<template>
  <div>
    <div style="margin-bottom:20px">
      <h2 style="margin:0;font-size:20px;font-weight:700">📜 历史记录</h2>
      <span style="font-size:12px;color:var(--el-text-color-secondary)">
        校准运行历史与结果归档
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
      :data="runs"
      stripe
      size="small"
      v-loading="loading"
      empty-text="暂无历史记录"
      highlight-current-row
      @row-click="showDetail"
      style="cursor:pointer"
    >
      <el-table-column prop="run_id" label="运行 ID" width="180" show-overflow-tooltip />
      <el-table-column prop="start_time" label="开始时间" width="180">
        <template #default="{ row }">
          {{ formatTime(row.start_time) }}
        </template>
      </el-table-column>
      <el-table-column prop="status" label="状态" width="100">
        <template #default="{ row }">
          <el-tag
            :type="statusTagType(row.status)"
            size="small"
            effect="plain"
          >
            {{ statusLabel(row.status) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="点数" width="100">
        <template #default="{ row }">
          {{ row.successful_points }}/{{ row.total_points }}
        </template>
      </el-table-column>
      <el-table-column prop="route_mode" label="路线模式" width="110" />
      <el-table-column prop="operator" label="操作员" min-width="120" />
    </el-table>

    <el-dialog
      v-model="detailVisible"
      :title="'运行详情: ' + detailRunId"
      width="720px"
      destroy-on-close
    >
      <el-descriptions v-if="detail" :column="2" border size="small" style="margin-bottom:16px">
        <el-descriptions-item label="状态">
          <el-tag :type="statusTagType(detail.status)" size="small">
            {{ statusLabel(detail.status) }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="路线模式">{{ detail.route_mode || '—' }}</el-descriptions-item>
        <el-descriptions-item label="总点数">{{ detail.total_points }}</el-descriptions-item>
        <el-descriptions-item label="成功点数">{{ detail.successful_points }}</el-descriptions-item>
        <el-descriptions-item label="开始时间">{{ formatTime(detail.start_time) }}</el-descriptions-item>
        <el-descriptions-item label="结束时间">{{ formatTime(detail.end_time) }}</el-descriptions-item>
        <el-descriptions-item label="操作员">{{ detail.operator || '—' }}</el-descriptions-item>
        <el-descriptions-item label="软件版本">{{ detail.software_version || '—' }}</el-descriptions-item>
      </el-descriptions>

      <h4 v-if="detail?.points?.length" style="margin:12px 0 8px">📊 采样点</h4>
      <el-table
        v-if="detail?.points?.length"
        :data="detail.points"
        stripe
        size="small"
        max-height="300"
      >
        <el-table-column prop="point_id" label="点 ID" width="200" show-overflow-tooltip />
        <el-table-column prop="sample_count" label="样本数" width="80" />
        <el-table-column label="CO₂ 均值 (ppm)" width="130">
          <template #default="{ row }">
            {{ row.co2_avg ?? '—' }}
          </template>
        </el-table-column>
        <el-table-column label="分析仪" min-width="150">
          <template #default="{ row }">
            <el-tag
              v-for="aid in row.analyzer_ids"
              :key="aid"
              size="small"
              type="info"
              effect="plain"
              style="margin:2px"
            >
              {{ aid }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
      <el-empty v-else description="该运行无采样点数据" :image-size="60" />

      <div style="margin-top:16px;text-align:right" v-if="detail">
        <el-button type="primary" @click="downloadBundle(detailRunId)" size="small">
          📥 下载证据包
        </el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { fetchHistory, fetchRunDetail } from '@/api'

const runs = ref([])
const loading = ref(false)
const errorMsg = ref('')
const detailVisible = ref(false)
const detail = ref(null)
const detailRunId = ref('')

function statusTagType(status) {
  const map = { completed: 'success', running: 'warning', failed: 'danger', aborted: 'info' }
  return map[status] || 'info'
}

function statusLabel(status) {
  const map = { completed: '已完成', running: '运行中', failed: '失败', aborted: '已中止' }
  return map[status] || status || '未知'
}

function formatTime(ts) {
  if (!ts) return '—'
  try {
    return new Date(ts).toLocaleString('zh-CN')
  } catch {
    return ts
  }
}

async function loadHistory() {
  loading.value = true
  errorMsg.value = ''
  try {
    const data = await fetchHistory(100)
    runs.value = data.runs || []
    if (data.note) errorMsg.value = data.note
  } catch (e) {
    errorMsg.value = e.message
  } finally {
    loading.value = false
  }
}

async function showDetail(row) {
  detailRunId.value = row.run_id
  detailVisible.value = true
  detail.value = null
  try {
    const data = await fetchRunDetail(row.run_id)
    detail.value = data
  } catch (e) {
    ElMessage.error('加载运行详情失败: ' + e.message)
  }
}

function downloadBundle(runId) {
  const url = `/api/runs/${encodeURIComponent(runId)}/bundle`
  const a = document.createElement('a')
  a.href = url
  a.download = `run_${runId}_evidence_bundle.zip`
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  ElMessage.success('证据包下载已开始')
}

onMounted(loadHistory)
</script>
