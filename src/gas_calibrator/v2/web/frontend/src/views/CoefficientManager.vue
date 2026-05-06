<template>
  <div>
    <div style="margin-bottom:20px">
      <h2 style="margin:0;font-size:20px;font-weight:700">📐 系数管理</h2>
      <span style="font-size:12px;color:var(--el-text-color-secondary)">
        分析仪校准系数版本历史 · 审批与部署
      </span>
    </div>

    <el-row :gutter="16" style="margin-bottom:16px">
      <el-col :span="8">
        <el-select
          v-model="selectedSensor"
          placeholder="选择分析仪"
          style="width:100%"
          @change="loadVersions"
          filterable
        >
          <el-option
            v-for="s in sensors"
            :key="s.sensor_id"
            :label="s.device_key || s.analyzer_id || s.sensor_id"
            :value="s.sensor_id"
          />
        </el-select>
      </el-col>
      <el-col :span="4">
        <el-button @click="loadSensors" :loading="sensorLoading" size="default">
          🔄 刷新列表
        </el-button>
      </el-col>
    </el-row>

    <el-alert
      v-if="errorMsg"
      :title="errorMsg"
      type="warning"
      closable
      @close="errorMsg = ''"
      style="margin-bottom:16px"
    />

    <el-table
      :data="versions"
      stripe
      size="small"
      v-loading="versionLoading"
      empty-text="请先选择分析仪"
    >
      <el-table-column prop="version" label="版本号" width="80" />
      <el-table-column prop="analyzer_id" label="分析仪ID" width="100" />
      <el-table-column label="系数" min-width="200">
        <template #default="{ row }">
          <div v-if="row.coefficients && Object.keys(row.coefficients).length">
            <el-tag
              v-for="(val, key) in row.coefficients"
              :key="key"
              size="small"
              type="info"
              effect="plain"
              style="margin:2px"
            >
              {{ key }}={{ typeof val === 'number' ? val.toFixed(6) : val }}
            </el-tag>
          </div>
          <span v-else style="color:var(--el-text-color-placeholder)">—</span>
        </template>
      </el-table-column>
      <el-table-column prop="created_at" label="创建时间" width="170">
        <template #default="{ row }">
          {{ formatTime(row.created_at) }}
        </template>
      </el-table-column>
      <el-table-column prop="created_by" label="创建者" width="100" />
      <el-table-column label="已审批" width="80" align="center">
        <template #default="{ row }">
          <el-tag :type="row.approved ? 'success' : 'info'" size="small" effect="plain">
            {{ row.approved ? '是' : '否' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="已部署" width="80" align="center">
        <template #default="{ row }">
          <el-tag :type="row.deployed ? 'success' : 'info'" size="small" effect="plain">
            {{ row.deployed ? '是' : '否' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="160" fixed="right">
        <template #default="{ row }">
          <el-button
            v-if="!row.approved"
            type="success"
            size="small"
            :loading="actionLoading === row.id"
            @click="handleApprove(row)"
          >
            审批
          </el-button>
          <el-button
            v-if="row.approved && !row.deployed"
            type="primary"
            size="small"
            :loading="actionLoading === row.id"
            @click="handleDeploy(row)"
          >
            部署
          </el-button>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { fetchSensors, fetchSensorCoefficients, approveCoefficient, deployCoefficient } from '@/api'

const sensors = ref([])
const versions = ref([])
const selectedSensor = ref('')
const sensorLoading = ref(false)
const versionLoading = ref(false)
const errorMsg = ref('')
const actionLoading = ref(null)

function formatTime(ts) {
  if (!ts) return '—'
  try {
    return new Date(ts).toLocaleString('zh-CN')
  } catch {
    return ts
  }
}

async function loadSensors() {
  sensorLoading.value = true
  errorMsg.value = ''
  try {
    const data = await fetchSensors()
    sensors.value = data.sensors || []
  } catch (e) {
    errorMsg.value = e.message
  } finally {
    sensorLoading.value = false
  }
}

async function loadVersions() {
  if (!selectedSensor.value) {
    versions.value = []
    return
  }
  versionLoading.value = true
  errorMsg.value = ''
  try {
    const data = await fetchSensorCoefficients(selectedSensor.value)
    versions.value = data.versions || []
  } catch (e) {
    errorMsg.value = e.message
  } finally {
    versionLoading.value = false
  }
}

async function handleApprove(row) {
  actionLoading.value = row.id
  try {
    await approveCoefficient(selectedSensor.value, row.id)
    ElMessage.success('审批成功')
    loadVersions()
  } catch (e) {
    ElMessage.error(e.message)
  } finally {
    actionLoading.value = null
  }
}

async function handleDeploy(row) {
  actionLoading.value = row.id
  try {
    await deployCoefficient(selectedSensor.value, row.id)
    ElMessage.success('部署成功')
    loadVersions()
  } catch (e) {
    ElMessage.error(e.message)
  } finally {
    actionLoading.value = null
  }
}

onMounted(loadSensors)
</script>
