<template>
  <div>
    <el-page-header @back="$router.back()" style="margin-bottom:20px">
      <template #title>
        <el-select
          v-model="selectedConfig"
          size="small"
          style="width:380px"
          @change="loadData"
        >
          <el-option
            v-for="c in configFiles"
            :key="c.name"
            :label="c.name"
            :value="c.name"
          />
        </el-select>
      </template>
      <template #content>
        <span style="font-size:20px;font-weight:700">📡 设备状态</span>
      </template>
    </el-page-header>

    <el-row :gutter="16" style="margin-bottom:16px">
      <el-col :xs="12" :sm="6">
        <StatCard :value="devices.length" label="设备总数" icon="🖥️" />
      </el-col>
      <el-col :xs="12" :sm="6">
        <StatCard :value="enabledCount" label="已启用" icon="✅" />
      </el-col>
    </el-row>

    <el-card shadow="never">
      <template #header><span>🔌 设备列表</span></template>
      <el-table :data="devices" stripe size="small" empty-text="暂无设备数据">
        <el-table-column prop="name" label="设备名称" width="200">
          <template #default="{ row }">
            <strong>{{ row.name }}</strong>
          </template>
        </el-table-column>
        <el-table-column prop="port" label="串口" width="120">
          <template #default="{ row }">
            <el-tag
              v-if="row.port && row.port !== '—'"
              type="info"
              size="small"
              effect="plain"
            >
              {{ row.port }}
            </el-tag>
            <span v-else>—</span>
          </template>
        </el-table-column>
        <el-table-column prop="baud" label="波特率" width="100" />
        <el-table-column prop="enabled" label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.enabled ? 'success' : 'info'" size="small">
              {{ row.enabled ? '已启用' : '已禁用' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="description" label="说明" />
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { fetchStatus, fetchConfigFiles } from '@/api'
import StatCard from '@/components/StatCard.vue'

const selectedConfig = ref('run001_h2o_only_1_point_no_write_real_machine.json')
const configFiles = ref([])
const devices = ref([])

const enabledCount = computed(() => devices.value.filter((d) => d.enabled).length)

async function loadData() {
  try {
    const res = await fetchStatus(selectedConfig.value)
    devices.value = res.devices || []
  } catch {
    /* ignore */
  }
}

onMounted(async () => {
  try {
    const cf = await fetchConfigFiles()
    configFiles.value = cf.configs || []
  } catch {
    /* ignore */
  }
  await loadData()
})
</script>
