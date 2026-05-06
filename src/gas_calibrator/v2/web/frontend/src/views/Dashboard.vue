<template>
  <div>
    <el-page-header @back="$router.back()" style="margin-bottom:20px">
      <template #title>
        <el-select
          v-model="selectedConfig"
          placeholder="选择配置文件"
          size="small"
          style="width:380px"
          @change="loadOverview"
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
        <span style="font-size:20px;font-weight:700">📋 仪表盘</span>
      </template>
    </el-page-header>

    <el-row :gutter="16" style="margin-bottom:16px">
      <el-col :xs="12" :sm="6">
        <StatCard :value="overview.device_count" label="已启用设备" icon="🖥️" />
      </el-col>
      <el-col :xs="12" :sm="6">
        <StatCard :value="overview.analyzer_count" label="气体分析仪" icon="📡" />
      </el-col>
      <el-col :xs="12" :sm="6">
        <StatCard :value="overview.run_mode || '—'" label="运行模式" icon="⚙️" />
      </el-col>
      <el-col :xs="12" :sm="6">
        <StatCard :value="overview.route_mode || '—'" label="路线模式" icon="🗺️" />
      </el-col>
    </el-row>

    <el-card shadow="never" style="margin-bottom:16px">
      <template #header><span>🖥️ 已启用设备</span></template>
      <el-row :gutter="12">
        <el-col
          v-for="name in overview.device_names"
          :key="name"
          :xs="12"
          :sm="8"
          :md="6"
        >
          <el-tag type="primary" effect="plain" style="margin:4px;font-size:13px">
            {{ name }}
          </el-tag>
        </el-col>
      </el-row>
      <el-empty v-if="!overview.device_names?.length" description="暂无已启用设备" />
    </el-card>

    <el-card shadow="never">
      <template #header><span>📈 配置场景信息</span></template>
      <el-descriptions :column="2" border size="small">
        <el-descriptions-item label="场景">{{
          overview.scenario || '—'
        }}</el-descriptions-item>
        <el-descriptions-item label="模式">{{
          overview.mode || '—'
        }}</el-descriptions-item>
        <el-descriptions-item label="温度组">{{
          (overview.selected_temps_c || []).join(', ') || '—'
        }} °C</el-descriptions-item>
        <el-descriptions-item label="配置文件">{{
          overview.config_file
        }}</el-descriptions-item>
      </el-descriptions>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { fetchOverview, fetchConfigFiles } from '@/api'
import StatCard from '@/components/StatCard.vue'

const selectedConfig = ref('run001_h2o_only_1_point_no_write_real_machine.json')
const configFiles = ref([])
const overview = ref({})

async function loadOverview() {
  try {
    overview.value = await fetchOverview(selectedConfig.value)
  } catch {
    /* handled by interceptor */
  }
}

onMounted(async () => {
  try {
    const res = await fetchConfigFiles()
    configFiles.value = res.configs || []
  } catch {
    /* ignore */
  }
  await loadOverview()
})
</script>
