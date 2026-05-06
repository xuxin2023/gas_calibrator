<template>
  <div>
    <el-page-header @back="$router.back()" style="margin-bottom:20px">
      <template #title>
        <el-select
          v-model="selectedConfig"
          size="small"
          style="width:380px"
          @change="loadConfig"
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
        <span style="font-size:20px;font-weight:700">⚙️ JSON 配置编辑器</span>
      </template>
    </el-page-header>

    <el-alert
      v-if="saveMsg"
      :title="saveMsg"
      :type="saveError ? 'error' : 'success'"
      closable
      @close="saveMsg = ''"
      style="margin-bottom:16px"
    />

    <el-card shadow="never">
      <div style="margin-bottom:12px">
        <el-input
          v-model="jsonText"
          type="textarea"
          :rows="28"
          placeholder="加载中..."
          style="font-family:'Consolas','Menlo',monospace;font-size:12px"
        />
      </div>
      <div style="display:flex;gap:10px">
        <el-button type="primary" @click="handleSave" :loading="saving">
          💾 保存 JSON
        </el-button>
        <el-button @click="handleFormat">📐 格式化</el-button>
        <el-button @click="loadConfig">🔄 重新加载</el-button>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { fetchConfig, saveConfig, fetchConfigFiles } from '@/api'

const selectedConfig = ref('run001_h2o_only_1_point_no_write_real_machine.json')
const configFiles = ref([])
const jsonText = ref('')
const saveMsg = ref('')
const saveError = ref(false)
const saving = ref(false)

async function loadConfig() {
  try {
    const data = await fetchConfig(selectedConfig.value)
    jsonText.value = JSON.stringify(data, null, 2)
    saveMsg.value = ''
  } catch (e) {
    ElMessage.error('加载配置失败: ' + e.message)
  }
}

function handleFormat() {
  try {
    const parsed = JSON.parse(jsonText.value)
    jsonText.value = JSON.stringify(parsed, null, 2)
    ElMessage.success('格式化完成')
  } catch (e) {
    ElMessage.error('JSON 格式错误，无法格式化')
  }
}

async function handleSave() {
  saving.value = true
  saveMsg.value = ''
  try {
    const parsed = JSON.parse(jsonText.value)
    await saveConfig(selectedConfig.value, parsed)
    saveMsg.value = `已保存到 ${selectedConfig.value}`
    saveError.value = false
  } catch (e) {
    saveMsg.value = e.message
    saveError.value = true
  } finally {
    saving.value = false
  }
}

onMounted(async () => {
  try {
    const cf = await fetchConfigFiles()
    configFiles.value = cf.configs || []
  } catch {
    /* ignore */
  }
  await loadConfig()
})
</script>
