<template>
  <div>
    <el-page-header @back="$router.back()" style="margin-bottom:20px">
      <template #title>
        <el-select
          v-model="selectedConfig"
          size="small"
          style="width:380px"
          @change="loadParams"
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
        <span style="font-size:20px;font-weight:700">🎛️ 参数配置</span>
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

    <el-collapse v-model="activeCollapse" style="margin-bottom:16px">
      <el-collapse-item
        v-for="group in groups"
        :key="group.id"
        :name="group.id"
      >
        <template #title>
          <span style="font-weight:600;font-size:15px">
            {{ group.icon }} {{ group.label }}
          </span>
        </template>

        <el-form label-width="140px" label-position="left" size="default">
          <el-form-item
            v-for="param in group.params"
            :key="param.key"
            :label="param.label"
          >
            <template #label>
              <div>
                <span style="font-weight:600">{{ param.label }}</span>
                <span style="font-size:11px;color:var(--el-text-color-secondary);margin-left:6px;font-family:monospace">
                  {{ param.key }}
                </span>
              </div>
            </template>
            <el-input
              v-model="formValues[group.id][param.key]"
              style="width:200px"
              size="default"
            >
              <template #suffix>
                <span style="color:var(--el-text-color-secondary);font-size:12px">
                  {{ param.unit }}
                </span>
              </template>
            </el-input>
            <el-tooltip
              :content="'V1 默认值: ' + param.v1_default + ' ' + param.unit"
              placement="top"
              effect="light"
            >
              <el-tag type="info" size="small" effect="plain" style="margin-left:10px;cursor:help">
                默认 {{ param.v1_default }}
              </el-tag>
            </el-tooltip>
          </el-form-item>
        </el-form>
      </el-collapse-item>
    </el-collapse>

    <div style="display:flex;gap:10px">
      <el-button type="primary" @click="handleSave" :loading="saving" size="large">
        💾 保存参数
      </el-button>
      <el-button @click="loadParams" size="large">🔄 重新加载</el-button>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { fetchParams, saveParams, fetchConfigFiles } from '@/api'

const selectedConfig = ref('run001_h2o_only_1_point_no_write_real_machine.json')
const configFiles = ref([])
const groups = ref([])
const formValues = reactive({})
const activeCollapse = ref([])
const saveMsg = ref('')
const saveError = ref(false)
const saving = ref(false)

async function loadParams() {
  try {
    const data = await fetchParams(selectedConfig.value)
    groups.value = data.groups || []

    const names = groups.value.map((g) => g.id)
    activeCollapse.value = names

    for (const group of groups.value) {
      if (!formValues[group.id]) {
        formValues[group.id] = {}
      }
      for (const param of group.params) {
        formValues[group.id][param.key] = String(param.current_value ?? param.v1_default ?? '')
      }
    }
    saveMsg.value = ''
  } catch (e) {
    ElMessage.error('加载参数失败: ' + e.message)
  }
}

async function handleSave() {
  saving.value = true
  saveMsg.value = ''
  try {
    const payload = {}
    for (const group of groups.value) {
      payload[group.id] = {}
      for (const param of group.params) {
        const raw = formValues[group.id]?.[param.key] ?? ''
        payload[group.id][param.key] = raw
      }
    }
    await saveParams(selectedConfig.value, payload)
    saveMsg.value = `参数已保存到 ${selectedConfig.value}`
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
  await loadParams()
})
</script>
