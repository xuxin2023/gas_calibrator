<template>
  <el-card shadow="hover" :body-style="{ padding: '20px 24px' }">
    <div style="display:flex;align-items:flex-start;justify-content:space-between">
      <div>
        <div style="font-size:28px;font-weight:700;color:var(--el-color-primary);line-height:1.2">
          {{ value }}
        </div>
        <div style="font-size:12px;color:var(--el-text-color-secondary);margin-top:4px">
          {{ label }}
        </div>
      </div>
      <span style="font-size:28px;opacity:0.25">{{ icon }}</span>
    </div>
  </el-card>
</template>

<script setup>
defineProps({
  value: { type: [String, Number], default: '—' },
  label: { type: String, default: '' },
  icon: { type: String, default: '' },
})
</script>
