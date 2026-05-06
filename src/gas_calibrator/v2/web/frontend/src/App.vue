<template>
  <el-container style="min-height:100vh">
    <el-aside width="220px" style="background:var(--el-color-primary);overflow:hidden">
      <div style="padding:20px 16px;display:flex;align-items:center;gap:8px">
        <span style="font-size:24px">🔬</span>
        <div>
          <div style="color:#fff;font-weight:700;font-size:15px">气体校准 V2</div>
          <div style="color:rgba(255,255,255,0.65);font-size:11px">自动校准与数据分析</div>
        </div>
      </div>
      <el-menu
        :default-active="currentRoute"
        router
        background-color="transparent"
        text-color="rgba(255,255,255,0.75)"
        active-text-color="#fff"
        style="border-right:none"
      >
        <el-menu-item index="/">
          <el-icon><DataBoard /></el-icon>
          <span>仪表盘</span>
        </el-menu-item>
        <el-menu-item index="/monitor">
          <el-icon><Monitor /></el-icon>
          <span>实时监控</span>
        </el-menu-item>
        <el-menu-item index="/status">
          <el-icon><Connection /></el-icon>
          <span>设备状态</span>
        </el-menu-item>
        <el-menu-item index="/analyzers">
          <el-icon><Cpu /></el-icon>
          <span>分析仪数据</span>
        </el-menu-item>
        <el-menu-item index="/history">
          <el-icon><Clock /></el-icon>
          <span>历史记录</span>
        </el-menu-item>
        <el-menu-item index="/config">
          <el-icon><Setting /></el-icon>
          <span>JSON 配置</span>
        </el-menu-item>
        <el-menu-item index="/params">
          <el-icon><Operation /></el-icon>
          <span>参数配置</span>
        </el-menu-item>
        <el-menu-item index="/run">
          <el-icon><VideoPlay /></el-icon>
          <span>运行控制</span>
        </el-menu-item>
      </el-menu>
    </el-aside>
    <el-main style="background:#f5f7fa;padding:24px 32px">
      <router-view />
    </el-main>
  </el-container>
</template>

<script setup>
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { DataBoard, Monitor, Connection, Cpu, Clock, Setting, Operation, VideoPlay } from '@element-plus/icons-vue'

const route = useRoute()
const currentRoute = computed(() => route.path)
</script>
