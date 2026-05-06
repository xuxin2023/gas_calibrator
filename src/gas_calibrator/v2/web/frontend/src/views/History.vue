<template>
  <div>
    <div style="margin-bottom:20px">
      <h2 style="margin:0;font-size:20px;font-weight:700">📜 历史记录</h2>
      <span style="font-size:12px;color:var(--el-text-color-secondary)">
        校准运行历史与结果归档
      </span>
    </div>

    <el-empty description="暂无历史记录" :image-size="120">
      <template #description>
        <div style="margin-top:8px">
          <p style="color:var(--el-text-color-secondary);font-size:13px">
            运行校准任务后，历史记录将在此处显示
          </p>
          <p style="color:var(--el-text-color-placeholder);font-size:12px;margin-top:4px">
            （历史记录模块待数据库集成后可用）
          </p>
        </div>
      </template>
    </el-empty>
  </div>
</template>
