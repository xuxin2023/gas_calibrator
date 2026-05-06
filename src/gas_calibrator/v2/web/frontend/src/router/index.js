import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    name: 'Dashboard',
    component: () => import('@/views/Dashboard.vue'),
  },
  {
    path: '/monitor',
    name: 'Monitor',
    component: () => import('@/views/Monitor.vue'),
  },
  {
    path: '/status',
    name: 'Status',
    component: () => import('@/views/Status.vue'),
  },
  {
    path: '/history',
    name: 'History',
    component: () => import('@/views/History.vue'),
  },
  {
    path: '/config',
    name: 'ConfigEditor',
    component: () => import('@/views/ConfigEditor.vue'),
  },
  {
    path: '/params',
    name: 'ParamsEditor',
    component: () => import('@/views/ParamsEditor.vue'),
  },
  {
    path: '/run',
    name: 'RunControl',
    component: () => import('@/views/RunControl.vue'),
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

export default router
