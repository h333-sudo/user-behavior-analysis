// ============================================================
// 全局常量
// ============================================================
const C = {sienna:'#C65D3E',teal:'#1B6B5A',gold:'#B8860B',plum:'#6B3A5D',sky:'#3B7EA1',red:'#B91C1C',muted:'#57534E',border:'#D6D3D1',cream:'#F8F5F0',charcoal:'#1C1917'};
const CHART_COLORS = [C.sienna, C.teal, C.gold, C.plum, C.sky, C.red];
const API_BASE = window.location.protocol + '//' + window.location.hostname + ':5000';

// 全局状态
let dateStart = null, dateEnd = null;
let behaviorFilter = null;
let allCharts = {};

// ============================================================
// 工具函数
// ============================================================
function fmtNum(n) { return Number(n).toLocaleString(); }

function bindResize(chart) {
  window.addEventListener('resize', () => chart.resize());
}

function baseOpt() {
  return {
    backgroundColor: 'transparent',
    textStyle: { color: C.muted, fontFamily: "'Source Serif 4',Georgia,serif" },
    tooltip: { backgroundColor: '#fff', borderColor: C.border, textStyle: { color: C.charcoal, fontFamily: "'Source Serif 4',serif" }, extraCssText: 'box-shadow:0 4px 20px rgba(0,0,0,0.08);border-radius:0;' },
    grid: { left: 50, right: 30, bottom: 30, top: 45, containLabel: false },
    xAxis: { axisLine: { lineStyle: { color: C.border } }, axisLabel: { color: C.muted, fontSize: 11 }, splitLine: { lineStyle: { color: '#EDEBE8' } } },
    yAxis: { axisLine: { show: false }, axisLabel: { color: C.muted, fontSize: 11 }, splitLine: { lineStyle: { color: '#EDEBE8' } } }
  };
}

function switchTab(name, btn) {
  document.querySelectorAll('.tab-section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  btn.classList.add('active');
  setTimeout(() => window.dispatchEvent(new Event('resize')), 100);
}

// ============================================================
// API 数据加载
// ============================================================
function buildParams() {
  const p = [];
  if (dateStart) p.push('date_start=' + dateStart);
  if (dateEnd) p.push('date_end=' + dateEnd);
  return p.length ? '?' + p.join('&') : '';
}

async function getData(filename) {
  try {
    const resp = await fetch(API_BASE + '/api/data/' + filename + buildParams());
    if (resp.ok) { const d = await resp.json(); if (d && (!Array.isArray(d) || d.length > 0)) return d; }
  } catch (e) {}
  return DEMO[filename.replace('.json', '')] || [];
}

async function apiFetch(endpoint) {
  try {
    const sep = endpoint.includes('?') ? '&' : '?';
    const resp = await fetch(API_BASE + endpoint + (dateStart || dateEnd ? sep + buildParams().substring(1) : ''));
    if (resp.ok) return await resp.json();
  } catch (e) {}
  return null;
}

// ============================================================
// 日期筛选
// ============================================================
async function initDatePicker() {
  let dates;
  try { const r = await fetch(API_BASE + '/api/dates'); if (r.ok) dates = await r.json(); } catch (e) {}
  if (!dates || !dates.length) dates = ['2017-11-25','2017-11-26','2017-11-27','2017-11-28','2017-11-29','2017-11-30','2017-12-01','2017-12-02','2017-12-03'];
  const ds = document.getElementById('dateStart'), de = document.getElementById('dateEnd');
  dates.forEach(d => { ds.innerHTML += `<option value="${d}">${d}</option>`; de.innerHTML += `<option value="${d}">${d}</option>`; });
  ds.value = dates[0]; de.value = dates[dates.length - 1];
}

function applyDateFilter() {
  dateStart = document.getElementById('dateStart').value;
  dateEnd = document.getElementById('dateEnd').value;
  main();
}

function resetDateFilter() {
  const ds = document.getElementById('dateStart'), de = document.getElementById('dateEnd');
  dateStart = ds.options[0].value; dateEnd = de.options[de.options.length - 1].value;
  ds.value = dateStart; de.value = dateEnd;
  main();
}

// ============================================================
// 饼图联动过滤
// ============================================================
function setBehaviorFilter(behavior) {
  if (behaviorFilter === behavior) { behaviorFilter = null; } else { behaviorFilter = behavior; }
  updateFilterIndicator();
  main();
}

function clearBehaviorFilter() { behaviorFilter = null; updateFilterIndicator(); main(); }

function updateFilterIndicator() {
  const el = document.getElementById('filterIndicator');
  if (behaviorFilter) { el.classList.add('active'); document.getElementById('filterTag').textContent = behaviorFilter; }
  else { el.classList.remove('active'); }
}

// ============================================================
// 全屏模式
// ============================================================
let fsChartInstance = null;

function goFullscreen(title, containerId) {
  const overlay = document.getElementById('fsOverlay');
  document.getElementById('fsTitle').textContent = title;
  overlay.classList.add('active');
  setTimeout(() => {
    const container = document.getElementById('fsChart');
    if (fsChartInstance) fsChartInstance.dispose();
    fsChartInstance = echarts.init(container);
    const srcChart = allCharts[containerId];
    if (srcChart) fsChartInstance.setOption(srcChart.getOption());
    window.addEventListener('resize', () => { if (fsChartInstance) fsChartInstance.resize(); });
  }, 50);
}

function closeFullscreen() {
  document.getElementById('fsOverlay').classList.remove('active');
  if (fsChartInstance) { fsChartInstance.dispose(); fsChartInstance = null; }
}

document.addEventListener('keydown', e => { if (e.key === 'Escape') closeFullscreen(); });
