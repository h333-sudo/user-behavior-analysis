// ============================================================
// 概览卡片更新
// ============================================================
function updateSummaryCards(dailyData, behaviorData) {
  const tr = dailyData.reduce((s, d) => s + (d.total_behavior || 0), 0);
  const tuv = Math.max(...dailyData.map(d => d.uv || 0));
  const tpv = dailyData.reduce((s, d) => s + (d.pv || 0), 0);
  const br = behaviorData.find(d => d.behavior === 'buy');
  const tb = br ? br.count : 0;
  document.getElementById('totalRecords').textContent = fmtNum(tr);
  document.getElementById('totalUV').textContent = fmtNum(tuv);
  document.getElementById('totalPV').textContent = fmtNum(tpv);
  document.getElementById('totalBuy').textContent = fmtNum(tb);
  document.getElementById('avgCVR').textContent = tpv > 0 ? (tb / tpv * 100).toFixed(2) + '%' : '--';
}

// ============================================================
// 搜索 & 详情面板
// ============================================================
document.getElementById('searchInput').addEventListener('keypress', e => { if (e.key === 'Enter') doSearch(); });

async function doSearch() {
  const type = document.getElementById('searchType').value;
  const val = document.getElementById('searchInput').value.trim();
  if (!val) return;
  if (type === 'item') await showItemDetail(parseInt(val));
  else if (type === 'category') await showCategoryDetail(parseInt(val));
}

function clearSearch() { document.getElementById('searchInput').value = ''; closePanel(); }

// 搜索数据缓存
let _itemDetails = null;
let _categoryItems = null;

// 加载搜索数据（从静态JSON文件）
async function loadSearchData() {
  try {
    const [items, catItems] = await Promise.all([
      getData('item_details.json'),
      getData('category_items.json')
    ]);
    _itemDetails = items || [];
    _categoryItems = catItems || {};
  } catch (e) {
    _itemDetails = [];
    _categoryItems = {};
  }
}

// 从静态数据中搜索商品
function findItemFromData(itemId) {
  if (!_itemDetails) return null;
  const found = _itemDetails.find(d => d.item_id == itemId);
  if (!found) return null;
  return {
    item_id: found.item_id,
    uv: found.pv_uv || found.total_uv || found.uv || 0,
    cvr: found.cvr || found.pv_to_buy_rate || 0,
    behavior_stats: { pv: found.pv || 0, buy: found.buy || 0, fav: found.fav || 0, cart: found.cart || 0 }
  };
}

// 从静态数据中搜索类目下的商品
function findCategoryFromData(catId) {
  if (!_categoryItems) return null;
  const items = _categoryItems[catId];
  if (!items || !items.length) return null;
  return items.slice(0, 20).map(d => ({
    item_id: d.item_id,
    pv: d.pv || 0, fav: d.fav || 0, cart: d.cart || 0, buy: d.buy || 0,
    cvr: d.cvr || d.pv_to_buy_rate || 0
  }));
}

async function showItemDetail(itemId) {
  const panel = document.getElementById('detailPanel');
  document.getElementById('panelTitle').textContent = '商品 #' + itemId;
  const content = document.getElementById('panelContent');
  content.innerHTML = '<p style="color:var(--muted)">加载中…</p>';
  panel.classList.add('open');
  // 优先尝试API，失败则从本地数据查找
  let data = await apiFetch('/api/item/' + itemId);
  if (!data) data = findItemFromData(itemId);
  if (!data) { content.innerHTML = '<p style="color:var(--red)">未找到该商品数据（仅支持已加载的商品ID）</p>'; return; }
  const bs = data.behavior_stats || {};
  content.innerHTML = `<div class="stat-grid"><div class="stat-item"><div class="label">独立用户</div><div class="value">${fmtNum(data.uv)}</div></div><div class="stat-item"><div class="label">转化率</div><div class="value">${data.cvr}%</div></div><div class="stat-item"><div class="label">浏览</div><div class="value">${fmtNum(bs.pv||0)}</div></div><div class="stat-item"><div class="label">购买</div><div class="value">${fmtNum(bs.buy||0)}</div></div><div class="stat-item"><div class="label">收藏</div><div class="value">${fmtNum(bs.fav||0)}</div></div><div class="stat-item"><div class="label">加购</div><div class="value">${fmtNum(bs.cart||0)}</div></div></div>`;
  if (data.hourly_dist && data.hourly_dist.length) {
    content.innerHTML += '<h4 style="font-family:var(--font-display);font-size:16px;margin:16px 0 8px;">按小时行为分布</h4><div class="mini-chart" id="panelChartHourly"></div>';
    const ch = echarts.init(document.getElementById('panelChartHourly'));
    ch.setOption({backgroundColor:'transparent',tooltip:{trigger:'axis'},grid:{left:40,right:10,bottom:20,top:10},xAxis:{type:'category',data:data.hourly_dist.map(d=>d.hour+':00'),axisLabel:{color:C.muted,fontSize:10},axisLine:{lineStyle:{color:C.border}}},yAxis:{axisLine:{show:false},axisLabel:{color:C.muted},splitLine:{lineStyle:{color:'#EDEBE8'}}},series:[{type:'bar',data:data.hourly_dist.map(d=>d.count),itemStyle:{color:C.sienna,borderRadius:[2,2,0,0]}}]});
  }
}

async function showCategoryDetail(catId) {
  const panel = document.getElementById('detailPanel');
  document.getElementById('panelTitle').textContent = '类目 #' + catId;
  const content = document.getElementById('panelContent');
  content.innerHTML = '<p style="color:var(--muted)">加载中…</p>';
  panel.classList.add('open');
  // 优先尝试API，失败则从本地数据查找
  let data = await apiFetch('/api/category/' + catId + '/items?limit=20');
  let items = data && data.items ? data.items : findCategoryFromData(catId);
  if (!items || !items.length) { content.innerHTML = '<p style="color:var(--red)">未找到该类目数据（仅支持已加载的类目ID）</p>'; return; }
  let html = '<p style="color:var(--muted);font-size:12px;margin-bottom:12px;font-family:var(--font-mono);">TOP ' + items.length + '</p><div style="max-height:70vh;overflow-y:auto;">';
  items.forEach((item, i) => {
    html += '<div class="category-card" onclick="showItemDetail(' + item.item_id + ')" style="margin-bottom:1px;"><div style="display:flex;justify-content:space-between;align-items:center;"><span style="color:var(--muted);font-size:12px;font-family:var(--font-mono);">#' + (i+1) + ' 商品' + item.item_id + '</span><span style="color:var(--teal);font-size:12px;font-family:var(--font-mono);">CVR ' + item.cvr + '%</span></div><div style="display:flex;gap:12px;margin-top:4px;font-size:12px;font-family:var(--font-mono);"><span>pv:' + fmtNum(item.pv) + '</span><span style="color:var(--gold);">fav:' + fmtNum(item.fav) + '</span><span style="color:var(--teal);">cart:' + fmtNum(item.cart) + '</span><span style="color:var(--sienna);">buy:' + fmtNum(item.buy) + '</span></div></div>';
  });
  html += '</div>';
  content.innerHTML = html;
}

function closePanel() { document.getElementById('detailPanel').classList.remove('open'); }
document.addEventListener('keydown', e => { if (e.key === 'Escape') { closePanel(); closeFullscreen(); } });

// ============================================================
// 用户分层画像（动态注入）
// ============================================================
let segmentChartInstance = null;

async function loadSegments() {
  const data = await apiFetch('/api/segments');
  if (!data) return;
  if (document.getElementById('segmentBtns')) return;
  const userTab = document.getElementById('tab-user');
  const dashboard = userTab.querySelector('.dashboard');
  const btnContainer = document.createElement('div');
  btnContainer.className = 'chart-card full-width';
  btnContainer.innerHTML = '<h3>用户分层画像</h3><div class="segment-btns" id="segmentBtns"></div><div id="segmentProfile" style="display:none"><div class="stat-grid" id="segStats"></div><div class="mini-chart" id="segChartHourly" style="width:100%;height:220px;"></div></div>';
  dashboard.insertBefore(btnContainer, dashboard.children[2]);
  const btnsDiv = document.getElementById('segmentBtns');
  data.forEach(seg => {
    const btn = document.createElement('button');
    btn.className = 'segment-btn';
    btn.textContent = seg.user_segment + ' (' + fmtNum(seg.user_count) + ')';
    btn.onclick = () => loadSegmentProfile(seg.user_segment, btn);
    btnsDiv.appendChild(btn);
  });
}

async function loadSegmentProfile(name, btnEl) {
  document.querySelectorAll('.segment-btn').forEach(b => b.classList.remove('active'));
  btnEl.classList.add('active');
  document.getElementById('segmentProfile').style.display = 'block';
  const statsDiv = document.getElementById('segStats');
  statsDiv.innerHTML = '<p style="color:var(--muted)">加载中…</p>';
  const data = await apiFetch('/api/segment/' + encodeURIComponent(name) + '/profile');
  if (!data) { statsDiv.innerHTML = '<p style="color:var(--red)">加载失败</p>'; return; }
  const bs = {};
  (data.behavior_dist || []).forEach(r => { bs[r.behavior] = r.count; });
  statsDiv.innerHTML = '<div class="stat-item"><div class="label">用户数</div><div class="value">' + fmtNum(data.user_count) + '</div></div><div class="stat-item"><div class="label">总行为</div><div class="value">' + fmtNum(Object.values(bs).reduce((a,b)=>a+b,0)) + '</div></div><div class="stat-item"><div class="label">浏览量</div><div class="value">' + fmtNum(bs.pv||0) + '</div></div><div class="stat-item"><div class="label">购买量</div><div class="value">' + fmtNum(bs.buy||0) + '</div></div>';
  if (data.hourly && data.hourly.length) {
    if (segmentChartInstance) segmentChartInstance.dispose();
    segmentChartInstance = echarts.init(document.getElementById('segChartHourly'));
    segmentChartInstance.setOption({backgroundColor:'transparent',tooltip:{trigger:'axis'},grid:{left:40,right:10,bottom:20,top:10},xAxis:{type:'category',data:data.hourly.map(d=>d.hour+':00'),axisLabel:{color:C.muted,fontSize:10},axisLine:{lineStyle:{color:C.border}}},yAxis:{axisLine:{show:false},axisLabel:{color:C.muted},splitLine:{lineStyle:{color:'#EDEBE8'}}},series:[{type:'bar',data:data.hourly.map(d=>d.behavior_count),itemStyle:{color:C.gold,borderRadius:[2,2,0,0]}}]});
  }
}

// ============================================================
// 主函数 — 根据注册表自动加载数据并渲染
// ============================================================
async function main() {
  if (typeof echarts === 'undefined') {
    document.querySelectorAll('.chart-container').forEach(el => { el.innerHTML = '<div class="no-echarts">ECharts 未加载，请检查网络连接</div>'; });
    return;
  }

  // 收集所有需要的数据文件
  const charts = ChartRegistry.getAll();
  const files = ChartRegistry.getDataFiles();

  // 并行加载所有数据
  const dataMap = {};
  await Promise.all(files.map(async f => {
    dataMap[f] = await getData(f);
  }));
  window._dataMap = dataMap;  // 暴露给搜索函数使用

  // 加载搜索数据（商品详情和类目商品列表）
  await loadSearchData();

  // 特殊处理：低转化数据（仅当 getData 失败时重试，避免覆盖已成功加载的数据）
  if (!dataMap['item_low_cvr.json'] || !dataMap['item_low_cvr.json'].length) {
    try { const r = await fetch(API_BASE + '/api/data/item_low_cvr.json' + buildParams()); if (r.ok) dataMap['item_low_cvr.json'] = await r.json(); } catch (e) {}
    if (!dataMap['item_low_cvr.json'] || !dataMap['item_low_cvr.json'].length) dataMap['item_low_cvr.json'] = DEMO.item_low_cvr || [];
  }

  // 数据来源提示
  const dd = dataMap['daily_uv_pv.json'] || [];
  const bd = dataMap['behavior_distribution.json'] || [];
  const hintEl = document.getElementById('dataHint');
  const isReal = dd && dd.length > 0 && dd[0].uv > 10000;
  if (isReal) { hintEl.className = 'data-hint real'; hintEl.textContent = '✓ 已加载真实分析数据'; }
  else { hintEl.className = 'data-hint demo'; hintEl.textContent = '◆ 当前展示演示数据'; }
  if (dateStart || dateEnd) { hintEl.textContent += ' | 日期: ' + (dateStart || '全部') + ' ~ ' + (dateEnd || '全部'); }

  // 概览卡片
  updateSummaryCards(dd, bd);

  // 处理行为过滤
  let filteredBD = bd;
  if (behaviorFilter) {
    const total = bd.reduce((s, d) => s + d.count, 0);
    const filteredCount = bd.find(d => d.behavior === behaviorFilter);
    if (filteredCount) filteredBD = [{ behavior: behaviorFilter, count: filteredCount.count, percentage: (filteredCount.count / total * 100).toFixed(2) }];
  }
  dataMap['behavior_distribution.json'] = filteredBD;

  // 漏斗数据：优先用 detail 版本
  const funnelData = dataMap['conversion_funnel_detail.json'] || dataMap['conversion_funnel.json'] || [];
  dataMap['_funnel_render'] = funnelData;

  // 渲染所有注册的图表
  charts.forEach(c => {
    const el = document.getElementById(c.id);
    if (!el) return;
    const chartData = c.dataFile === 'conversion_funnel.json'
      ? (dataMap['conversion_funnel_detail.json'] || dataMap['conversion_funnel.json'] || [])
      : dataMap[c.dataFile];
    if (chartData && chartData.length) {
      c.render(chartData, el);
    }
  });

  // 用户分层
  loadSegments();
}

// 启动
initDatePicker().then(main);
