// ============================================================
// 图表注册表
// ============================================================
const ChartRegistry = {
  _charts: [],

  register(def) {
    this._charts.push(def);
  },

  getByTab(tab) {
    return this._charts.filter(c => c.tab === tab);
  },

  getAll() {
    return this._charts;
  },

  getDataFiles() {
    return [...new Set(this._charts.map(c => c.dataFile).filter(Boolean))];
  }
};

// 全局注册函数，供各 chart 文件调用
function registerChart(def) {
  ChartRegistry.register(def);
}
