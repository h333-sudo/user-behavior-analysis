// 用户行为路径 Top15
registerChart({
  id: "chartBehaviorPath",
  title: "用户行为路径 Top15",
  tab: "user",
  dataFile: "behavior_paths.json",
  render: function (data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartBehaviorPath = chart;
    const top15 = data.slice(0, 15).reverse();
    chart.setOption({
      ...baseOpt(),
      tooltip: { ...baseOpt().tooltip, trigger: "axis", axisPointer: { type: "shadow" } },
      grid: { left: 200, right: 80, bottom: 30, top: 20 },
      xAxis: {
        type: "value",
        axisLine: { lineStyle: { color: C.border } },
        axisLabel: { color: C.muted, fontSize: 11 },
        splitLine: { lineStyle: { color: "#EDEBE8" } }
      },
      yAxis: {
        type: "category",
        data: top15.map(function (d) { return d.path_segment; }),
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: C.muted, fontSize: 11 }
      },
      series: [{
        type: "bar",
        data: top15.map(function (d) { return d.path_count || d.user_count || 0; }),
        itemStyle: { color: C.sienna, borderRadius: [0, 3, 3, 0] },
        label: {
          show: true,
          position: "right",
          formatter: function (p) { return fmtNum(p.value); },
          fontSize: 11,
          color: C.muted
        },
        barWidth: 18
      }]
    });
    bindResize(chart);
    return chart;
  }
});
