// 用户活跃天数分布
registerChart({
  id: "chartActiveDays",
  title: "用户活跃天数分布",
  tab: "user",
  dataFile: "active_days_distribution.json",
  render: function (data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartActiveDays = chart;
    const sorted = data.slice().reverse();
    chart.setOption({
      ...baseOpt(),
      tooltip: { ...baseOpt().tooltip, trigger: "axis", axisPointer: { type: "shadow" } },
      grid: { left: 120, right: 80, bottom: 30, top: 20 },
      xAxis: {
        type: "value",
        axisLine: { lineStyle: { color: C.border } },
        axisLabel: { color: C.muted, fontSize: 11 },
        splitLine: { lineStyle: { color: "#EDEBE8" } }
      },
      yAxis: {
        type: "category",
        data: sorted.map(function (d) { return d.active_group; }),
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: C.muted, fontSize: 11 }
      },
      series: [{
        type: "bar",
        data: sorted.map(function (d) { return d.user_count; }),
        itemStyle: { color: C.teal, borderRadius: [0, 3, 3, 0] },
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
