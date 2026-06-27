registerChart({
  id: "chartHVHourly",
  title: "高价值用户活跃时段",
  tab: "user",
  dataFile: "hv_user_hourly.json",
  render: function(data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartHVHourly = chart;
    chart.setOption({
      ...baseOpt(),
      tooltip: { trigger: 'axis' },
      grid: { left: 50, right: 20, bottom: 30, top: 20 },
      xAxis: {
        type: 'category',
        data: data.map(d => d.hour + ':00'),
        axisLabel: { color: C.muted, fontSize: 11, interval: 1 },
        axisLine: { lineStyle: { color: C.border } }
      },
      yAxis: {
        type: 'value',
        name: '行为次数',
        axisLabel: { color: C.muted },
        splitLine: { lineStyle: { color: '#EDEBE8' } }
      },
      series: [{
        type: 'bar',
        data: data.map(d => d.behavior_count),
        itemStyle: { color: C.sienna, borderRadius: [3, 3, 0, 0] },
        barWidth: '50%'
      }]
    });
    bindResize(chart);
    return chart;
  }
});
