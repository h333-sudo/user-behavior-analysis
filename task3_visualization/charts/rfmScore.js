registerChart({
  id: "chartRFMScore",
  title: "RFM评分分布",
  tab: "user",
  dataFile: "rfm_score_distribution.json",
  render: function(data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartRFMScore = chart;
    chart.setOption({
      ...baseOpt,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' }
      },
      grid: {
        left: 60,
        right: 40,
        bottom: 40,
        top: 30
      },
      xAxis: {
        type: 'category',
        data: data.map(d => d.total_score),
        name: '总分'
      },
      yAxis: {
        type: 'value',
        name: '用户数',
        axisLabel: { formatter: v => fmtNum(v) }
      },
      series: [{
        type: 'bar',
        barWidth: '60%',
        itemStyle: {
          color: C.gold,
          borderRadius: [4, 4, 0, 0]
        },
        data: data.map(d => d.user_count)
      }]
    });
    bindResize(chart);
    return chart;
  }
});
