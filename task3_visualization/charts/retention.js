registerChart({
  id: "chartRetention",
  title: "用户留存率",
  tab: "overview",
  dataFile: "retention_rate.json",
  render: function(data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartRetention = chart;
    const dates = data.map(d => d.first_date || d.date);
    chart.setOption({
      ...baseOpt,
      tooltip: {
        trigger: 'axis',
        formatter: function(params) {
          let s = params[0].axisValue + '<br/>';
          params.forEach(p => {
            s += p.marker + p.seriesName + ': ' + p.value + '%<br/>';
          });
          return s;
        }
      },
      legend: {
        top: 10,
        right: 20,
        data: ['次日留存', '3日留存', '7日留存']
      },
      grid: {
        left: 60,
        right: 40,
        bottom: 40,
        top: 50
      },
      xAxis: {
        type: 'category',
        data: dates,
        axisLabel: { rotate: 30 }
      },
      yAxis: {
        type: 'value',
        max: 100,
        axisLabel: { formatter: '{value}%' }
      },
      series: [
        {
          name: '次日留存',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 6,
          lineStyle: { width: 2 },
          itemStyle: { color: C.sienna },
          data: data.map(d => d.retention_rate_day1 || d.day1_retention)
        },
        {
          name: '3日留存',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 6,
          lineStyle: { width: 2 },
          itemStyle: { color: C.gold },
          data: data.map(d => d.retention_rate_day3 || d.day3_retention)
        },
        {
          name: '7日留存',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 6,
          lineStyle: { width: 2 },
          itemStyle: { color: C.teal },
          data: data.map(d => d.retention_rate_day7 || d.day7_retention)
        }
      ]
    });
    bindResize(chart);
    return chart;
  }
});
