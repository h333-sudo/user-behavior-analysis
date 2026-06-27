registerChart({
  id: "chartRFM",
  title: "RFM用户分群",
  tab: "user",
  dataFile: "rfm_segments.json",
  render: function(data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartRFM = chart;
    const chartData = data.map(d => ({
      name: d.user_segment || d.segment,
      value: d.user_count
    }));
    chart.setOption({
      ...baseOpt,
      tooltip: {
        trigger: 'item',
        formatter: '{b}: {c} ({d}%)'
      },
      legend: {
        orient: 'vertical',
        right: 20,
        top: 'center'
      },
      series: [{
        type: 'pie',
        radius: ['40%', '70%'],
        center: ['40%', '50%'],
        label: {
          show: true,
          formatter: function(params) {
            return params.name + '\n' + fmtNum(params.value) + '人';
          }
        },
        data: chartData
      }]
    });
    bindResize(chart);
    return chart;
  }
});
