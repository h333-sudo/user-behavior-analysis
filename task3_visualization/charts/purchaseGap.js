registerChart({
  id: "chartPurchaseGap",
  title: "首次购买间隔分布",
  tab: "user",
  dataFile: "first_purchase_gap.json",
  render: function(data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartPurchaseGap = chart;
    const chartData = data.map(d => ({
      name: d.gap_group || d.gap_range,
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
            return params.name + '\n' + params.percent + '%';
          }
        },
        data: chartData
      }]
    });
    bindResize(chart);
    return chart;
  }
});
