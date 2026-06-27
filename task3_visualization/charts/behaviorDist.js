registerChart({
  id: "chartBehaviorDist",
  title: "用户行为分布",
  tab: "overview",
  dataFile: "behavior_distribution.json",
  render: function(data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartBehaviorDist = chart;
    const nameMap = { pv: '浏览', fav: '收藏', cart: '加购', buy: '购买' };
    const colorMap = { pv: C.sky, fav: C.gold, cart: C.teal, buy: C.sienna };
    const chartData = data.map(d => ({
      name: nameMap[d.behavior] || d.behavior,
      value: d.count,
      _behavior: d.behavior,
      itemStyle: { color: colorMap[d.behavior] || C.text }
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
        roseType: 'area',
        label: {
          show: true,
          formatter: '{b}\n{d}%'
        },
        data: chartData
      }]
    });
    chart.on('click', function(params) {
      if (params.data && params.data._behavior) {
        setBehaviorFilter(params.data._behavior);
      }
    });
    bindResize(chart);
    return chart;
  }
});
