registerChart({
  id: "chartDailyUVPV",
  title: "每日UV/PV趋势",
  tab: "overview",
  dataFile: "daily_uv_pv.json",
  render: function(data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartDailyUVPV = chart;
    const dates = data.map(d => d.date);
    const uvData = data.map(d => d.uv);
    const pvData = data.map(d => d.pv);
    chart.setOption({
      ...baseOpt,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' }
      },
      legend: {
        top: 10,
        right: 20,
        data: ['UV', 'PV']
      },
      grid: {
        left: 60,
        right: 60,
        bottom: 40,
        top: 50
      },
      xAxis: {
        type: 'category',
        data: dates,
        axisLabel: { rotate: 30 }
      },
      yAxis: [
        {
          type: 'value',
          name: 'UV',
          position: 'left',
          axisLabel: { formatter: v => fmtNum(v) }
        },
        {
          type: 'value',
          name: 'PV',
          position: 'right',
          axisLabel: { formatter: v => fmtNum(v) }
        }
      ],
      series: [
        {
          name: 'UV',
          type: 'line',
          yAxisIndex: 0,
          smooth: true,
          symbol: 'circle',
          symbolSize: 6,
          lineStyle: { width: 2 },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: C.sky },
              { offset: 1, color: 'rgba(70,130,180,0.05)' }
            ])
          },
          itemStyle: { color: C.sky },
          data: uvData
        },
        {
          name: 'PV',
          type: 'line',
          yAxisIndex: 1,
          smooth: true,
          symbol: 'circle',
          symbolSize: 6,
          lineStyle: { width: 2 },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: C.gold },
              { offset: 1, color: 'rgba(218,165,32,0.05)' }
            ])
          },
          itemStyle: { color: C.gold },
          data: pvData
        }
      ]
    });
    bindResize(chart);
    return chart;
  }
});
