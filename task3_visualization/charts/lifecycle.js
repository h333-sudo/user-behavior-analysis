registerChart({
  id: "chartLifecycle",
  title: "用户生命周期",
  tab: "user",
  dataFile: "user_lifecycle.json",
  render: function(data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartLifecycle = chart;
    const dates = data.map(d => d.date);
    const newUsers = data.map(d => d.new_users);
    const returnUsers = data.map(d => d.returning_users || d.return_users);
    const newRatio = data.map(d => d.new_user_ratio);
    chart.setOption({
      ...baseOpt,
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'cross' }
      },
      legend: {
        top: 10,
        right: 20,
        data: ['新用户', '回访用户', '新用户占比']
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
          name: '用户数',
          position: 'left',
          axisLabel: { formatter: v => fmtNum(v) }
        },
        {
          type: 'value',
          name: '占比',
          position: 'right',
          max: 100,
          axisLabel: { formatter: '{value}%' }
        }
      ],
      series: [
        {
          name: '新用户',
          type: 'bar',
          stack: 'total',
          yAxisIndex: 0,
          barWidth: '50%',
          itemStyle: {
            color: C.sienna,
            borderRadius: [0, 0, 0, 0]
          },
          data: newUsers
        },
        {
          name: '回访用户',
          type: 'bar',
          stack: 'total',
          yAxisIndex: 0,
          barWidth: '50%',
          itemStyle: {
            color: C.teal,
            borderRadius: [4, 4, 0, 0]
          },
          data: returnUsers
        },
        {
          name: '新用户占比',
          type: 'line',
          yAxisIndex: 1,
          smooth: true,
          symbol: 'circle',
          symbolSize: 6,
          lineStyle: {
            width: 2,
            type: 'dashed'
          },
          itemStyle: { color: C.gold },
          data: newRatio
        }
      ]
    });
    bindResize(chart);
    return chart;
  }
});
