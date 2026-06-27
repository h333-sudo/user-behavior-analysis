registerChart({
  id: "chartFunnelBar",
  title: "转化漏斗各阶段",
  tab: "funnel",
  dataFile: "conversion_funnel.json",
  render: function (data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartFunnelBar = chart;

    const stages = data.map(function (d) { return d.stage; }).reverse();
    const values = data.map(function (d) { return d.user_count || d.count || 0; }).reverse();

    chart.setOption(
      Object.assign(baseOpt(), {
        tooltip: Object.assign(baseOpt().tooltip, {
          trigger: "axis",
          axisPointer: { type: "shadow" },
          formatter: function (p) {
            var v = p[0];
            return v.name + "<br/>数量: " + fmtNum(v.value);
          }
        }),
        grid: { left: 120, right: 80, top: 30, bottom: 30 },
        xAxis: {
          type: "value",
          axisLine: { lineStyle: { color: C.border } },
          axisLabel: { color: C.muted, fontSize: 11 },
          splitLine: { lineStyle: { color: "#EDEBE8" } }
        },
        yAxis: {
          type: "category",
          data: stages,
          axisLine: { lineStyle: { color: C.border } },
          axisLabel: { color: C.muted, fontSize: 12 },
          axisTick: { show: false }
        },
        series: [{
          type: "bar",
          data: values.map(function (v, i) {
            return {
              value: v,
              itemStyle: { color: CHART_COLORS[i % CHART_COLORS.length] },
              label: {
                show: true,
                position: "right",
                formatter: function (p) { return fmtNum(p.value); },
                fontSize: 11,
                color: C.muted
              }
            };
          }),
          barWidth: "55%"
        }]
      })
    );

    bindResize(chart);
    return chart;
  }
});
