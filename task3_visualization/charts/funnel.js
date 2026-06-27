// 转化漏斗
registerChart({
  id: "chartFunnel",
  title: "转化漏斗",
  tab: "funnel",
  dataFile: "conversion_funnel.json",
  render: function (data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartFunnel = chart;
    var maxVal = Math.max.apply(null, data.map(function (d) { return d.user_count || d.count || 0; }));
    chart.setOption({
      ...baseOpt(),
      tooltip: {
        ...baseOpt().tooltip,
        trigger: "item",
        formatter: function (p) {
          return p.name + ": " + fmtNum(p.value) + " 人 (" + p.percent.toFixed(1) + "%)";
        }
      },
      series: [{
        type: "funnel",
        left: "10%",
        top: 20,
        bottom: 20,
        width: "80%",
        min: 0,
        max: maxVal,
        minSize: "10%",
        maxSize: "100%",
        sort: "descending",
        gap: 4,
        label: {
          show: true,
          position: "inside",
          formatter: function (p) {
            return p.name + "\n" + fmtNum(p.value) + " 人 " + p.percent.toFixed(1) + "%";
          },
          fontSize: 12,
          color: "#fff",
          lineHeight: 18
        },
        labelLine: { show: false },
        data: data.map(function (d, i) {
          return {
            name: d.stage || d.name,
            value: d.user_count || d.count || 0,
            itemStyle: { color: CHART_COLORS[i % CHART_COLORS.length] }
          };
        })
      }]
    });
    bindResize(chart);
    return chart;
  }
});
