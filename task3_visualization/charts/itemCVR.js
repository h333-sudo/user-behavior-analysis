// 商品转化率 Top15 (柱状+折线双轴)
registerChart({
  id: "chartItemCVR",
  title: "商品转化率 Top15",
  tab: "product",
  dataFile: "item_cvr_top.json",
  render: function (data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartItemCVR = chart;
    const top15 = data.slice(0, 15);
    chart.setOption({
      ...baseOpt(),
      tooltip: { ...baseOpt().tooltip, trigger: "axis", axisPointer: { type: "shadow" } },
      legend: {
        top: 0,
        textStyle: { color: C.muted, fontSize: 11 },
        data: ["购买数", "转化率"]
      },
      grid: { left: 60, right: 60, bottom: 80, top: 40 },
      xAxis: {
        type: "category",
        data: top15.map(function (d) { return d.item_id; }),
        axisLine: { lineStyle: { color: C.border } },
        axisLabel: { color: C.muted, fontSize: 11, rotate: 45 }
      },
      yAxis: [
        {
          type: "value",
          name: "购买数",
          nameTextStyle: { color: C.muted, fontSize: 11 },
          axisLine: { lineStyle: { color: C.border } },
          axisLabel: { color: C.muted, fontSize: 11 },
          splitLine: { lineStyle: { color: "#EDEBE8" } }
        },
        {
          type: "value",
          name: "转化率",
          nameTextStyle: { color: C.muted, fontSize: 11 },
          axisLine: { lineStyle: { color: C.border } },
          axisLabel: {
            color: C.muted,
            fontSize: 11,
            formatter: function (v) { return (v * 100).toFixed(1) + "%"; }
          },
          splitLine: { show: false }
        }
      ],
      series: [
        {
          name: "购买数",
          type: "bar",
          data: top15.map(function (d) { return d.buy || d.buy_count || 0; }),
          itemStyle: { color: C.teal, borderRadius: [3, 3, 0, 0] },
          barWidth: 24
        },
        {
          name: "转化率",
          type: "line",
          yAxisIndex: 1,
          data: top15.map(function (d) { return d.pv_to_buy_rate || d.cvr || 0; }),
          itemStyle: { color: C.sienna },
          lineStyle: { color: C.sienna, width: 2 },
          symbol: "circle",
          symbolSize: 6
        }
      ]
    });
    bindResize(chart);
    return chart;
  }
});
