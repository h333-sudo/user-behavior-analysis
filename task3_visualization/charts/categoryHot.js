// 类目热度排行榜 Top20
registerChart({
  id: "chartCategoryHot",
  title: "类目热度排行榜 Top20",
  tab: "product",
  dataFile: "category_hot_rank.json",
  render: function (data, container) {
    if (!data || !data.length) return;
    const chart = echarts.init(container);
    allCharts.chartCategoryHot = chart;
    const top20 = data.slice(0, 20).reverse();
    chart.setOption({
      ...baseOpt(),
      tooltip: { ...baseOpt().tooltip, trigger: "axis", axisPointer: { type: "shadow" } },
      legend: {
        top: 0,
        textStyle: { color: C.muted, fontSize: 11 },
        data: ["浏览", "收藏", "加购", "购买"]
      },
      grid: { left: 120, right: 40, bottom: 30, top: 40 },
      xAxis: {
        type: "value",
        axisLine: { lineStyle: { color: C.border } },
        axisLabel: { color: C.muted, fontSize: 11 },
        splitLine: { lineStyle: { color: "#EDEBE8" } }
      },
      yAxis: {
        type: "category",
        data: top20.map(function (d) { return "类目" + d.category_id; }),
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: C.muted, fontSize: 11 }
      },
      series: [
        {
          name: "浏览",
          type: "bar",
          stack: "total",
          data: top20.map(function (d) { return d.pv; }),
          itemStyle: { color: C.sky },
          barWidth: 18
        },
        {
          name: "收藏",
          type: "bar",
          stack: "total",
          data: top20.map(function (d) { return d.fav; }),
          itemStyle: { color: C.gold },
          barWidth: 18
        },
        {
          name: "加购",
          type: "bar",
          stack: "total",
          data: top20.map(function (d) { return d.cart; }),
          itemStyle: { color: C.teal },
          barWidth: 18
        },
        {
          name: "购买",
          type: "bar",
          stack: "total",
          data: top20.map(function (d) { return d.buy; }),
          itemStyle: { color: C.sienna },
          barWidth: 18
        }
      ]
    });
    bindResize(chart);
    return chart;
  }
});
