registerChart({
  id: "chartDailyFunnelTrend",
  title: "每日转化漏斗趋势",
  tab: "time",
  dataFile: "daily_funnel_trend.json",
  render: function (data, container) {
    if (!data || !data.length) return;
    var chart = echarts.init(container);
    allCharts.chartDailyFunnelTrend = chart;

    var dates = data.map(function (d) { return d.date || d.label; });
    var browseToInterest = data.map(function (d) { return d.pv_to_interest_rate || d.browse_to_interest || d.view_to_interest || 0; });
    var interestToBuy = data.map(function (d) { return d.interest_to_buy_rate || d.interest_to_buy || d.interest_to_purchase || 0; });
    var overallCVR = data.map(function (d) { return d.overall_cvr || d.overall_conversion || 0; });

    chart.setOption(
      Object.assign(baseOpt(), {
        tooltip: Object.assign(baseOpt().tooltip, {
          trigger: "axis",
          formatter: function (params) {
            var s = params[0].axisValue + "<br/>";
            params.forEach(function (p) {
              s += p.marker + " " + p.seriesName + ": " + p.value.toFixed(2) + "%<br/>";
            });
            return s;
          }
        }),
        legend: {
          data: ["浏览→兴趣", "兴趣→购买", "整体转化率"],
          top: 5,
          textStyle: { color: C.muted, fontSize: 11 }
        },
        grid: { left: 60, right: 30, top: 45, bottom: 30 },
        xAxis: {
          type: "category",
          data: dates,
          boundaryGap: false,
          axisLine: { lineStyle: { color: C.border } },
          axisLabel: { color: C.muted, fontSize: 11 },
          axisTick: { alignWithLabel: true }
        },
        yAxis: {
          type: "value",
          name: "转化率(%)",
          nameTextStyle: { color: C.muted, fontSize: 11 },
          axisLine: { show: false },
          axisLabel: {
            color: C.muted,
            fontSize: 11,
            formatter: function (v) { return v.toFixed(1) + "%"; }
          },
          splitLine: { lineStyle: { color: "#EDEBE8" } }
        },
        series: [
          {
            name: "浏览→兴趣",
            type: "line",
            data: browseToInterest,
            itemStyle: { color: C.sienna },
            lineStyle: { color: C.sienna, width: 2 },
            symbol: "circle",
            symbolSize: 5,
            smooth: true
          },
          {
            name: "兴趣→购买",
            type: "line",
            data: interestToBuy,
            itemStyle: { color: C.gold },
            lineStyle: { color: C.gold, width: 2 },
            symbol: "diamond",
            symbolSize: 5,
            smooth: true
          },
          {
            name: "整体转化率",
            type: "line",
            data: overallCVR,
            itemStyle: { color: C.teal },
            lineStyle: { color: C.teal, width: 2, type: "dashed" },
            symbol: "triangle",
            symbolSize: 5,
            smooth: true
          }
        ]
      })
    );

    bindResize(chart);
    return chart;
  }
});
