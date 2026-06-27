registerChart({
  id: "chartWeekdayWeekend",
  title: "工作日 vs 周末对比",
  tab: "time",
  dataFile: "weekday_vs_weekend.json",
  render: function (data, container) {
    if (!data || !data.length) return;
    var chart = echarts.init(container);
    allCharts.chartWeekdayWeekend = chart;

    var categories = data.map(function (d) { return d.type || d.label || d.category; });
    var dailyBehavior = data.map(function (d) { return d.daily_avg_behavior || d.avg_daily_behavior || d.daily_behavior || 0; });
    var uv = data.map(function (d) { return d.uv || d.uv_count || 0; });
    var purchases = data.map(function (d) { return d.buy || d.purchases || d.buy_count || 0; });

    chart.setOption(
      Object.assign(baseOpt(), {
        tooltip: Object.assign(baseOpt().tooltip, {
          trigger: "axis",
          axisPointer: { type: "shadow" },
          formatter: function (params) {
            var s = params[0].axisValue + "<br/>";
            params.forEach(function (p) {
              s += p.marker + " " + p.seriesName + ": " + fmtNum(p.value) + "<br/>";
            });
            return s;
          }
        }),
        legend: {
          data: ["日均行为", "UV", "购买量"],
          top: 5,
          textStyle: { color: C.muted, fontSize: 11 }
        },
        grid: { left: 70, right: 30, top: 45, bottom: 30 },
        xAxis: {
          type: "category",
          data: categories,
          axisLine: { lineStyle: { color: C.border } },
          axisLabel: { color: C.muted, fontSize: 12 },
          axisTick: { alignWithLabel: true }
        },
        yAxis: {
          type: "value",
          axisLine: { show: false },
          axisLabel: { color: C.muted, fontSize: 11 },
          splitLine: { lineStyle: { color: "#EDEBE8" } }
        },
        series: [
          {
            name: "日均行为",
            type: "bar",
            data: dailyBehavior,
            itemStyle: { color: C.sienna },
            barGap: "15%",
            barWidth: "20%",
            label: {
              show: true,
              position: "top",
              formatter: function (p) { return fmtNum(p.value); },
              fontSize: 10,
              color: C.muted
            }
          },
          {
            name: "UV",
            type: "bar",
            data: uv,
            itemStyle: { color: C.teal },
            barWidth: "20%",
            label: {
              show: true,
              position: "top",
              formatter: function (p) { return fmtNum(p.value); },
              fontSize: 10,
              color: C.muted
            }
          },
          {
            name: "购买量",
            type: "bar",
            data: purchases,
            itemStyle: { color: C.gold },
            barWidth: "20%",
            label: {
              show: true,
              position: "top",
              formatter: function (p) { return fmtNum(p.value); },
              fontSize: 10,
              color: C.muted
            }
          }
        ]
      })
    );

    bindResize(chart);
    return chart;
  }
});
