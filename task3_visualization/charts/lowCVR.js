registerChart({
  id: "chartLowCVR",
  title: "低转化率商品 TOP 15",
  tab: "funnel",
  dataFile: "item_low_cvr.json",
  render: function (data, container) {
    if (!data || !data.length) return;
    var chart = echarts.init(container);
    allCharts.chartLowCVR = chart;

    var top = data.slice(0, 15);
    var ids = top.map(function (d) { return "" + d.item_id; });
    var views = top.map(function (d) { return d.pv || d.view_count || d.views || 0; });
    var cvr = top.map(function (d) { return d.pv_to_buy_rate || d.cvr || d.conversion_rate || 0; });

    chart.setOption(
      Object.assign(baseOpt(), {
        tooltip: Object.assign(baseOpt().tooltip, {
          trigger: "axis",
          axisPointer: { type: "cross" },
          formatter: function (params) {
            var s = params[0].axisValue + "<br/>";
            params.forEach(function (p) {
              var unit = p.seriesName === "转化率" ? "%" : "";
              s += p.marker + " " + p.seriesName + ": " + (unit === "%" ? p.value.toFixed(2) : fmtNum(p.value)) + unit + "<br/>";
            });
            return s;
          }
        }),
        legend: {
          data: ["浏览量", "转化率"],
          top: 5,
          textStyle: { color: C.muted, fontSize: 11 }
        },
        grid: { left: 60, right: 60, top: 45, bottom: 70 },
        xAxis: {
          type: "category",
          data: ids,
          axisLine: { lineStyle: { color: C.border } },
          axisLabel: { color: C.muted, fontSize: 10, rotate: 45 },
          axisTick: { alignWithLabel: true }
        },
        yAxis: [
          {
            type: "value",
            name: "浏览量",
            nameTextStyle: { color: C.muted, fontSize: 11 },
            axisLine: { show: false },
            axisLabel: { color: C.muted, fontSize: 11 },
            splitLine: { lineStyle: { color: "#EDEBE8" } }
          },
          {
            type: "value",
            name: "转化率(%)",
            nameTextStyle: { color: C.red, fontSize: 11 },
            axisLine: { show: false },
            axisLabel: {
              color: C.red,
              fontSize: 11,
              formatter: function (v) { return v.toFixed(1) + "%"; }
            },
            splitLine: { show: false }
          }
        ],
        series: [
          {
            name: "浏览量",
            type: "bar",
            yAxisIndex: 0,
            data: views,
            itemStyle: { color: C.sky },
            barWidth: "50%"
          },
          {
            name: "转化率",
            type: "line",
            yAxisIndex: 1,
            data: cvr,
            itemStyle: { color: C.red },
            lineStyle: { color: C.red, width: 2, type: "dashed" },
            symbol: "circle",
            symbolSize: 6
          }
        ]
      })
    );

    bindResize(chart);
    return chart;
  }
});
