registerChart({
  id: "chartHourlyCVR",
  title: "每小时浏览量与转化率",
  tab: "time",
  dataFile: "hourly_cvr.json",
  render: function (data, container) {
    if (!data || !data.length) return;
    var chart = echarts.init(container);
    allCharts.chartHourlyCVR = chart;

    var hours = data.map(function (d) { return d.hour != null ? d.hour + ":00" : d.label; });
    var views = data.map(function (d) { return d.pv_count || d.view_count || d.views || 0; });
    var purchases = data.map(function (d) { return d.buy_count || d.purchases || 0; });
    var cvr = data.map(function (d) { return d.pv_to_buy_rate || d.cvr || d.conversion_rate || 0; });

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
          data: ["浏览量", "购买量", "转化率"],
          top: 5,
          textStyle: { color: C.muted, fontSize: 11 }
        },
        grid: { left: 60, right: 60, top: 45, bottom: 30 },
        xAxis: {
          type: "category",
          data: hours,
          axisLine: { lineStyle: { color: C.border } },
          axisLabel: { color: C.muted, fontSize: 11 },
          axisTick: { alignWithLabel: true }
        },
        yAxis: [
          {
            type: "value",
            name: "数量",
            nameTextStyle: { color: C.muted, fontSize: 11 },
            axisLine: { show: false },
            axisLabel: { color: C.muted, fontSize: 11 },
            splitLine: { lineStyle: { color: "#EDEBE8" } }
          },
          {
            type: "value",
            name: "转化率(%)",
            nameTextStyle: { color: C.sienna, fontSize: 11 },
            axisLine: { show: false },
            axisLabel: {
              color: C.sienna,
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
            barGap: "10%",
            barWidth: "30%"
          },
          {
            name: "购买量",
            type: "bar",
            yAxisIndex: 0,
            data: purchases,
            itemStyle: { color: C.teal },
            barWidth: "30%"
          },
          {
            name: "转化率",
            type: "line",
            yAxisIndex: 1,
            data: cvr,
            itemStyle: { color: C.sienna },
            lineStyle: { color: C.sienna, width: 2 },
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
