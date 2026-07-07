# 项目介绍与面试准备

> 淘宝用户行为多维分析系统 — 答辩/面试参考

---

## 一、项目一句话介绍

基于 Hadoop HDFS + PySpark 对淘宝1亿条用户行为数据进行10个维度的分布式分析，通过 Flask API 提供数据服务，前端用 ECharts 渲染19个交互式图表，实现了从数据清洗到可视化展示的完整大数据流水线。

---

## 二、项目背景与价值

**数据集**：阿里巴巴公开的淘宝用户行为数据集（UserBehavior），包含2017年11月25日至12月3日期间约100万随机用户的全部行为记录，共100,095,182条（约1亿条），原始CSV约3.67GB。

**为什么用大数据技术**：
- 单机处理3.67GB数据需要数十分钟，且容易OOM
- RFM分层、行为路径分析等需要多次shuffle操作，Spark内存计算比MapReduce快10-100倍
- HDFS提供可靠的分布式存储，支持多节点并行读取

**业务价值**：
- 用户价值分层（RFM）→ 精细化运营
- 转化漏斗分析 → 找到流失环节
- 行为路径分析 → 优化用户旅程
- 商品转化率分析 → 识别高潜力商品

---

## 三、技术架构

```
数据流：HDFS(CSV) → PySpark清洗(Parquet) → 10个分析模块(JSON) → Flask API → ECharts前端

┌─────────────────────────────────────────────────────────────────┐
│  存储层：Hadoop HDFS（副本因子=1，适配3节点小集群）              │
│  ├─ /data/raw/user.csv          原始CSV（3.67GB）               │
│  ├─ /data/clean/                清洗后Parquet（1.9GB，按日期分区）│
│  └─ /data/output/               分析结果JSON（22个文件）         │
├─────────────────────────────────────────────────────────────────┤
│  计算层：PySpark 3.5.8（local[2]模式）                          │
│  ├─ task1_data_cleaning.py      数据清洗（Schema定义、时间过滤、去重）│
│  ├─ task2_main.py               分析主入口（编排10个模块）       │
│  └─ lib/analyses/               10个自注册分析模块               │
├─────────────────────────────────────────────────────────────────┤
│  服务层：Flask REST API（端口5000）                              │
│  ├─ /api/data/<filename>        通用JSON数据端点（支持日期过滤）  │
│  ├─ /api/overview               全局概览指标                     │
│  ├─ /api/item/<id>              商品详情                         │
│  └─ /api/search                 搜索接口                         │
├─────────────────────────────────────────────────────────────────┤
│  展示层：ECharts 5.5.1（端口8088）                              │
│  ├─ 19个交互式图表（5个Tab页）                                  │
│  ├─ 日期筛选、行为联动、全屏查看                                │
│  └─ ChartRegistry自注册机制                                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 四、核心技术点

### 4.1 数据清洗

```python
# Schema定义（避免自动推断性能开销）
schema = StructType([
    StructField("user_id",     IntegerType(), False),
    StructField("item_id",     IntegerType(), False),
    StructField("category_id", IntegerType(), False),
    StructField("behavior",    StringType(),  False),
    StructField("timestamp",   IntegerType(), False),
])

# 过滤时间范围（北京时间2017-11-25至2017-12-03）
df = df.filter((col("timestamp") >= TS_START) & (col("timestamp") <= TS_END))
df = df.dropDuplicates()  # 去重
df = df.withColumn("datetime", from_unixtime("timestamp"))
df = df.withColumn("hour", hour("datetime"))
```

### 4.2 严格递进漏斗（关键设计）

传统漏斗的问题：各阶段独立统计，不保证包含关系。

```python
# 严格递进：每阶段是上阶段的子集（inner join）
pv_user_set = df.filter(behavior=="pv").select("user_id").distinct()
interest_user_set = df.filter(behavior.isin("cart","fav")).select("user_id").distinct()
buy_user_set = df.filter(behavior=="buy").select("user_id").distinct()

# Stage2 = pv ∩ interest
pv_and_interest = pv_user_set.join(interest_user_set, on="user_id", how="inner")
# Stage3 = Stage2 ∩ buy
pv_interest_and_buy = pv_and_interest.join(buy_user_set, on="user_id", how="inner")
```

结果：浏览984,105 → 加购/收藏855,498（86.9%）→ 购买600,278（61.0%）

### 4.3 RFM用户分层

```python
# R（最近购买时间）和 F（购买频率）加权评分
total_score = R_score × 0.4 + F_score × 0.6

# 分层（阈值通过config.yaml可配置）
>= 4.0 → 高价值用户
>= 3.0 → 潜力用户
>= 2.0 → 一般用户
>= 1.4 → 预流失用户
<  1.4 → 流失用户
```

### 4.4 模块自注册机制

```python
# 每个模块声明元数据
MODULE_ID = "2.8"
MODULE_NAME = "商品转化率分析"
MODULE_GROUP = "product"
MODULE_ENABLED = True
OUTPUT_FILES = ["item_cvr_top.json", "item_low_cvr.json", "category_cvr.json"]

def run(df, config, save_fn):
    # 分析逻辑...
    save_fn(df_result, "item_cvr_top.json", "商品转化率TOP15")
```

```python
# 自动发现
def discover_analyses(config):
    for _, module_name, _ in pkgutil.iter_modules([package_dir]):
        mod = importlib.import_module(f".{module_name}", package="lib.analyses")
        if hasattr(mod, "MODULE_ID"):
            analyses.append({"id": mod.MODULE_ID, "run": mod.run, ...})
```

### 4.5 前端图表注册表

```javascript
// 每个图表文件自注册
registerChart({
  id: "chartDailyUVPV",
  title: "每日UV/PV趋势",
  tab: "overview",
  dataFile: "daily_uv_pv.json",
  render: function(data, container) {
    const chart = echarts.init(container);
    chart.setOption({...});
  }
});

// app.js 自动分发
const charts = ChartRegistry.getAll();
const files = ChartRegistry.getDataFiles();
await Promise.all(files.map(async f => { dataMap[f] = await getData(f); }));
charts.forEach(c => {
  const el = document.getElementById(c.id);
  if (el && dataMap[c.dataFile]?.length) c.render(dataMap[c.dataFile], el);
});
```

---

## 五、踩坑经验（面试加分项）

### 5.1 磁盘空间不足
**问题**：Spark处理3.67GB数据时卡住，报 `No space left on device`。
**根因**：HDFS默认副本因子为3，3.6GB实际占用约11GB，加上Spark shuffle需要2-3倍临时空间。
**解决**：`hdfs dfs -setrep -w 1` 降低副本因子，清理swap文件释放4GB，master扩容至40GB。

### 5.2 商品CVR超过100%
**问题**：67个商品CVR超过100%，最高705%。
**排查过程**：
1. 第一步：发现用 `count(behavior)` 统计购买次数，改为 `countDistinct(user_id)` → 仍超过100%
2. 第二步：逐商品验证，发现85-97%的购买者在数据期内没有浏览记录
3. 根因：购物车/收藏夹的跨期持久性，用户在数据期前加购，数据期内直接购买
**结论**：不是计算错误，是数据集的核心特征。行为级CVR（2.25%）是唯一可靠指标。

### 5.3 前端图表不渲染（静默失败）
**问题**：重构后12个图表空白，控制台无错误。
**根因**：`document.getElementById(c.id)` 返回null时 `if(!el) return` 静默跳过。图表ID（`dailyUVPV`）与HTML容器ID（`chartDailyUVPV`）不匹配。
**教训**：静默失败比报错更难排查。应该加 `console.warn`。

### 5.4 漏斗比例虚高
**问题**：每日漏斗趋势显示 pv→interest 46%、interest→buy 42%，远高于行业水平。
**根因**：三个指标独立统计，不是递进关系。interest_uv包含了很多当天没有pv的用户（跨期加购）。
**解决**：改为按日期的递进漏斗（inner join），interest→buy从42%降至24%。

---

## 六、面试常见问题

### Q1：为什么选择PySpark而不是Hive或Flink？

**A**：
- **vs Hive**：Hive是SQL-on-Hadoop，适合简单聚合，但RFM分层、行为路径等需要复杂窗口函数和多步join，PySpark DataFrame API更灵活
- **vs Flink**：Flink是流处理，本项目是离线批处理场景，不需要实时性
- **PySpark优势**：内存计算快、Python生态丰富、DataFrame API简洁、支持复杂转换

### Q2：数据清洗做了哪些事情？为什么？

**A**：
1. **Schema定义**：避免Spark自动推断类型（性能开销大，且可能推断错误）
2. **时间戳过滤**：只保留2017-11-25至2017-12-03的数据（约5万条脏数据被过滤）
3. **去重**：日志采集系统可能产生重复记录
4. **字段派生**：从timestamp生成datetime、date、hour，便于后续按时间维度分析
5. **Parquet输出**：列式存储，压缩比约2:1（3.67GB→1.9GB），支持列裁剪和谓词下推

### Q3：RFM模型为什么只用R和F，不用M（金额）？

**A**：数据集不包含交易金额字段，只有行为类型（pv/cart/fav/buy）。所以RFM实际是RF二维模型。这是数据集的限制，不是设计选择。如果有金额数据，可以扩展为完整的RFM三维模型。

### Q4：转化漏斗为什么用inner join而不是独立统计？

**A**：
- 独立统计的问题：分别统计有pv的用户、有cart的用户、有buy的用户，三个集合不保证包含关系。统计出的"pv→cart转化率"实际上是"有cart行为的用户÷有pv行为的用户"，但这两个用户群可能完全不重叠。
- inner join确保每阶段是上阶段的子集，转化率单调递减，才是真正的"漏斗"。

### Q5：46.4%的购买没有浏览记录，怎么解释？

**A**：这是数据集的核心特征，不是bug：
- 数据集只记录详情页浏览（pv），不包含搜索结果页、推荐流的曝光
- 购物车/收藏夹是持久状态，用户可能在数据期前加购，数据期内直接下单
- 数据窗口只有9天，而用户的购物决策周期可能更长
- 这导致商品维度CVR系统性偏高，但行为级CVR（buy笔数/pv笔数=2.25%）是可靠的

### Q6：前端为什么用注册表模式而不是直接写在HTML里？

**A**：
- **可扩展性**：新增图表只需添加一个JS文件，调用registerChart()，无需修改index.html
- **数据驱动**：app.js通过ChartRegistry.getDataFiles()自动收集所有需要的数据文件，并行加载后按dataFile分发
- **模块解耦**：每个图表文件独立，互不影响，便于团队协作
- **对比**：原始方案是738行的单文件HTML，所有图表代码混在一起，维护困难

### Q7：遇到过哪些性能问题？怎么解决？

**A**：
1. **countDistinct比count慢很多**：CVR修复后task2从15分钟增至40分钟。countDistinct需要额外shuffle。解决：增加超时时间，支持按模块选择性运行
2. **Spark shuffle磁盘空间不足**：3.67GB数据需要约10GB临时空间。解决：降低副本因子、清理磁盘、扩容
3. **YARN资源分配失败**：executor内存请求超过YARN上限。解决：回退到local模式

### Q8：如果数据量增长到10亿条，架构需要怎么调整？

**A**：
1. **存储**：HDFS副本因子恢复为3，增加节点数量
2. **计算**：从local模式切回YARN模式，增加executor数量和内存
3. **shuffle优化**：增加shuffle分区数（spark.sql.shuffle.partitions），避免数据倾斜
4. **缓存**：对频繁访问的DataFrame使用cache()/persist()
5. **增量处理**：引入增量更新机制，避免每次全量重跑
6. **实时化**：如果需要实时分析，引入Spark Streaming或Flink

### Q9：模块自注册机制的设计思路？

**A**：
- **问题**：最初所有分析模块硬编码在task2_main.py中，新增/删除模块需要修改主文件
- **方案**：每个模块声明MODULE_ID/MODULE_NAME/MODULE_ENABLED等元数据，__init__.py通过pkgutil自动扫描和加载
- **三级启用控制**：CLI参数（最高优先级）> config.yaml的disabled列表 > 模块自身的MODULE_ENABLED
- **效果**：新增模块只需添加一个.py文件，关闭模块只需在config.yaml写一行disabled

### Q10：项目中最有成就感的发现是什么？

**A**：商品CVR超过100%的排查过程。最初以为是计算bug（count vs countDistinct），修复后仍有异常。通过逐商品统计购买用户的浏览情况，发现85-97%的购买者在数据期内没有浏览记录。进一步分析发现这是数据集的核心特征——购物车/收藏夹的跨期持久性。这个发现改变了我们对所有转化率指标的理解：行为级CVR（2.25%）才是唯一可靠的指标。

---

## 七、项目数据一览

| 指标 | 值 |
|------|------|
| 原始数据量 | 100,095,182条（约1亿条） |
| 原始文件大小 | 3.67GB（CSV） |
| 清洗后大小 | 1.9GB（Parquet） |
| 独立用户数 | 987,991 |
| 数据时间窗口 | 9天（2017-11-25至2017-12-03） |
| 分析模块数 | 10个 |
| 输出JSON文件 | 22个 |
| 前端图表数 | 19个 |
| Flask API端点 | 9个 |
| 集群节点数 | 3个（1 Master + 2 Slave） |
| 代码行数 | 约6,300行 |

---

## 八、可改进方向

1. **数据质量**：引入数据质量校验框架（如Great Expectations），自动检测异常值
2. **实时分析**：引入Spark Streaming或Flink，支持实时仪表板
3. **A/B测试**：支持不同用户群体的对比分析
4. **机器学习**：基于RFM分层结果，训练用户流失预测模型
5. **数据仓库**：引入Hive/Spark SQL构建数据仓库，支持更复杂的OLAP查询
6. **自动化测试**：对关键模块（漏斗、RFM）添加单元测试
7. **容器化**：Docker部署，一键启动整个系统
