# 淘宝用户行为分析系统 — 项目总结

## 一、项目概述

基于 Hadoop HDFS + PySpark + Flask + ECharts 的大数据分析系统，处理 1 亿条淘宝用户行为数据，完成数据清洗、10 维度统计分析、Web 可视化全链路。

| 项目 | 内容 |
|------|------|
| 数据规模 | 100,095,182 条记录，3.67GB CSV |
| 用户规模 | 987,991 独立用户 |
| 集群 | 3 节点（master + slave1 + slave2） |
| 分析模块 | 10 个，输出 22 个 JSON 文件 |
| 前端 | 19 个 ECharts 图表，5 个 Tab |

---

## 二、环境配置阶段的问题

### 问题 1：YARN 资源分配失败

**现象**：`Required executor memory 2048MB is above the max threshold 2048MB`

**原因**：YARN 的 `yarn.scheduler.maximum-allocation-mb` 默认 2048MB，而 Spark executor 请求的内存超过了这个上限。

**解决**：将 executor memory 降为 1g，或直接回退到 Spark local 模式。

**经验**：小集群（3 节点）用 YARN 模式经常遇到资源限制问题，local 模式反而更稳定。

### 问题 2：HDFS 副本因子导致空间不足

**现象**：`File could only be written to 0 of the 1 minReplication nodes`

**原因**：HDFS 默认副本因子为 3，3.67GB 的文件需要 11GB 存储空间，小集群磁盘不够。

**解决**：`hdfs dfs -setrep -w 1 /data/raw/user.csv`，并添加 Spark 配置 `--conf spark.hadoop.dfs.replication=1`。

**经验**：小集群必须修改副本因子，否则空间会迅速耗尽。

### 问题 3：磁盘空间不足导致 Spark 卡死

**现象**：Spark 任务卡在 0% CPU，`rchar` 停在 3.3GB 不动。

**原因**：master 节点磁盘 96% 满（仅剩 788MB），Spark 无法写入临时文件。

**解决**：清理 `/tmp/spark-*` 临时文件，或将任务转移到磁盘空间更大的 slave 节点。

**经验**：Spark shuffle 操作需要 2-3 倍输入数据的临时空间，必须预留足够磁盘。

### 问题 4：`saveAsTextFile` 的 FileAlreadyExistsException

**现象**：`Output directory hdfs:///data/output/xxx.json already exists`

**原因**：Spark 的 `saveAsTextFile` 要求目标目录不存在。旧的输出目录残留导致重复运行失败。

**解决**：创建 `run_all.sh` 脚本，运行前自动清理旧目录并验证删除结果：

```bash
clean_hdfs_dir() {
    local dir=$1
    if hdfs dfs -test -d "$dir" 2>/dev/null; then
        hdfs dfs -rm -r -f "$dir" 2>/dev/null
        sleep 2
        # 验证删除
        if hdfs dfs -test -d "$dir" 2>/dev/null; then
            echo "清理失败: $dir 仍存在"
            return 1
        fi
    fi
    hdfs dfs -mkdir -p "$dir"
}
```

**经验**：`hdfs dfs -rm -r` 删除目录后需要等待 1-2 秒，不能立即重建。

### 问题 5：Spark 路径在不同节点不一致

**现象**：`/home/hadoop/spark/bin/spark-submit: No such file or directory`

**原因**：master 节点的 Spark 安装在 `/usr/local/spark/`，slave 节点在 `/home/hadoop/spark/`。

**解决**：在 `run_all.sh` 中使用 master 的路径 `/usr/local/spark/bin/spark-submit`。

**经验**：集群环境配置不一致是常见问题，脚本中要硬编码正确的路径。

---

## 三、数据分析阶段的问题

### 问题 6：商品转化率超过 100%（最高 705%）

**现象**：商品 1881953 的 `pv_to_buy_rate = 1053%`（修复后仍为 705%）。

**原因分析**：

原始代码用 `count(behavior)` 统计购买次数，同一用户多次购买会重复计数。修复为 `countDistinct(user_id)` 后仍有商品 CVR > 100%。

进一步分析发现：**85-97% 的购买者在数据期内没有浏览过该商品。** 根因是购物车/收藏夹的跨期持久性 — 用户在数据期前加购，数据期内直接下单，不产生 pv 记录。

**验证方法**：

```python
# 对极端CVR商品，检查buy用户是否在数据期内有pv
buy_users = df.filter((col('item_id') == item) & (col('behavior') == 'buy')).select('user_id').distinct()
pv_users = df.filter((col('item_id') == item) & (col('behavior') == 'pv')).select('user_id').distinct()
buy_no_pv = buy_users.join(pv_users, on='user_id', how='left_anti')
```

**结论**：CVR > 100% 不是计算错误，是数据截断导致的结构性偏差。数据集的 pv 只记录详情页浏览，不包含搜索/推荐等渠道的曝光，分母不完整。

### 问题 7：转化漏斗比例异常（86.9% → 61%）

**现象**：漏斗显示 86.9% 的浏览用户有加购/收藏行为，61% 完成购买，远高于行业水平（5-15%）。

**原因**：

1. 数据集筛选了活跃用户（98.1% 的用户有 10+ 条行为）
2. 跨期效应 — 数据期前的加购/收藏在数据期内完成购买
3. 9 天时间窗口内用户行为重复

**解决**：在报告中说明数据集的局限性，用行为级 CVR（2.25%）作为可信的转化率指标。

### 问题 8：46.4% 的购买在当天没有浏览记录

**现象**：统计当天的 (user, item) 对，1,966,120 个有 buy 的对中，912,534 个没有 pv。

**验证**：

```sql
SELECT
    COUNT(*) AS buy_cnt,
    SUM(has_pv) AS click_cnt
FROM (
    SELECT date, user_id, item_id,
        MAX(IF(behavior='pv', 1, 0)) AS has_pv,
        MAX(IF(behavior='buy', 1, 0)) AS has_buy
    FROM user_behavior
    GROUP BY date, user_id, item_id
) t
WHERE has_buy = 1;
```

**结论**：这是数据集的核心特征，不是 bug。用户通过购物车/收藏夹直接购买，不经过详情页浏览。

### 问题 9：路径分析跨越会话边界

**现象**：用户最后一个会话的行为可能和下一个会话的第一个行为被连在一起。

**原因**：`lag()` 的窗口是 `partitionBy("user_id")`，不区分会话。

**解决**：改为 `partitionBy("user_id", "session_id")`，确保路径只在会话内部拼接。

```python
window_session = Window.partitionBy("user_id", "session_id").orderBy("timestamp")
```

### 问题 10：工作日/周末天数硬编码

**现象**：`weekday_count=5, weekend_count=4` 硬编码，数据范围变化会出错。

**解决**：从数据动态计算：

```python
df_dow_dates = df_dow.select("date", "day_of_week").distinct()
weekday_count = df_dow_dates.filter(col("day_of_week").isin(2, 3, 4, 5, 6)).count()
weekend_count = df_dow_dates.filter(col("day_of_week").isin(1, 7)).count()
```

---

## 四、运行过程中的问题

### 问题 11：task2 超时（30 分钟不够）

**现象**：`run_all.sh` 的 30 分钟超时终止了 task2，CVR 分析（`countDistinct`）比旧的 `count` 慢很多。

**解决**：将超时从 30 分钟改为 60 分钟，并支持单独运行某个模块：

```bash
spark-submit task2_main.py --only 2.8    # 只跑 CVR 分析
```

### 问题 12：内存不足导致 showString 失败

**现象**：`An error occurred while calling o173.showString.`

**原因**：商品维度的 `countDistinct` 聚合产生大量中间数据，内存不足。

**解决**：将 2.8 模块拆分运行，先跑高转化商品（成功），再单独跑低转化和类目转化率。

### 问题 13：Flask 后端 CVR 公式未同步

**现象**：前端展示的 CVR 仍是旧公式（`buy/pv`），与分析模块的修复不一致。

**原因**：修改了分析模块但没有同步修改 Flask 后端的 4 个 API 端点。

**解决**：统一改为 `countDistinct(buy用户) / countDistinct(interest用户) * 100`。

**经验**：后端 API 和分析模块使用独立的代码计算相同指标，容易不一致。应该统一计算逻辑。

---

## 五、代码设计与重构

### 5.1 为什么重构？

**原始架构的问题**：

| 问题 | 表现 |
|------|------|
| 硬编码模块列表 | `task2_main.py` 中手动 import 10 个模块，新增/删除要改 3 处 |
| 不能选择性运行 | 跑一个分析必须跑全部，调试代价高 |
| 前端单文件 | 738 行 HTML+CSS+JS 混合，改一个图表要翻整个文件 |
| 前后端耦合 | 后端输出文件名硬编码在前端 `getData()` 调用中 |

### 5.2 重构方案

**后端：模块自注册机制**

每个分析模块在文件顶部声明元数据：

```python
MODULE_ID = "2.1"
MODULE_NAME = "每日UV/PV统计"
MODULE_GROUP = "user"
MODULE_ENABLED = True
OUTPUT_FILES = ["daily_uv_pv.json"]
```

`__init__.py` 通过 `discover_analyses(config)` 自动扫描所有模块，不需要手动 import。

**启用/关闭机制（三级优先级）**：

```
CLI --only/--skip/--enable    （最高）
config.yaml disabled 列表     （项目级）
MODULE_ENABLED                （模块级）
```

**前端：图表自注册机制**

每个图表文件调用 `registerChart()` 注册自己：

```js
registerChart({
  id: "chartDailyUVPV",
  title: "每日UV/PV统计",
  tab: "overview",
  dataFile: "daily_uv_pv.json",
  render: function(data, container) { ... }
});
```

`app.js` 根据注册表自动收集数据文件、并行加载、分发渲染。

### 5.3 重构效果

| 指标 | 重构前 | 重构后 |
|------|--------|--------|
| 新增分析 | 改 3 个文件 | 加 1 个 .py 文件 |
| 关闭分析 | 删代码或注释 | config.yaml 写一行 |
| 选择性运行 | 不支持 | `--only`/`--skip`/`--enable` |
| index.html | 738 行 | 134 行 |
| 前端新增图表 | 改 2 处 | 加 1 个 .js + 1 行 script |

---

## 六、分析项选择的思考

### 6.1 为什么选这 10 个分析维度？

| 维度 | 选择理由 |
|------|---------|
| 每日 UV/PV | 基础指标，验证数据完整性 |
| 日留存率 | 衡量用户粘性，经典指标 |
| RFM 分层 | 用户价值分层，运营基础 |
| 行为路径 | 理解用户行为序列 |
| 用户生命周期 | 新老用户占比，平台健康度 |
| 时间维度 | 发现活跃规律，指导运营 |
| 商品热度 | 商品排行，选品依据 |
| 商品转化率 | 发现高转化/低转化商品 |
| 转化漏斗 | 流失分析，优化关键节点 |
| 行为分布 | 数据概览，验证合理性 |

### 6.2 没有选但值得做的分析

| 分析 | 说明 |
|------|------|
| 商品-类目一致性检查 | 1,405 个商品对应多个类目（0.34% 脏数据） |
| 用户行为数分布 | 验证数据集是否经过活跃用户筛选 |
| 当天无 pv 的 buy 统计 | 揭示数据集的核心特征 |
| 购物篮分析 | 商品关联性，适合推荐系统 |
| 序列模式挖掘 | 用户行为序列的频繁模式 |

---

## 七、踩坑经验总结

### 7.1 集群环境

- 小集群用 Spark local 模式比 YARN 更稳定
- HDFS 副本因子必须设为 1
- 每个节点的 Spark 安装路径可能不同
- 磁盘空间要预留 2-3 倍输入数据量

### 7.2 PySpark 开发

- `saveAsTextFile` 要求目标目录不存在
- `countDistinct` 比 `count` 慢 3-5 倍
- `show()` 会触发计算，大数据集慎用
- `approxQuantile` 比精确分位数快很多

### 7.3 数据分析

- CVR 的分母定义决定了指标含义
- 跨期数据截断会导致系统性偏差
- 活跃用户筛选会让所有指标偏高
- 行为级指标（笔数）比用户级指标更稳定

### 7.4 工程实践

- 分析模块要有统一的 `run(df, config, save_fn)` 接口
- 每个模块要声明输出文件名，方便自动化
- 运行脚本要有磁盘检查、进程清理、目录清理
- 前后端相同指标要用相同公式计算

---

## 八、数据集的核心特征（对分析结论的影响）

| 特征 | 数据表现 | 影响 |
|------|---------|------|
| 活跃用户筛选 | 98.1% 用户有 10+ 条行为 | 所有用户级指标偏高 |
| 跨期行为 | 46.4% 购买无当日浏览 | 商品 CVR 严重偏高 |
| pv 只记录详情页 | 不含搜索/推荐曝光 | CVR 分母不完整 |
| 9 天时间窗口 | 用户行为重复 | 漏斗比例偏高 |
| 脏数据 | 5 万条时间戳异常 | 需要数据清洗 |

**唯一可信的转化率指标**：行为级 CVR = buy笔数 / pv笔数 = 2.25%

---

## 九、部署清单

| 步骤 | 命令 | 说明 |
|------|------|------|
| 上传代码 | `scp -r lib/ hadoop@master:~/` | 分析模块 |
| 清理输出 | `hdfs dfs -rm -r -f /data/output` | 删除旧结果 |
| 运行分析 | `bash run_all.sh task2` | 全量运行 |
| 选择性运行 | `spark-submit task2_main.py --only 2.1` | 单模块 |
| 导出数据 | `bash run_all.sh export` | HDFS → 本地 JSON |
| 重启 Flask | `ps aux \| grep app.py \| xargs kill; cd ~/backend && nohup python3 app.py &` | 后端 API |
| 前端访问 | `http://master:8088/` | 可视化页面 |

---

## 十、项目文件结构

```
user_behavior_analysis/
├── config.yaml                    # 全局配置
├── task1_data_cleaning.py         # 数据清洗
├── task2_main.py                  # 分析编排（支持 CLI）
├── run_all.sh                     # 一键运行脚本
├── lib/
│   ├── data_loader.py             # 数据加载 + 校验
│   ├── json_writer.py             # JSON 输出
│   └── analyses/                  # 10 个分析模块（自注册）
│       ├── __init__.py            # 自动发现
│       ├── user_daily.py          # 2.1 每日UV/PV
│       ├── user_retention.py      # 2.2 留存率
│       ├── user_rfm.py            # 2.3 RFM分层
│       ├── user_path.py           # 2.4 行为路径
│       ├── user_lifecycle.py      # 2.5 生命周期
│       ├── user_time.py           # 2.6 时间分析
│       ├── product_hot.py         # 2.7 商品热度
│       ├── product_cvr.py         # 2.8 转化率
│       ├── funnel.py              # 2.9 漏斗
│       └── overview.py            # 2.10 行为分布
├── backend/
│   └── app.py                     # Flask API
├── task3_visualization/
│   ├── index.html                 # 布局（134行）
│   ├── css/style.css              # 样式
│   ├── js/
│   │   ├── utils.js               # 工具函数
│   │   ├── demo.js                # 演示数据
│   │   └── app.js                 # 数据加载调度
│   └── charts/
│       ├── registry.js            # 图表注册表
│       └── 20个图表模块            # 自注册
├── fixing.md                      # 问题修复记录
├── metrics_analysis.md            # 指标计算方式
├── system_architecture.md         # 系统架构文档
├── data_analysis_report.md        # 数据探查报告
├── project_summary.md             # 本文档
└── CLAUDE.md                      # 项目规范
```
