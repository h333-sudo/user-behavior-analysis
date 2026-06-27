# 系统架构文档

## 1. 系统概述

淘宝用户行为数据分析系统，基于 Hadoop HDFS + PySpark + Flask + ECharts 构建，分析 1 亿条用户行为数据（2017.11.25 - 2017.12.03，共9天）。

---

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                    用户浏览器                              │
│              http://master:8088                          │
└──────────────────────┬──────────────────────────────────┘
                       │ HTTP
┌──────────────────────▼──────────────────────────────────┐
│              Web 前端 (index.html)                       │
│  - ECharts 5.5.1 图表库                                  │
│  - 19种可视化图表                                        │
│  - 日期筛选 / 行为过滤 / 全屏模式                         │
└──────────────────────┬──────────────────────────────────┘
                       │ REST API (fetch)
┌──────────────────────▼──────────────────────────────────┐
│              Flask 后端 (app.py) :5000                    │
│  - /api/dates          返回可用日期                       │
│  - /api/data/<file>    返回JSON数据(支持日期筛选)          │
│  - /api/overview       行为概览                           │
│  - /api/search         商品搜索                           │
└──────────────────────┬──────────────────────────────────┘
                       │ PySpark DataFrame API
┌──────────────────────▼──────────────────────────────────┐
│              HDFS 数据层                                  │
│  /data/raw/user.csv       原始CSV (3.67GB, 1亿条)         │
│  /data/clean/             清洗后Parquet (1.9GB, 按日期分区) │
│  /data/output/*.json      分析结果 (22个JSON文件)          │
└─────────────────────────────────────────────────────────┘
```

---

## 3. 集群环境

| 节点 | IP | 角色 | 磁盘 | 内存 |
|------|-----|------|------|------|
| master | 192.168.131.131 | NameNode, DataNode, Flask, HTTP Server | 38GB | 5.7GB |
| slave1 | 192.168.131.132 | DataNode, NodeManager, ResourceManager | 19GB | 5.7GB |
| slave2 | 192.168.131.133 | DataNode, NodeManager | 19GB | 5.7GB |

**运行模式**: Spark local[2]（YARN 资源不足时回退到本地模式）

---

## 4. 数据流水线

### 4.1 Task1: 数据清洗 (`task1_data_cleaning.py`)

```
输入: /data/raw/user.csv (3.67GB, 无表头)
  ↓
清洗步骤:
  1. 定义Schema: user_id, item_id, category_id, behavior, timestamp
  2. 类型转换: timestamp → datetime → 提取 date, hour
  3. 过滤无效记录 (user_id为空等)
  4. 去重
  ↓
输出: /data/clean/ (Parquet, 按date分区, 9个分区)
```

### 4.2 Task2: 核心统计分析 (`task2_main.py`)

```
输入: /data/clean/ (Parquet)
  ↓
10个分析模块 (顺序执行):
  2.1  每日UV/PV统计        → daily_uv_pv.json
  2.2  日留存率分析          → retention_rate.json
  2.3  RFM用户价值分层       → rfm_segments.json, rfm_score_distribution.json, rfm_detail/
  2.4  用户行为路径分析      → behavior_paths.json, behavior_paths_3step.json
  2.5  用户生命周期分析      → user_lifecycle.json
  2.6  时间维度深度分析      → hourly_cvr.json, weekday_vs_weekend.json, daily_funnel_trend.json
  2.7  商品/类目热度排行     → item_hot_rank.json, category_hot_rank.json
  2.8  商品转化率分析        → item_cvr_top.json, item_low_cvr.json, category_cvr.json
  2.9  转化漏斗分析          → conversion_funnel.json, conversion_funnel_detail.json
  2.10 行为分布概览          → behavior_distribution.json, active_days_distribution.json, session_stats.json, first_purchase_gap.json
  ↓
输出: /data/output/ (22个JSON文件)
```

### 4.3 Export: 数据导出

```
HDFS /data/output/*.json
  ↓ hdfs dfs -getmerge
本地 ~/webapp/data/*.json
  ↓
Flask API 读取并提供服务
```

---

## 5. 目录结构

```
~/                                    # hadoop用户主目录
├── task1_data_cleaning.py            # 数据清洗脚本
├── task2_main.py                     # 核心分析入口
├── config.yaml                       # 配置文件 (RFM阈值, 输出路径等)
├── run_all.sh                        # 一键运行脚本 (自动检查/清理/重试)
├── lib/
│   ├── data_loader.py                # 数据加载与校验
│   ├── json_writer.py                # JSON输出工具
│   └── analyses/
│       ├── user_daily.py             # 每日UV/PV
│       ├── user_retention.py         # 留存率
│       ├── user_rfm.py               # RFM分层
│       ├── user_path.py              # 行为路径
│       ├── user_lifecycle.py         # 生命周期
│       ├── user_time.py              # 时间分析
│       ├── product_hot.py            # 商品热度
│       ├── product_cvr.py            # 商品转化率
│       ├── funnel.py                 # 转化漏斗
│       └── overview.py               # 行为概览
├── webapp/
│   ├── index.html                    # 前端页面
│   └── data/                         # JSON数据文件 (22个)
└── backend/
    ├── app.py                        # Flask API后端
    └── flask.log                     # 运行日志
```

---

## 6. 前端架构

### 6.1 技术栈
- **HTML5** + **CSS3** (响应式设计)
- **ECharts 5.5.1** (CDN加载)
- **原生 JavaScript** (无框架依赖)

### 6.2 设计风格
- **Editorial/Magazine** 风格
- 字体: DM Serif Display (标题) + Source Serif 4 (正文) + JetBrains Mono (数据)
- 配色: 暖白背景 #F8F5F0, 赭石强调色 #C65D3E
- 响应式断点: 1024px (平板), 640px (手机)

### 6.3 图表列表 (19个)
| 图表 | 类型 | 数据源 |
|------|------|--------|
| PV/UV趋势 | 折线图 | daily_uv_pv.json |
| 行为分布 | 饼图 | behavior_distribution.json |
| 小时分布 | 柱状图 | hourly_cvr.json |
| 转化漏斗 | 漏斗图 | conversion_funnel.json |
| 商品热度TOP | 横向柱状图 | item_hot_rank.json |
| 类目热度TOP | 横向柱状图 | category_hot_rank.json |
| 留存率趋势 | 折线图 | retention_rate.json |
| RFM分层 | 饼图+柱状图 | rfm_segments.json |
| 用户生命周期 | 饼图 | user_lifecycle.json |
| 行为路径 | 桑基图 | behavior_paths.json |
| 工作日vs周末 | 对比柱状图 | weekday_vs_weekend.json |
| 商品转化率TOP | 横向柱状图 | item_cvr_top.json |
| 类目转化率 | 横向柱状图 | category_cvr.json |
| 漏斗趋势 | 折线图 | daily_funnel_trend.json |
| RFM评分分布 | 柱状图 | rfm_score_distribution.json |
| 首次购买间隔 | 柱状图 | first_purchase_gap.json |
| 活跃天数分布 | 柱状图 | active_days_distribution.json |
| 会话统计 | 指标卡 | session_stats.json |
| 低转化商品 | 横向柱状图 | item_low_cvr.json |

### 6.4 交互功能
- **日期筛选**: 选择起止日期过滤数据
- **饼图联动**: 点击行为分布饼图筛选对应行为
- **全屏模式**: 点击图表右上角按钮全屏查看
- **图表克隆**: 全屏时独立渲染，不影响原图

---

## 7. 后端 API

### 7.1 Flask API 端点

| 端点 | 方法 | 参数 | 返回 |
|------|------|------|------|
| `/api/dates` | GET | - | `["2017-11-25", ...]` |
| `/api/data/<filename>` | GET | `?date_start=&date_end=` | JSON数组 |
| `/api/overview` | GET | - | 行为概览数据 |
| `/api/search?q=` | GET | `q` 搜索词 | 商品搜索结果 |

### 7.2 数据加载策略
1. 优先从本地 `~/webapp/data/` 读取 JSON
2. 本地不存在时，从 HDFS `/data/output/` 读取
3. Flask 启动时预加载 Parquet 数据到内存（用于实时查询）

---

## 8. 配置文件 (config.yaml)

```yaml
data:
  input_path: "/data/clean"
  output_path: "/data/output"

analysis:
  retention_days: [1, 3, 7]        # 留存率天数
  rfm:
    weights: {r: 0.4, f: 0.6}      # RFM权重
    thresholds:
      high_value: 3.8               # 高价值用户阈值
      potential: 2.6                # 潜力用户阈值
      normal: 1.4                   # 一般用户阈值
      pre_churn: 1.0                # 预流失用户阈值
  top_n:
    hot_item: 10                    # 热门商品TOP-N
    cvr_item: 10                    # 高转化商品TOP-N
    low_cvr_item: 10                # 低转化商品TOP-N
  min_pv_for_cvr: 5                 # 转化率计算最低浏览量

spark:
  app_name: "UserBehaviorAnalysis"
  timezone: "Asia/Shanghai"
```

---

## 9. 运行脚本 (run_all.sh)

### 功能
- 自动检查磁盘空间和 HDFS 空间
- 自动清理残留 Spark 进程
- 自动清理旧输出目录（避免 FileAlreadyExistsException）
- 支持分步运行: `bash run_all.sh [task1|task2|export|all]`
- 超时保护: task1 60分钟, task2 30分钟

### 用法
```bash
bash run_all.sh all       # 完整流程
bash run_all.sh task1     # 仅数据清洗
bash run_all.sh task2     # 仅统计分析
bash run_all.sh export    # 仅导出数据
```

---

## 10. 已知限制

1. **内存不足**: 集群总内存 17.1GB，Spark 处理 1 亿条数据时内存紧张
2. **磁盘空间**: 各节点 19-38GB，Spark shuffle 需要大量临时空间
3. **单点故障**: 无 HA 配置，master 故障则系统不可用
4. **数据量**: 仅 9 天数据，留存率等需要长时间窗口的分析不够精确
5. **无金额数据**: RFM 分析缺少 Monetary 维度
