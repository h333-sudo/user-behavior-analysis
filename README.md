# 淘宝用户行为多维分析系统

> 基于 Hadoop HDFS + PySpark + Flask + ECharts 的电商用户行为分析平台

基于阿里巴巴淘宝用户行为数据集（约1亿条记录），构建完整的电商用户行为分析流水线：从数据清洗、多维统计分析到交互式可视化，覆盖用户价值分层、转化漏斗、行为路径、留存分析等核心电商指标。

## 系统架构

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   HDFS      │───▶│  PySpark    │───▶│  Flask API  │───▶│  ECharts    │
│  CSV 3.67GB │    │  10个分析模块│    │  REST JSON  │    │  19个图表   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
   分布式存储          分布式计算          API服务层          前端可视化
```

## 快速开始

```bash
# 1. 数据清洗（CSV → Parquet）
bash run_all.sh task1

# 2. 运行全部10个分析模块
bash run_all.sh task2

# 3. 选择性运行
spark-submit task2_main.py --list              # 列出所有模块
spark-submit task2_main.py --only 2.1 2.8      # 只跑指定模块
spark-submit task2_main.py --skip 2.4          # 跳过指定模块
spark-submit task2_main.py --group user        # 只跑某分组

# 4. 导出数据并启动服务
bash run_all.sh export
cd backend && python3 app.py &                 # API服务（端口5000）
cd task3_visualization && python3 -m http.server 8088 &  # 前端（端口8088）
```

## 项目结构

```
├── config.yaml                  # 全局配置（阈值、路径、模块开关）
├── task1_data_cleaning.py       # 数据清洗流水线
├── task2_main.py                # 分析主入口（编排10个模块）
├── run_all.sh                   # 自动化运行脚本（磁盘检查、超时、清理）
│
├── lib/                         # 核心分析库
│   ├── data_loader.py           # Parquet数据加载与验证
│   ├── json_writer.py           # DataFrame → JSON导出
│   └── analyses/                # 10个自注册分析模块
│       ├── user_daily.py        #   2.1 每日UV/PV统计
│       ├── user_retention.py    #   2.2 日留存率分析
│       ├── user_rfm.py          #   2.3 RFM用户价值分层
│       ├── user_path.py         #   2.4 用户行为路径（会话级）
│       ├── user_lifecycle.py    #   2.5 用户生命周期分析
│       ├── user_time.py         #   2.6 时间维度深度分析
│       ├── product_hot.py       #   2.7 商品/类目热度排行
│       ├── product_cvr.py       #   2.8 商品转化率分析
│       ├── funnel.py            #   2.9 转化漏斗分析
│       └── overview.py          #   2.10 行为分布概览
│
├── backend/
│   └── app.py                   # Flask REST API
│
├── task3_visualization/         # 前端可视化
│   ├── index.html               # 单页布局
│   ├── css/style.css            # 编辑杂志风格样式
│   ├── js/
│   │   ├── utils.js             # 数据加载、日期筛选、全屏
│   │   ├── app.js               # 主调度函数
│   │   └── demo.js              # 演示备用数据
│   └── charts/                  # 20个自注册图表模块
│       ├── registry.js          # ChartRegistry注册表
│       └── *.js                 # 各图表文件
│
└── report/                      # 课程报告（生成脚本）
    ├── generate_report.js
    └── generate_appendix.js
```

## 分析模块

| 编号 | 模块 | 核心公式 | 输出文件 |
|------|------|----------|----------|
| 2.1 | 每日UV/PV | `UV = countDistinct(user_id)` | `daily_uv_pv.json` |
| 2.2 | 留存率 | `rate = retained_dayN / new_users × 100%` | `retention_rate.json` |
| 2.3 | RFM分层 | `score = R×0.4 + F×0.6` → 5级分层 | `rfm_segments.json` |
| 2.4 | 行为路径 | 会话级 `lag()` 窗口函数，30分钟会话分割 | `behavior_paths.json` |
| 2.5 | 生命周期 | 新用户/回访用户、首购间隔 | `user_lifecycle.json` |
| 2.6 | 时间分析 | 每小时CVR、工作日vs周末 | `hourly_cvr.json` |
| 2.7 | 热度排行 | `total = pv + fav + cart + buy` | `item_hot_rank.json` |
| 2.8 | 商品CVR | `CVR = buy_uv / interest_uv × 100%` | `item_cvr_top.json` |
| 2.9 | 转化漏斗 | 严格递进：`pv ⟶ cart/fav ⟶ buy`（inner join） | `conversion_funnel.json` |
| 2.10 | 行为分布 | `count(*)` 按行为类型分组 | `behavior_distribution.json` |

## 核心设计

**严格递进漏斗** — 每阶段通过 `inner join` 确保是上阶段的子集，转化率单调递减。正确处理了数据集的跨期效应（46.4%的购买在当天无浏览记录）。

**模块自注册** — 每个模块声明 `MODULE_ID`、`MODULE_NAME`、`MODULE_ENABLED` 元数据，`__init__.py` 通过 `pkgutil` 自动发现。三级启用控制：CLI参数 > config.yaml > MODULE_ENABLED。

**图表注册表** — 前端每个图表调用 `registerChart()` 注册 `id`、`tab`、`dataFile`、`render()`。`app.js` 通过 `ChartRegistry` 收集数据文件名，`Promise.all` 并行加载，按 `dataFile` 分发渲染。

## 数据集

阿里巴巴淘宝用户行为数据集（UserBehavior）：
- **数据量**：约1亿条行为记录（pv/cart/fav/buy）
- **用户数**：987,991 独立用户
- **时间窗口**：9天（2017-11-25 至 2017-12-03）
- **字段**：user_id, item_id, category_id, behavior, timestamp

## 技术栈

| 层级 | 技术 | 版本 |
|------|------|------|
| 存储层 | Hadoop HDFS | 3.3.6 |
| 计算层 | PySpark | 3.5.8 |
| 服务层 | Flask | 2.x |
| 展示层 | ECharts | 5.5.1 |
| 语言 | Python | 3.12.3 |

## License

高校课程作业，仅供学习参考。
