# 淘宝用户行为数据分析系统

> 高校大数据课程作业 | Hadoop HDFS + PySpark + Flask + ECharts

基于阿里巴巴淘宝用户行为数据集（UserBehavior），对约1亿条用户行为记录（2017.11.25—2017.12.03）进行多维度分布式分析，并通过Web可视化展示结果。

## 项目架构

```
数据流：HDFS(CSV) → PySpark清洗(Parquet) → 10个分析模块(JSON) → Flask API → ECharts前端
```

- **存储层**：Hadoop HDFS（分布式存储，副本因子=1适配小集群）
- **计算层**：PySpark DataFrame API（local[2]或YARN模式）
- **服务层**：Flask REST API（端口5000，支持日期过滤）
- **展示层**：ECharts 5.5.1（19个交互式图表，5个Tab页）

## 快速开始

```bash
# 1. 数据清洗
bash run_all.sh task1

# 2. 运行全部分析（10个模块）
bash run_all.sh task2

# 3. 选择性运行
spark-submit task2_main.py --list              # 列出所有模块
spark-submit task2_main.py --only 2.1 2.8      # 只跑指定模块
spark-submit task2_main.py --skip 2.4          # 跳过指定模块
spark-submit task2_main.py --group user        # 只跑某分组

# 4. 导出数据到前端
bash run_all.sh export

# 5. 启动服务
cd backend && python3 app.py &                 # Flask API (端口5000)
cd task3_visualization && python3 -m http.server 8088 &  # 前端 (端口8088)
```

## 项目结构

```
user_behavior_analysis/
├── config.yaml                        # 全局配置（阈值、路径、模块开关）
├── task1_data_cleaning.py             # 数据清洗（CSV→Parquet）
├── task2_main.py                      # 分析主入口（编排10个模块）
├── run_all.sh                         # 自动化运行脚本
│
├── lib/                               # 核心分析库
│   ├── data_loader.py                 # Parquet数据加载与验证
│   ├── json_writer.py                 # DataFrame→JSON写入HDFS
│   └── analyses/                      # 10个分析模块（自注册）
│       ├── __init__.py                # discover_analyses()自动发现
│       ├── user_daily.py     (2.1)    # 每日UV/PV统计
│       ├── user_retention.py (2.2)    # 日留存率分析
│       ├── user_rfm.py       (2.3)    # RFM用户价值分层
│       ├── user_path.py      (2.4)    # 用户行为路径分析
│       ├── user_lifecycle.py (2.5)    # 用户生命周期分析
│       ├── user_time.py      (2.6)    # 时间维度深度分析
│       ├── product_hot.py    (2.7)    # 商品/类目热度排行
│       ├── product_cvr.py    (2.8)    # 商品转化率分析
│       ├── funnel.py         (2.9)    # 转化漏斗分析
│       └── overview.py       (2.10)   # 行为分布概览
│
├── backend/                           # Flask API后端
│   └── app.py                         # REST API（端口5000）
│
├── task3_visualization/               # 前端可视化
│   ├── index.html                     # 主页面（纯布局）
│   ├── css/style.css                  # 编辑杂志风格样式
│   ├── js/
│   │   ├── utils.js                   # 工具函数+数据加载
│   │   ├── app.js                     # 主调度函数
│   │   └── demo.js                    # 演示备用数据
│   └── charts/                        # 20个独立图表模块
│       ├── registry.js                # ChartRegistry注册表
│       ├── dailyUVPV.js               # 每日UV/PV趋势
│       ├── behaviorDist.js            # 行为分布饼图
│       ├── funnel.js                  # 转化漏斗
│       └── ...                        # 共20个图表
│
└── report/                            # 课程报告
    ├── generate_report.js             # 报告生成脚本
    └── generate_appendix.js           # 附录生成脚本
```

## 分析模块

| 编号 | 模块 | 输出 | 说明 |
|------|------|------|------|
| 2.1 | 每日UV/PV统计 | `daily_uv_pv.json` | 每日独立用户数和页面浏览量 |
| 2.2 | 日留存率分析 | `retention_rate.json` | 次日/3日/7日留存率 |
| 2.3 | RFM用户价值分层 | `rfm_segments.json` | R×0.4+F×0.6加权评分，5级分层 |
| 2.4 | 用户行为路径 | `behavior_paths.json` | 会话级2步/3步行为序列 |
| 2.5 | 用户生命周期 | `user_lifecycle.json` | 新用户/回访用户、首购间隔 |
| 2.6 | 时间维度分析 | `hourly_cvr.json` | 每小时转化率、工作日vs周末 |
| 2.7 | 商品/类目热度 | `item_hot_rank.json` | 按行为次数TOP排名 |
| 2.8 | 商品转化率 | `item_cvr_top.json` | CVR=buy_uv/interest_uv |
| 2.9 | 转化漏斗 | `conversion_funnel.json` | 严格递进：pv→cart/fav→buy |
| 2.10 | 行为分布概览 | `behavior_distribution.json` | pv/cart/fav/buy占比 |

## 核心设计

- **模块自注册**：每个分析模块声明MODULE_ID/MODULE_NAME/MODULE_ENABLED等元数据，`__init__.py`自动发现
- **三级启用控制**：CLI参数 > config.yaml disabled > MODULE_ENABLED
- **图表注册表**：前端每个图表调用registerChart()自注册，app.js通过ChartRegistry自动分发数据
- **严格递进漏斗**：通过inner join确保每阶段是上阶段的子集

## 环境要求

- Hadoop 3.x + Spark 3.x（local或YARN模式）
- Python 3.8+ + PySpark + PyYAML
- Flask + flask-cors（后端API）
- 浏览器（查看ECharts前端）

## 数据集

阿里巴巴淘宝用户行为数据集（UserBehavior）：
- 约1亿条行为记录（pv/cart/fav/buy）
- 987,991独立用户，9天时间窗口（2017.11.25—2017.12.03）
- 字段：user_id, item_id, category_id, behavior, timestamp

## License

本项目为高校课程作业，仅供学习参考。
