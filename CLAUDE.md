# CLAUDE.md — 项目指南

## 项目概述

淘宝用户行为数据分析系统。基于 Hadoop HDFS + PySpark + Flask + ECharts，分析 1 亿条用户行为数据。

**数据范围**: 2017.11.25 - 2017.12.03（共9天）
**数据规模**: 3.67GB CSV → 1.9GB Parquet → 22个 JSON 分析结果

---

## 了解项目必读文件

| 文件 | 内容 | 优先级 |
|------|------|--------|
| `system_architecture.md` | 系统架构、数据流水线、前后端设计 | ⭐⭐⭐ |
| `metrics_analysis.md` | 10大指标的计算方式与合理性分析 | ⭐⭐⭐ |
| `fixing.md` | 当前待修复问题清单 | ⭐⭐ |
| `config.yaml` | RFM阈值、TOP-N数量、输出路径等配置 | ⭐⭐ |
| `run_all.sh` | 一键运行脚本（含自动检查/清理逻辑） | ⭐ |

---

## 项目规范

### 代码规范

1. **分析模块**: 每个模块放在 `lib/analyses/` 下，暴露 `run(df, config, save_fn)` 函数
2. **输出格式**: 用 `save_fn(dataframe, "filename.json", "描述")` 写入 HDFS
3. **DataFrame API**: 优先用 PySpark DataFrame API，避免直接写 SQL
4. **除零保护**: 所有除法用 `when(col > 0, ...).otherwise(0.0)` 保护
5. **用户数统计**: 用 `countDistinct("user_id")` 而非 `count("*")`，避免重复计数

### 前端规范

1. **数据加载**: 统一通过 `getData(filename)` 函数走 Flask API
2. **API 地址**: `const API_BASE = window.location.protocol + '//' + window.location.hostname + ':5000'`
3. **图表注册**: 新图表加入 `allCharts` 对象，支持全屏模式
4. **响应式**: 断点 1024px（平板）、640px（手机）

### 运行规范

1. **跑数据前**: 必须告知用户确认（见 remind-before-run.md）
2. **清理旧数据**: `saveAsTextFile` 不允许目录已存在，运行前必须删除旧输出
3. **检查磁盘**: 运行前检查本地磁盘 ≥ 5GB、HDFS ≥ 5GB
4. **副本因子**: 小集群用 `--conf spark.hadoop.dfs.replication=1`
5. **Spark 内存**: 推荐 `--conf spark.driver.memory=2g`

---

## 注意事项

### 易踩坑点

1. **SSH Key 路径**: 是 `D:\program\hadoop-key\id_rsa`，不是 `D:\program\hadoop-key`（那是目录）
2. **Spark 路径不同**: master 用 `/usr/local/spark/bin/spark-submit`，slave 用 `/home/hadoop/spark/bin/spark-submit`
3. **Flask 端口**: API 在 5000，前端在 8088，不要搞混
4. **HDFS 副本因子**: 默认 3，小集群磁盘会爆，必须改为 1
5. **/swap.img**: 每个节点有 4.1GB swap 文件，磁盘紧张时可缩小
6. **saveAsTextFile**: 写入前目录必须不存在，否则报 FileAlreadyExistsException
7. **CVR 公式**: 统一用 `countDistinct(buy用户) / countDistinct(pv+cart+fav用户) × 100`，不能用总次数
8. **漏斗必须严格递进**: 每阶段是前一阶段的子集，不能独立统计各行为人数
9. **路径分析必须在会话内**: `lag()` 窗口用 `partitionBy("user_id", "session_id")`，不能跨会话

### 当前待修复问题

见 `fixing.md`。主要：
- 商品转化率指标名 `pv_to_buy_rate` 不准确（分母已改为 pv+cart+fav 用户数）
- 漏斗已改为严格递进式，需重新运行 task2 生效

### 资源限制

- 集群总内存: 17.1GB（3 × 5.7GB）
- 单节点磁盘: 19-38GB
- Spark shuffle 需要大量临时空间，建议 `spark.sql.shuffle.partitions=8`
- YARN 模式资源不足时，回退到 `local[2]` 模式

---

## 快速命令参考

```bash
# SSH 连接
ssh -i /d/program/hadoop-key/id_rsa hadoop@192.168.131.131

# 一键运行
bash run_all.sh all

# 分步运行
bash run_all.sh task1    # 数据清洗
bash run_all.sh task2    # 统计分析
bash run_all.sh export   # 导出 JSON

# 检查状态
hdfs dfs -ls /data/clean/
hdfs dfs -ls /data/output/
hdfs dfsadmin -report | head -10
ps aux | grep SparkSubmit | grep -v grep
curl -s http://localhost:5000/api/dates

# 重启服务
ps aux | grep app.py | awk '{print $2}' | xargs kill -9
cd ~/backend && nohup python3 app.py > flask.log 2>&1 &
```
