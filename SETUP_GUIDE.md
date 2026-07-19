# 项目部署与运行指南

> 从零开始跑通整个项目：环境搭建 → 数据清洗 → 分析计算 → 前端部署

---

## 一、环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| Ubuntu | 20.04 LTS | 集群操作系统 |
| Hadoop | 3.3.6 | HDFS 分布式存储 |
| Spark | 3.5.8 | PySpark 计算引擎 |
| Python | 3.12.3 | 脚本语言 |
| Java | OpenJDK 8 | Hadoop/Spark 依赖 |
| Flask | 2.x | API 后端 |
| ECharts | 5.5.1 | 前端图表库（CDN引入） |

---

## 二、集群规划（3节点）

| 节点 | IP | 角色 |
|------|------|------|
| master | 192.168.131.131 | NameNode + DataNode + ResourceManager |
| slave1 | 192.168.131.132 | DataNode + NodeManager |
| slave2 | 192.168.131.133 | DataNode + NodeManager |

---

## 三、Linux 项目目录结构

项目部署在集群 master 节点的 `/home/hadoop/` 目录下：

```
/home/hadoop/
├── user_behavior_analysis/              # 项目源码
│   ├── config.yaml                      # 全局配置
│   ├── task1_data_cleaning.py           # 数据清洗
│   ├── task2_main.py                    # 分析主入口
│   ├── run_all.sh                       # 自动化运行脚本
│   ├── lib/
│   │   ├── data_loader.py               # 数据加载
│   │   ├── json_writer.py               # JSON导出
│   │   └── analyses/                    # 10个分析模块
│   │       ├── __init__.py              # 自动发现
│   │       ├── user_daily.py            # 2.1 每日UV/PV
│   │       ├── user_retention.py        # 2.2 留存率
│   │       ├── user_rfm.py              # 2.3 RFM分层
│   │       ├── user_path.py             # 2.4 行为路径
│   │       ├── user_lifecycle.py        # 2.5 生命周期
│   │       ├── user_time.py             # 2.6 时间分析
│   │       ├── product_hot.py           # 2.7 热度排行
│   │       ├── product_cvr.py           # 2.8 商品CVR
│   │       ├── funnel.py                # 2.9 转化漏斗
│   │       └── overview.py              # 2.10 行为分布
│   └── backend/
│       └── app.py                       # Flask API
│
├── webapp/                              # 前端部署目录
│   ├── index.html                       # 主页面
│   ├── css/style.css                    # 样式
│   ├── js/
│   │   ├── utils.js                     # 工具函数
│   │   ├── app.js                       # 主调度
│   │   └── demo.js                      # 备用数据
│   ├── charts/                          # 20个图表模块
│   └── data/                            # JSON数据文件（22个）
│       ├── daily_uv_pv.json
│       ├── retention_rate.json
│       ├── rfm_segments.json
│       ├── conversion_funnel.json
│       └── ...
│
└── data/                                # HDFS数据目录
    └── raw/
        └── user.csv                     # 原始数据（3.67GB）
```

HDFS 上的目录结构：

```
hdfs:///data/
├── raw/
│   └── user.csv                         # 原始CSV（3.67GB）
├── clean/                               # 清洗后Parquet（1.9GB，按日期分区）
│   ├── _SUCCESS
│   ├── date=2017-11-25/
│   ├── date=2017-11-26/
│   └── ...
└── output/                              # 分析结果（22个JSON目录）
    ├── daily_uv_pv.json/
    ├── retention_rate.json/
    └── ...
```

---

## 四、Step by Step 运行指南

### Step 1：上传原始数据到 HDFS

```bash
# 登录 master 节点
ssh hadoop@192.168.131.131

# 创建 HDFS 目录
hdfs dfs -mkdir -p /data/raw

# 上传 CSV 文件（3.67GB，约需5-10分钟）
hdfs dfs -put ~/user.csv /data/raw/user.csv

# 验证上传成功
hdfs dfs -ls -h /data/raw/user.csv
# 输出应显示约 3.4G

# 检查数据行数（约1亿行）
hdfs dfs -cat /data/raw/user.csv | wc -l
```

### Step 2：数据清洗（task1）

```bash
cd ~/user_behavior_analysis

# 运行数据清洗
PYSPARK_PYTHON=python3 /usr/local/spark/bin/spark-submit \
  --master local[2] \
  --conf spark.sql.session.timeZone=Asia/Shanghai \
  --conf spark.driver.memory=2g \
  --conf spark.hadoop.dfs.replication=1 \
  task1_data_cleaning.py

# 预期输出：
# [任务1] 原始数据总行数：100150807
# [任务1] 过滤时间范围后行数：100095182
# [任务1] 去重后行数：100095182
# [任务1] 数据清洗完成！
# [任务1] 最终数据行数：100095182
# [任务1] 输出路径：/data/clean/ （Parquet格式，按date分区）

# 验证输出
hdfs dfs -ls /data/clean/ | head -5
hdfs dfs -ls -h /data/clean/date=2017-11-25/ | head -3
```

### Step 3：运行分析模块（task2）

```bash
# 方式1：运行全部10个模块（约30-40分钟）
cd ~/user_behavior_analysis
bash run_all.sh task2

# 方式2：选择性运行
# 列出所有模块及其状态
spark-submit task2_main.py --list

# 只运行指定模块
spark-submit task2_main.py --only 2.1 2.8

# 跳过指定模块
spark-submit task2_main.py --skip 2.4

# 只运行某分组
spark-submit task2_main.py --group user

# 强制启用被config.yaml禁用的模块
spark-submit task2_main.py --enable 2.4

# 预期输出：
# 将运行 10 个分析：
#   [2.1] 每日UV/PV统计
#   [2.2] 日留存率分析
#   ...
# ──────────────────────────────────────────
#   ✓ 成功     8.2s  2.1 每日UV/PV统计
#   ✓ 成功    12.5s  2.2 日留存率分析
#   ...
# ──────────────────────────────────────────
#   总耗时: 1847.3s
#   输出路径: /data/output/
```

### Step 4：导出数据到前端目录

```bash
# 导出 JSON 文件到 webapp/data/
bash run_all.sh export

# 预期输出：
# [✓] HDFS 输出: 22 个文件
# [✓] 导出完成: 成功 22, 失败 0
# [✓] 数据目录: /home/hadoop/webapp/data

# 验证导出
ls -lh ~/webapp/data/*.json | wc -l
# 输出：22
```

### Step 5：启动 Flask API

```bash
# 安装依赖
pip3 install flask flask-cors findspark pyyaml

# 启动 Flask（后台运行）
cd ~/user_behavior_analysis/backend
nohup python3 app.py > flask.log 2>&1 &

# 等待数据加载（约30秒）
sleep 30

# 验证 API
curl http://localhost:5000/api/dates
# 输出：["2017-11-25","2017-11-26",...,"2017-12-03"]

curl http://localhost:5000/api/overview
# 输出：{"total_behavior":100095182,"uv":987991,...}
```

### Step 6：启动前端 HTTP 服务

```bash
# 启动前端（后台运行，端口8088）
cd ~/webapp
nohup python3 -m http.server 8088 --bind 0.0.0.0 > http.log 2>&1 &

# 验证前端
curl -s http://localhost:8088/ | head -5
# 输出：<!DOCTYPE html>...
```

### Step 7：浏览器访问

打开浏览器访问：`http://192.168.131.131:8088/`

应看到：
- 5个 Tab 页（行为概览、用户分析、商品分析、转化漏斗、时间分析）
- 19个交互式图表
- 数据来源提示显示"✓ 已加载真实分析数据"

---

## 五、一键运行脚本

`run_all.sh` 封装了完整的运行流程：

```bash
# 查看帮助
bash run_all.sh

# 完整流程：task1 → task2 → export
bash run_all.sh all

# 单独运行
bash run_all.sh task1    # 数据清洗
bash run_all.sh task2    # 全部分析
bash run_all.sh export   # 导出数据

# 选择性运行 task2
bash run_all.sh task2:2.1,2.8          # 只跑 2.1 和 2.8
bash run_all.sh task2:skip:2.4         # 跳过 2.4
bash run_all.sh task2:enable:2.4       # 强制启用 2.4
```

---

## 六、运行/调试常见问题

### 问题1：YARN 内存不足

**现象**：
```
Required executor memory 2048MB is above the max threshold 2048MB
```

**原因**：YARN 的 `yarn.scheduler.maximum-allocation-mb` 默认为 2048MB，Spark executor 请求的内存超过上限。

**解决**：
```bash
# 方案1：降低 executor 内存
spark-submit --conf spark.executor.memory=1g ...

# 方案2：回退到 local 模式（推荐小集群）
spark-submit --master local[2] ...
```

### 问题2：HDFS 副本因子导致磁盘不足

**现象**：
```
java.io.IOException: No space left on device
```

**原因**：HDFS 默认副本因子为 3，3.6GB 数据实际占用约 11GB，加上 Spark shuffle 需要 2-3 倍临时空间。

**解决**：
```bash
# 降低副本因子为 1
hdfs dfs -setrep -w 1 /data/raw/user.csv

# Spark 配置也设置副本因子
spark-submit --conf spark.hadoop.dfs.replication=1 ...
```

### 问题3：FileAlreadyExistsException

**现象**：
```
org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs:///data/output/daily_uv_pv.json already exists
```

**原因**：Spark 的 `saveAsTextFile` 要求目标目录不存在，重复运行时旧输出目录残留。

**解决**：
```bash
# 手动删除旧输出
hdfs dfs -rm -r -f /data/output/daily_uv_pv.json

# 或使用 run_all.sh（自动清理）
bash run_all.sh task2
```

### 问题4：PySpark 找不到 Python

**现象**：
```
Python version 2.7 is not supported
```

**原因**：Spark 默认使用系统的 Python 2.7。

**解决**：
```bash
# 设置环境变量
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# 或写入 ~/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
source ~/.bashrc
```

### 问题5：Flask 启动失败

**现象**：
```
ModuleNotFoundError: No module named 'flask'
```

**解决**：
```bash
pip3 install flask flask-cors findspark pyyaml
```

**现象**：Flask 启动后端口 5000 无法访问。

**原因**：Flask 默认只监听 localhost，不监听外部 IP。

**解决**：
```python
# app.py 中确保绑定 0.0.0.0
app.run(host="0.0.0.0", port=5000, debug=False)
```

### 问题6：前端图表不显示

**现象**：页面加载但图表区域空白。

**排查步骤**：
```bash
# 1. 检查 Flask 是否运行
curl http://localhost:5000/api/dates

# 2. 检查 HTTP 服务是否运行
curl http://localhost:8088/

# 3. 检查 JSON 文件是否存在
ls ~/webapp/data/*.json | wc -l

# 4. 检查浏览器控制台（F12）是否有错误
```

**常见原因**：
- Flask 未启动 → 启动 Flask
- JSON 文件缺失 → 重新 `bash run_all.sh export`
- 浏览器缓存 → Ctrl+F5 强制刷新

### 问题7：Spark shuffle 磁盘空间不足

**现象**：
```
java.io.IOException: No space left on device
```

**原因**：Spark shuffle 需要 2-3 倍输入数据的临时磁盘空间。

**解决**：
```bash
# 检查磁盘空间
df -h

# 清理临时文件
hdfs dfs -rm -r -f /tmp/spark-*
rm -rf /tmp/spark-*

# 如果空间不足，考虑扩容磁盘或降低副本因子
```

### 问题8：task2 超时

**现象**：`run_all.sh` 中 task2 超过 60 分钟超时。

**原因**：`countDistinct` 比 `count` 慢很多，需要额外的 shuffle 操作。

**解决**：
```bash
# 方案1：增加超时时间（修改 run_all.sh）
timeout 120 spark-submit ...

# 方案2：分批运行
spark-submit task2_main.py --only 2.1 2.2 2.3
spark-submit task2_main.py --only 2.4 2.5 2.6
spark-submit task2_main.py --only 2.7 2.8 2.9 2.10
```

### 问题9：NodeManager 未注册到 ResourceManager

**现象**：YARN 界面显示活跃节点数为 0。

**排查**：
```bash
# 检查 NodeManager 状态
yarn node -list

# 检查 NodeManager 日志
tail -100 ~/hadoop/logs/yarn-hadoop-nodemanager-*.log

# 重启 NodeManager
yarn-daemon.sh stop nodemanager
yarn-daemon.sh start nodemanager
```

### 问题10：数据行数不对

**现象**：清洗后数据不是约 1 亿行。

**排查**：
```bash
# 检查原始数据行数
hdfs dfs -cat /data/raw/user.csv | wc -l

# 检查清洗后数据行数
hdfs dfs -cat /data/clean/date=*/*.parquet | wc -l
# 注意：Parquet 是二进制格式，不能直接 cat，需要用 Spark 读取

# 用 PySpark 检查
pyspark -c "spark.read.parquet('/data/clean/').count()"
```

---

## 七、配置文件说明

`config.yaml` 集中管理所有可调参数：

```yaml
# 数据路径
data:
  raw_path: hdfs:///data/raw/user.csv
  clean_path: /data/clean/
  output_path: /data/output/

# Spark 配置
spark:
  app_name: UserBehavior_Task2
  timezone: Asia/Shanghai

# 分析参数
analysis:
  session_gap_minutes: 30       # 会话分割阈值（分钟）
  min_pv_for_cvr: 10           # CVR 计算最低 PV 阈值
  disabled: []                 # 禁用的模块 ID，如 [2.4, 2.8]

  # RFM 参数
  rfm:
    r_weight: 0.4              # R 权重
    f_weight: 0.6              # F 权重
    r_thresholds: [1, 3, 5, 7] # R 评分阈值
    f_thresholds: [1, 2, 4, 9] # F 评分阈值
    segment_thresholds:        # 分层阈值
      high_value: 4.0
      potential: 3.0
      normal: 2.0
      pre_churn: 1.4

  # TOP-N 配置
  top_n:
    item: 50                   # 商品热度排行数量
    category: 30               # 类目热度排行数量
```

---

## 八、常用运维命令

```bash
# 查看 HDFS 空间使用
hdfs dfs -du -h /data/

# 查看 Spark 任务状态
yarn application -list

# 杀死正在运行的 Spark 任务
yarn application -kill <application_id>

# 查看 Flask 日志
tail -f ~/user_behavior_analysis/backend/flask.log

# 查看 HTTP 服务日志
tail -f ~/webapp/http.log

# 重启所有服务
pkill -f "python3 app.py"
pkill -f "http.server 8088"
cd ~/user_behavior_analysis/backend && nohup python3 app.py > flask.log 2>&1 &
cd ~/webapp && nohup python3 -m http.server 8088 --bind 0.0.0.0 > http.log 2>&1 &

# 检查端口占用
netstat -tlnp | grep -E '5000|8088'
```

---

## 九、面试常见运行问题

**Q: 你是怎么部署这个项目的？**

A: 三节点 Hadoop 集群（master + slave1 + slave2），数据上传到 HDFS，用 PySpark local[2] 模式运行分析，结果导出为 JSON，Flask 提供 API，前端用 HTTP Server 部署。整个流程通过 `run_all.sh` 一键执行。

**Q: 运行过程中遇到过什么问题？**

A: ①磁盘空间不足——HDFS 副本因子 3 导致数据膨胀 3 倍，降为 1 解决；②YARN 资源不足——executor 内存超限，回退 local 模式；③FileAlreadyExistsException——saveAsTextFile 不覆盖旧目录，编写自动清理函数；④countDistinct 性能差——task2 从 15 分钟增至 40 分钟，增加超时时间。

**Q: 为什么用 local 模式而不是 YARN？**

A: 小集群每节点只有 4GB 内存，YARN 的最大分配内存上限也是 2048MB，executor 请求的资源经常超限。local[2] 模式直接在 master 上运行，绕过 YARN 资源调度，更稳定。如果是生产环境有足够节点，应该用 YARN 模式。

**Q: 数据清洗做了什么？为什么？**

A: ①定义 Schema 避免自动推断（性能开销大）；②过滤时间戳异常数据（约 5 万条）；③去重（日志采集可能重复）；④派生 datetime/date/hour 字段；⑤输出 Parquet 格式（列式存储，压缩比 2:1）。

**Q: 怎么保证模块化运行的灵活性？**

A: 三级启用控制——CLI 参数（--only/--skip/--enable）优先级最高，其次是 config.yaml 的 disabled 列表，最后是模块自身的 MODULE_ENABLED 属性。新增模块只需添加一个 .py 文件声明元数据，__init__.py 通过 pkgutil 自动发现。

**Q: 如果要重新跑某个模块怎么办？**

A: `spark-submit task2_main.py --only 2.8` 只跑指定模块。旧输出会被 run_all.sh 自动清理（clean_hdfs_dir 函数），也可以手动 `hdfs dfs -rm -r -f /data/output/<文件名>` 删除后重跑。
