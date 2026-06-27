"""
===============================================================================
任务2：核心统计分析脚本
===============================================================================
功能（共9大分析维度）：

  用户分析：
    2.1  每日UV/PV统计
    2.2  日留存率分析
    2.3  RFM用户分层
    2.4  用户行为路径分析
    2.5  用户画像（高价值用户活跃时段 & 偏好类目）

  商品分析：
    2.6  商品/类目热度排行
    2.7  商品转化率分析

  转化分析：
    2.8  转化漏斗分析
    2.9  行为分布概览

输入：/data/clean/ （任务1输出的Parquet）
输出：/data/output/*.json （JSON格式，供前端ECharts读取）

运行方式：
  spark-submit --master yarn task2_core_statistics.py
===============================================================================
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, min as _min, max as _max,
    datediff, from_unixtime, hour, when, lit, round as _round,
    lag, lead, first, last, collect_list, size, array_contains,
    concat_ws, desc, asc
)
from pyspark.sql.window import Window

# ===========================================================================
# 第一步：创建SparkSession
# ===========================================================================
spark = SparkSession.builder \
    .appName("UserBehavior_CoreStatistics") \
    .getOrCreate()

# 设置时区为北京时间，确保日期函数输出正确
spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")

# ===========================================================================
# 第二步：读取任务1清洗后的Parquet数据
# ===========================================================================
df = spark.read.parquet("/data/clean/")

# 缓存数据到内存，后续多个分析任务都会用到，避免重复读取
df.cache()

total_count = df.count()
print("=" * 60)
print(f"[任务2] 读取清洗数据完成，总行数：{total_count}")
print("[任务2] Schema信息：")
df.printSchema()
df.show(5, truncate=False)
print("=" * 60)

# 定义输出路径前缀
OUTPUT_PATH = "/data/output"


# ===========================================================================
# 工具函数：将DataFrame转为JSON写入HDFS
# ===========================================================================
def save_as_json(dataframe, filename, description=""):
    """
    将DataFrame收集到Driver，转为JSON格式写入HDFS。
    注意：collect()会将所有数据拉到Driver端，仅适用于聚合后的少量结果。

    参数：
      dataframe : 要保存的DataFrame
      filename  : 输出文件名（不含路径）
      description : 打印描述信息
    """
    rows = dataframe.collect()
    # 将Row对象转为字典列表
    data = [row.asDict() for row in rows]

    # 转为JSON字符串
    json_str = json.dumps(data, ensure_ascii=False, indent=2, default=str)

    # 通过SparkContext写入HDFS
    # 使用sc._jsc来写入，或者用pandas中转
    # 这里用最简单的方式：先写到本地再上传，或者直接用Spark的text方式
    # 为简化，我们使用parallelize + saveAsTextFile
    spark.sparkContext.parallelize([json_str]) \
        .coalesce(1) \
        .saveAsTextFile(f"{OUTPUT_PATH}/{filename}")

    print(f"[任务2] ✓ {description} 已保存到 {OUTPUT_PATH}/{filename}")


# ===========================================================================
# 2.1 每日UV（独立访客数）和 PV（页面浏览量）
# ===========================================================================
print("\n" + "=" * 60)
print("2.1 每日UV/PV统计")
print("=" * 60)

# 提取日期字段（前10个字符，即 yyyy-MM-dd）
df = df.withColumn("date", col("datetime").substr(1, 10))

# 按日期聚合：UV = 去重用户数，PV = 总浏览次数（仅behavior='pv'）
df_daily_uv_pv = df.groupBy("date").agg(
    countDistinct("user_id").alias("uv"),           # 独立访客数
    count(when(col("behavior") == "pv", 1)).alias("pv"),  # 浏览量
    count("*").alias("total_behavior")               # 总行为数
).orderBy("date")

df_daily_uv_pv.show(10)
save_as_json(df_daily_uv_pv, "daily_uv_pv.json", "每日UV/PV统计")


# ===========================================================================
# 2.2 日留存率分析
# ===========================================================================
# 留存率 = 第N天仍活跃的用户数 / 第1天的用户数
# 思路：
#   1. 获取每个用户最早出现的日期（首次访问日）
#   2. 获取每个用户所有活跃的日期列表
#   3. 对于每个首次访问日，计算其后第1天、第3天、第7天的留存率
# ===========================================================================
print("\n" + "=" * 60)
print("2.2 日留存率分析")
print("=" * 60)

# 每个用户最早访问日期
df_user_first = df.groupBy("user_id").agg(
    _min("date").alias("first_date")
)

# 每个用户的所有活跃日期（去重）
df_user_dates = df.select("user_id", "date").distinct()

# 关联：得到每个用户首次访问日期 + 每次活跃日期
df_retention = df_user_dates.join(df_user_first, on="user_id", how="inner")

# 计算活跃日期与首次访问日期的天数差
df_retention = df_retention.withColumn(
    "day_diff",
    datediff(col("date"), col("first_date"))
)

# 计算每日新增用户数（首次访问日 = 该日的用户数）
df_new_users = df_user_first.groupBy("first_date").agg(
    countDistinct("user_id").alias("new_users")
).orderBy("first_date")

# 计算第1天、第3天、第7天的留存用户数
retention_days = [1, 3, 7]
df_retention_result = df_new_users

for day in retention_days:
    # 找出在首次访问后第N天仍然活跃的用户
    df_day_n = df_retention.filter(col("day_diff") == day) \
        .groupBy("first_date") \
        .agg(countDistinct("user_id").alias(f"retained_day{day}"))

    # 关联到新增用户表
    df_retention_result = df_retention_result.join(
        df_day_n, on="first_date", how="left"
    )

    # 计算留存率（保留2位小数）
    df_retention_result = df_retention_result.withColumn(
        f"retention_rate_day{day}",
        _round(
            col(f"retained_day{day}") / col("new_users") * 100, 2
        )
    ).fillna(0, subset=[f"retained_day{day}", f"retention_rate_day{day}"])

df_retention_result = df_retention_result.orderBy("first_date")
df_retention_result.show(10)
save_as_json(df_retention_result, "retention_rate.json", "日留存率分析")


# ===========================================================================
# 2.3 RFM用户价值分层（简化版：仅R和F）
# ===========================================================================
# R (Recency)：最近一次消费距今的天数（越小越好）
# F (Frequency)：消费次数（越大越好）
#
# 分层逻辑：
#   R值小（近期消费）+ F值高（高频） → 高价值用户
#   R值小 + F值低 → 新用户/潜力用户
#   R值大 + F值高 → 流失预警用户
#   R值大 + F值低 → 流失用户
# ===========================================================================
print("\n" + "=" * 60)
print("2.3 RFM用户价值分层")
print("=" * 60)

# 只分析有购买行为的用户
df_buy = df.filter(col("behavior") == "buy")

# 计算基准日期（数据集中最晚日期的下一天，模拟"今天"）
max_date = df.agg(_max("date")).collect()[0][0]
print(f"[任务2] 数据集最晚日期：{max_date}")

# 计算每个购买用户的R值和F值
df_rfm = df_buy.groupBy("user_id").agg(
    # R：最近一次购买距最晚日期的天数
    datediff(lit(max_date), _max("date")).alias("recency"),
    # F：购买总次数
    count("*").alias("frequency")
)

# 计算R和F的中位数，用于分层（用近似中位数approxQuantile）
r_median = df_rfm.approxQuantile("recency", [0.5], 0.01)[0]
f_median = df_rfm.approxQuantile("frequency", [0.5], 0.01)[0]
print(f"[任务2] Recency中位数：{r_median}, Frequency中位数：{f_median}")

# 分层：基于R和F与中位数的比较
df_rfm = df_rfm.withColumn(
    "user_segment",
    when(
        (col("recency") <= r_median) & (col("frequency") >= f_median),
        "高价值用户"   # 近期消费 + 高频
    ).when(
        (col("recency") <= r_median) & (col("frequency") < f_median),
        "潜力用户"     # 近期消费 + 低频
    ).when(
        (col("recency") > r_median) & (col("frequency") >= f_median),
        "流失预警用户"  # 长期未消费 + 曾高频
    ).otherwise(
        "流失用户"     # 长期未消费 + 低频
    )
)

# 统计各分层用户数
df_rfm_summary = df_rfm.groupBy("user_segment").agg(
    count("*").alias("user_count")
).orderBy(desc("user_count"))

df_rfm_summary.show()
save_as_json(df_rfm_summary, "rfm_segments.json", "RFM用户分层统计")

# 保存完整RFM数据，供后续用户画像使用
df_rfm.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/rfm_detail")


# ===========================================================================
# 2.4 用户行为路径分析
# ===========================================================================
# 追踪用户的典型路径：浏览(pv) → 收藏(fav)/加购(cart) → 购买(buy)
# 思路：
#   1. 对每个用户的行为按时间排序
#   2. 提取每个用户的行为序列
#   3. 统计常见路径模式
# ===========================================================================
print("\n" + "=" * 60)
print("2.4 用户行为路径分析")
print("=" * 60)

# 为每个用户的行为按时间排序
window_user = Window.partitionBy("user_id").orderBy("timestamp")

# 获取每个用户的前一个行为
df_path = df.withColumn(
    "prev_behavior", lag("behavior", 1).over(window_user)
)

# 过滤掉第一条行为（没有前一个行为）
df_path = df_path.filter(col("prev_behavior").isNotNull())

# 构建行为路径（前一行为 → 当前行为）
df_path = df_path.withColumn(
    "path_segment",
    concat_ws(" → ", col("prev_behavior"), col("behavior"))
)

# 统计各路径出现次数
df_path_stats = df_path.groupBy("path_segment").agg(
    count("*").alias("path_count")
).orderBy(desc("path_count"))

df_path_stats.show(20, truncate=False)
save_as_json(df_path_stats, "behavior_paths.json", "用户行为路径分析")

# 进一步分析：三步路径（如 pv → fav → buy）
# 获取前两个行为
df_path3 = df.withColumn("prev1", lag("behavior", 1).over(window_user)) \
    .withColumn("prev2", lag("behavior", 2).over(window_user)) \
    .filter(col("prev1").isNotNull() & col("prev2").isNotNull()) \
    .withColumn("path3", concat_ws(" → ", col("prev2"), col("prev1"), col("behavior")))

df_path3_stats = df_path3.groupBy("path3").agg(
    count("*").alias("count")
).orderBy(desc("count"))

df_path3_stats.show(20, truncate=False)
save_as_json(df_path3_stats, "behavior_paths_3step.json", "三步行为路径分析")


# ===========================================================================
# 2.5 用户画像（高价值用户特征）
# ===========================================================================
# 高价值用户 = RFM中"高价值用户"分层的用户
# 分析他们的：活跃时段分布、偏好商品类目
# ===========================================================================
print("\n" + "=" * 60)
print("2.5 用户画像分析")
print("=" * 60)

# 获取高价值用户列表
df_high_value_users = spark.read.parquet(f"{OUTPUT_PATH}/rfm_detail") \
    .filter(col("user_segment") == "高价值用户") \
    .select("user_id").distinct()

# 关联原始数据，获取高价值用户的所有行为
df_hv = df.join(df_high_value_users, on="user_id", how="inner")

# 高价值用户活跃时段分布（按小时统计）
df_hv_hourly = df_hv.groupBy("hour").agg(
    count("*").alias("behavior_count")
).orderBy("hour")

print("[任务2] 高价值用户活跃时段分布：")
df_hv_hourly.show(24)
save_as_json(df_hv_hourly, "hv_user_hourly.json", "高价值用户活跃时段")

# 高价值用户偏好类目 TOP20
df_hv_category = df_hv.groupBy("category_id").agg(
    count("*").alias("behavior_count"),
    countDistinct("user_id").alias("user_count")
).orderBy(desc("behavior_count")).limit(20)

print("[任务2] 高价值用户偏好类目 TOP20：")
df_hv_category.show(20)
save_as_json(df_hv_category, "hv_user_category.json", "高价值用户偏好类目")


# ===========================================================================
# 2.6 商品/类目热度排行
# ===========================================================================
# 统计被 pv/fav/cart/buy 最多的商品ID和类目ID
# ===========================================================================
print("\n" + "=" * 60)
print("2.6 商品/类目热度排行")
print("=" * 60)

# 商品维度：按商品ID统计各行为次数
df_item_stats = df.groupBy("item_id").agg(
    count(when(col("behavior") == "pv", 1)).alias("pv"),
    count(when(col("behavior") == "fav", 1)).alias("fav"),
    count(when(col("behavior") == "cart", 1)).alias("cart"),
    count(when(col("behavior") == "buy", 1)).alias("buy"),
    count("*").alias("total")
).orderBy(desc("total"))

# 取 TOP50 热门商品
df_item_top50 = df_item_stats.limit(50)
print("[任务2] 商品热度排行 TOP50：")
df_item_top50.show(10)
save_as_json(df_item_top50, "item_hot_rank.json", "商品热度排行TOP50")

# 类目维度：按类目ID统计各行为次数
df_cat_stats = df.groupBy("category_id").agg(
    count(when(col("behavior") == "pv", 1)).alias("pv"),
    count(when(col("behavior") == "fav", 1)).alias("fav"),
    count(when(col("behavior") == "cart", 1)).alias("cart"),
    count(when(col("behavior") == "buy", 1)).alias("buy"),
    count("*").alias("total")
).orderBy(desc("total"))

# 取 TOP30 热门类目
df_cat_top30 = df_cat_stats.limit(30)
print("[任务2] 类目热度排行 TOP30：")
df_cat_top30.show(10)
save_as_json(df_cat_top30, "category_hot_rank.json", "类目热度排行TOP30")


# ===========================================================================
# 2.7 商品转化率分析
# ===========================================================================
# 转化率 = 购买数 / 浏览数
# 找出"高流量低转化"的商品
# ===========================================================================
print("\n" + "=" * 60)
print("2.7 商品转化率分析")
print("=" * 60)

# 在商品统计基础上计算转化率
df_item_cvr = df_item_stats \
    .withColumn(
        "pv_to_buy_rate",
        when(col("pv") > 0, _round(col("buy") / col("pv") * 100, 2)) \
            .otherwise(0.0)
    ) \
    .filter(col("pv") >= 10)  # 至少有10次浏览才有统计意义

# 转化率TOP50（高转化商品）
df_item_cvr_top = df_item_cvr.orderBy(desc("pv_to_buy_rate")).limit(50)
print("[任务2] 高转化商品 TOP50：")
df_item_cvr_top.show(10)
save_as_json(df_item_cvr_top, "item_cvr_top.json", "高转化商品TOP50")

# 高流量低转化商品：浏览量高但转化率低
# 取浏览量前20%中转化率最低的50个
pv_threshold = df_item_cvr.approxQuantile("pv", [0.8], 0.01)[0]
df_item_low_cvr = df_item_cvr.filter(col("pv") >= pv_threshold) \
    .orderBy(asc("pv_to_buy_rate")) \
    .limit(50)
print(f"[任务2] 高流量低转化商品（PV>={int(pv_threshold)}）：")
df_item_low_cvr.show(10)
save_as_json(df_item_low_cvr, "item_low_cvr.json", "高流量低转化商品")

# 类目转化率
df_cat_cvr = df_cat_stats \
    .withColumn(
        "pv_to_buy_rate",
        when(col("pv") > 0, _round(col("buy") / col("pv") * 100, 2)) \
            .otherwise(0.0)
    ) \
    .orderBy(desc("pv_to_buy_rate"))

print("[任务2] 类目转化率：")
df_cat_cvr.show(20)
save_as_json(df_cat_cvr, "category_cvr.json", "类目转化率分析")


# ===========================================================================
# 2.8 转化漏斗分析
# ===========================================================================
# 漏斗层级：浏览(pv) → 加购/收藏(cart+fav) → 购买(buy)
# 计算每一层级的用户数和转化率
# ===========================================================================
print("\n" + "=" * 60)
print("2.8 转化漏斗分析")
print("=" * 60)

# 计算各行为的独立用户数
funnel_stages = {
    "浏览(pv)": "pv",
    "收藏(fav)": "fav",
    "加购(cart)": "cart",
    "购买(buy)": "buy"
}

funnel_data = []
for stage_name, behavior in funnel_stages.items():
    uv = df.filter(col("behavior") == behavior) \
        .select("user_id").distinct().count()
    funnel_data.append({"stage": stage_name, "user_count": uv})

# 构建漏斗 DataFrame
df_funnel = spark.createDataFrame(funnel_data)

# 计算转化率（以浏览为基准100%）
pv_users = funnel_data[0]["user_count"]
df_funnel = df_funnel.withColumn(
    "conversion_rate",
    _round(col("user_count") / lit(pv_users) * 100, 2)
)

print("[任务2] 转化漏斗：")
df_funnel.show()
save_as_json(df_funnel, "conversion_funnel.json", "转化漏斗分析")

# 进一步：构建复合漏斗（浏览 → 加购或收藏 → 购买）
# 浏览过的用户
pv_user_set = df.filter(col("behavior") == "pv") \
    .select("user_id").distinct()

# 有加购或收藏行为的用户
interest_user_set = df.filter(col("behavior").isin("cart", "fav")) \
    .select("user_id").distinct()

# 购买过的用户
buy_user_set = df.filter(col("behavior") == "buy") \
    .select("user_id").distinct()

# 计算各阶段人数
pv_count = pv_user_set.count()
interest_count = interest_user_set.count()
buy_count = buy_user_set.count()

# 浏览→加购/收藏 转化
pv_to_interest = pv_user_set.join(interest_user_set, on="user_id", how="inner").count()
# 加购/收藏→购买 转化
interest_to_buy = interest_user_set.join(buy_user_set, on="user_id", how="inner").count()
# 浏览→购买 转化
pv_to_buy = pv_user_set.join(buy_user_set, on="user_id", how="inner").count()

funnel_detail = [
    {
        "stage": "浏览(pv)",
        "user_count": pv_count,
        "conversion_rate": 100.0,
        "step_conversion_rate": 100.0
    },
    {
        "stage": "加购/收藏(cart+fav)",
        "user_count": interest_count,
        "conversion_rate": round(pv_to_interest / pv_count * 100, 2) if pv_count > 0 else 0,
        "step_conversion_rate": round(pv_to_interest / pv_count * 100, 2) if pv_count > 0 else 0
    },
    {
        "stage": "购买(buy)",
        "user_count": buy_count,
        "conversion_rate": round(pv_to_buy / pv_count * 100, 2) if pv_count > 0 else 0,
        "step_conversion_rate": round(interest_to_buy / interest_count * 100, 2) if interest_count > 0 else 0
    }
]

df_funnel_detail = spark.createDataFrame(funnel_detail)
print("[任务2] 复合转化漏斗（浏览→加购/收藏→购买）：")
df_funnel_detail.show()
save_as_json(df_funnel_detail, "conversion_funnel_detail.json", "复合转化漏斗")


# ===========================================================================
# 2.9 行为分布概览
# ===========================================================================
# 统计四种行为（pv, fav, cart, buy）的总量及占比
# ===========================================================================
print("\n" + "=" * 60)
print("2.9 行为分布概览")
print("=" * 60)

# 按行为类型统计总量
df_behavior_dist = df.groupBy("behavior").agg(
    count("*").alias("count")
)

# 计算占比
total = df.count()
df_behavior_dist = df_behavior_dist.withColumn(
    "percentage",
    _round(col("count") / lit(total) * 100, 2)
).orderBy(desc("count"))

print("[任务2] 行为分布概览：")
df_behavior_dist.show()
save_as_json(df_behavior_dist, "behavior_distribution.json", "行为分布概览")


# ===========================================================================
# 汇总：输出所有分析结果文件列表
# ===========================================================================
print("\n" + "=" * 60)
print("[任务2] 所有分析完成！")
print("[任务2] 输出文件列表（位于HDFS: /data/output/）：")
print("  - daily_uv_pv.json          每日UV/PV统计")
print("  - retention_rate.json        日留存率分析")
print("  - rfm_segments.json          RFM用户分层统计")
print("  - behavior_paths.json        两步行为路径")
print("  - behavior_paths_3step.json  三步行为路径")
print("  - hv_user_hourly.json        高价值用户活跃时段")
print("  - hv_user_category.json      高价值用户偏好类目")
print("  - item_hot_rank.json         商品热度排行TOP50")
print("  - category_hot_rank.json     类目热度排行TOP30")
print("  - item_cvr_top.json          高转化商品TOP50")
print("  - item_low_cvr.json          高流量低转化商品")
print("  - category_cvr.json          类目转化率")
print("  - conversion_funnel.json     转化漏斗")
print("  - conversion_funnel_detail.json 复合转化漏斗")
print("  - behavior_distribution.json 行为分布概览")
print("=" * 60)

# 取消缓存并关闭Spark
df.unpersist()
spark.stop()
