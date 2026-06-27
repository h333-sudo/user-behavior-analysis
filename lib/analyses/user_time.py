"""
2.6 时间维度深度分析（新增模块）
输出: hourly_cvr.json, weekday_vs_weekend.json, daily_funnel_trend.json

分析维度:
  1. 每小时转化率（各时段的浏览→购买转化率）
  2. 工作日 vs 周末 行为差异
  3. 各日期的漏斗变化趋势
"""

MODULE_ID = "2.6"
MODULE_NAME = "时间维度深度分析"
MODULE_GROUP = "user"
MODULE_ENABLED = True
OUTPUT_FILES = ["hourly_cvr.json", "weekday_vs_weekend.json", "daily_funnel_trend.json"]

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, when, lit, round as _round, desc, dayofweek
)


def run(df: DataFrame, config: dict, save_fn) -> None:
    """
    执行时间维度深度分析

    参数:
        df: 清洗后的数据集
        config: 配置字典
        save_fn: JSON保存函数
    """
    # ====================================================================
    # 1. 每小时转化率
    # ====================================================================
    # 统计每小时的各行为UV
    df_hourly = df.groupBy("hour").agg(
        count(when(col("behavior") == "pv", 1)).alias("pv_count"),
        count(when(col("behavior") == "buy", 1)).alias("buy_count"),
        countDistinct(when(col("behavior") == "pv", col("user_id"))).alias("pv_uv"),
        countDistinct(when(col("behavior") == "buy", col("user_id"))).alias("buy_uv"),
        count("*").alias("total_behavior")
    )

    # 计算转化率（防除零）
    df_hourly = df_hourly \
        .withColumn(
            "pv_to_buy_rate",
            when(col("pv_count") > 0, _round(col("buy_count") / col("pv_count") * 100, 2)).otherwise(0.0)
        ) \
        .withColumn(
            "uv_cvr",
            when(col("pv_uv") > 0, _round(col("buy_uv") / col("pv_uv") * 100, 2)).otherwise(0.0)
        ) \
        .orderBy("hour")

    print("[时间分析] 每小时转化率:")
    df_hourly.show(24, truncate=False)
    save_fn(df_hourly, "hourly_cvr.json", "每小时转化率")

    # ====================================================================
    # 2. 工作日 vs 周末 行为差异
    # ====================================================================
    # dayofweek: 1=周日, 2=周一, ..., 7=周六
    # 标记工作日(2-6) vs 周末(1,7)
    df_dow = df.withColumn(
        "day_of_week", dayofweek(col("date"))
    ).withColumn(
        "is_weekend",
        when(col("day_of_week").isin(1, 7), 1).otherwise(0)
    )

    # 工作日统计
    df_weekday = df_dow.filter(col("is_weekend") == 0).agg(
        count("*").alias("total_behavior"),
        countDistinct("user_id").alias("uv"),
        count(when(col("behavior") == "pv", 1)).alias("pv"),
        count(when(col("behavior") == "buy", 1)).alias("buy"),
    )

    # 周末统计
    df_weekend = df_dow.filter(col("is_weekend") == 1).agg(
        count("*").alias("total_behavior"),
        countDistinct("user_id").alias("uv"),
        count(when(col("behavior") == "pv", 1)).alias("pv"),
        count(when(col("behavior") == "buy", 1)).alias("buy"),
    )

    # 动态计算工作日/周末天数（day_of_week 在 df_dow 中已计算）
    df_dow_dates = df_dow.select("date", "day_of_week").distinct()
    weekday_count = df_dow_dates.filter(col("day_of_week").isin(2, 3, 4, 5, 6)).count()
    weekend_count = df_dow_dates.filter(col("day_of_week").isin(1, 7)).count()

    wd = df_weekday.collect()[0]
    we = df_weekend.collect()[0]

    comparison_data = [
        {
            "type": "工作日",
            "days": weekday_count,
            "total_behavior": wd["total_behavior"],
            "daily_avg_behavior": round(wd["total_behavior"] / weekday_count),
            "uv": wd["uv"],
            "pv": wd["pv"],
            "buy": wd["buy"],
            "buy_rate": round(wd["buy"] / wd["pv"] * 100, 2) if wd["pv"] and wd["pv"] > 0 else 0,
        },
        {
            "type": "周末",
            "days": weekend_count,
            "total_behavior": we["total_behavior"],
            "daily_avg_behavior": round(we["total_behavior"] / weekend_count),
            "uv": we["uv"],
            "pv": we["pv"],
            "buy": we["buy"],
            "buy_rate": round(we["buy"] / we["pv"] * 100, 2) if we["pv"] and we["pv"] > 0 else 0,
        },
    ]

    spark = df.sparkSession
    df_comparison = spark.createDataFrame(comparison_data)

    print("[时间分析] 工作日 vs 周末:")
    df_comparison.show(truncate=False)
    save_fn(df_comparison, "weekday_vs_weekend.json", "工作日vs周末对比")

    # ====================================================================
    # 3. 各日期的漏斗变化趋势（严格递进漏斗）
    # ====================================================================
    # 按日期提取各阶段的独立用户集合
    df_daily_pv = df.filter(col("behavior") == "pv").select("date", "user_id").distinct()
    df_daily_interest = df.filter(col("behavior").isin("cart", "fav")).select("date", "user_id").distinct()
    df_daily_buy = df.filter(col("behavior") == "buy").select("date", "user_id").distinct()

    # Stage 1: 当日有 pv 的用户数
    stage1 = df_daily_pv.groupBy("date").agg(
        countDistinct("user_id").alias("pv_uv")
    )

    # Stage 2: 当日有 pv 且 有 cart/fav 的用户（递进：必须是 stage1 的子集）
    pv_interest = df_daily_pv.join(df_daily_interest, on=["date", "user_id"], how="inner")
    stage2 = pv_interest.groupBy("date").agg(
        countDistinct("user_id").alias("interest_uv")
    )

    # Stage 3: 当日有 pv 且 有 cart/fav 且 有 buy 的用户（递进：必须是 stage2 的子集）
    pv_interest_buy = pv_interest.join(df_daily_buy, on=["date", "user_id"], how="inner")
    stage3 = pv_interest_buy.groupBy("date").agg(
        countDistinct("user_id").alias("buy_uv")
    )

    # 合并三个阶段数据
    df_daily_funnel = stage1 \
        .join(stage2, on="date", how="left") \
        .join(stage3, on="date", how="left") \
        .fillna(0) \
        .orderBy("date")

    df_daily_funnel = df_daily_funnel \
        .withColumn(
            "pv_to_interest_rate",
            when(col("pv_uv") > 0, _round(col("interest_uv") / col("pv_uv") * 100, 2)).otherwise(0.0)
        ) \
        .withColumn(
            "interest_to_buy_rate",
            when(col("interest_uv") > 0, _round(col("buy_uv") / col("interest_uv") * 100, 2)).otherwise(0.0)
        ) \
        .withColumn(
            "overall_cvr",
            when(col("pv_uv") > 0, _round(col("buy_uv") / col("pv_uv") * 100, 2)).otherwise(0.0)
        ) \
        .orderBy("date")

    print("[时间分析] 各日期漏斗变化趋势:")
    df_daily_funnel.show(10, truncate=False)
    save_fn(df_daily_funnel, "daily_funnel_trend.json", "日期漏斗趋势")
