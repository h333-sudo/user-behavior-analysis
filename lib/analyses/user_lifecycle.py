"""
2.5 用户生命周期分析（新增模块）
输出: user_lifecycle.json, first_purchase_gap.json

分析维度:
  1. 新用户 vs 回访用户 每日占比
  2. 首次浏览到首次购买的间隔分布
  3. 用户活跃天数分布
"""

MODULE_ID = "2.5"
MODULE_NAME = "用户生命周期分析"
MODULE_GROUP = "user"
MODULE_ENABLED = True
OUTPUT_FILES = ["user_lifecycle.json", "first_purchase_gap.json", "active_days_distribution.json"]

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, count, countDistinct, min as _min, max as _max,
    datediff, when, lit, round as _round, desc, sum as _sum
)


def run(df: DataFrame, config: dict, save_fn) -> None:
    """
    执行用户生命周期分析

    参数:
        df: 清洗后的数据集
        config: 配置字典
        save_fn: JSON保存函数
    """
    # ====================================================================
    # 1. 新用户 vs 回访用户 每日占比
    # ====================================================================
    # 每个用户最早出现的日期
    df_user_first = df.groupBy("user_id").agg(
        _min("date").alias("first_date")
    )

    # 每日活跃用户关联其首次日期
    df_daily_users = df.select("user_id", "date").distinct()
    df_daily_users = df_daily_users.join(df_user_first, on="user_id", how="inner")

    # 判断是否为当日新用户
    df_daily_users = df_daily_users.withColumn(
        "is_new",
        when(col("date") == col("first_date"), 1).otherwise(0)
    )

    # 按日统计新/老用户数
    df_lifecycle = df_daily_users.groupBy("date").agg(
        count("*").alias("total_users"),
        _sum("is_new").alias("new_users"),
    ).withColumn(
        "returning_users",
        col("total_users") - col("new_users")
    ).withColumn(
        "new_user_ratio",
        _round(col("new_users") / col("total_users") * 100, 2)
    ).orderBy("date")

    print("[生命周期] 新用户 vs 回访用户 每日占比:")
    df_lifecycle.show(10, truncate=False)
    save_fn(df_lifecycle, "user_lifecycle.json", "用户生命周期(新/回访)")

    # ====================================================================
    # 2. 首次浏览到首次购买的间隔分布
    # ====================================================================
    # 每个用户首次浏览日期
    df_first_pv = df.filter(col("behavior") == "pv") \
        .groupBy("user_id").agg(_min("date").alias("first_pv_date"))

    # 每个用户首次购买日期
    df_first_buy = df.filter(col("behavior") == "buy") \
        .groupBy("user_id").agg(_min("date").alias("first_buy_date"))

    # 关联
    df_gap = df_first_pv.join(df_first_buy, on="user_id", how="inner")
    df_gap = df_gap.withColumn(
        "gap_days",
        datediff(col("first_buy_date"), col("first_pv_date"))
    )

    # 间隔分布统计
    df_gap_dist = df_gap.withColumn(
        "gap_group",
        when(col("gap_days") == 0, "当天购买")
        .when(col("gap_days") <= 1, "1天内")
        .when(col("gap_days") <= 3, "2-3天")
        .when(col("gap_days") <= 7, "4-7天")
        .otherwise("7天以上")
    ).groupBy("gap_group").agg(
        count("*").alias("user_count")
    ).orderBy("gap_group")

    print("[生命周期] 首次浏览到首次购买的间隔分布:")
    df_gap_dist.show(10, truncate=False)
    save_fn(df_gap_dist, "first_purchase_gap.json", "首购间隔分布")

    # ====================================================================
    # 3. 用户活跃天数分布
    # ====================================================================
    df_active_days = df.groupBy("user_id").agg(
        countDistinct("date").alias("active_days")
    )

    df_active_dist = df_active_days.withColumn(
        "active_group",
        when(col("active_days") == 1, "仅1天")
        .when(col("active_days") <= 3, "2-3天")
        .when(col("active_days") <= 5, "4-5天")
        .when(col("active_days") <= 7, "6-7天")
        .otherwise("8-9天")
    ).groupBy("active_group").agg(
        count("*").alias("user_count")
    ).orderBy(desc("user_count"))

    print("[生命周期] 用户活跃天数分布:")
    df_active_dist.show(10, truncate=False)
    save_fn(df_active_dist, "active_days_distribution.json", "活跃天数分布")
