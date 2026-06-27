"""
2.9 转化漏斗分析
输出: conversion_funnel.json, conversion_funnel_detail.json
"""

MODULE_ID = "2.9"
MODULE_NAME = "转化漏斗分析"
MODULE_GROUP = "conversion"
MODULE_ENABLED = True
OUTPUT_FILES = ["conversion_funnel.json", "conversion_funnel_detail.json"]

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct, round as _round, lit


def run(df: DataFrame, config: dict, save_fn) -> None:
    """
    执行转化漏斗分析

    漏斗层级：浏览(pv) → 加购/收藏(cart+fav) → 购买(buy)

    参数:
        df: 清洗后的数据集
        config: 配置字典
        save_fn: JSON保存函数
    """
    spark = df.sparkSession

    # ====================================================================
    # 严格递进漏斗：浏览 → 加购/收藏 → 购买
    # 每一步只统计完成上一步的用户
    # ====================================================================
    pv_user_set = df.filter(col("behavior") == "pv").select("user_id").distinct()
    interest_user_set = df.filter(col("behavior").isin("cart", "fav")).select("user_id").distinct()
    buy_user_set = df.filter(col("behavior") == "buy").select("user_id").distinct()

    pv_count = pv_user_set.count()

    # 第一步：有浏览且有加购/收藏的用户
    pv_and_interest = pv_user_set.join(interest_user_set, on="user_id", how="inner")
    pv_and_interest_count = pv_and_interest.count()

    # 第二步：有浏览、加购/收藏、且购买的用户（三步全通）
    pv_interest_and_buy = pv_and_interest.join(buy_user_set, on="user_id", how="inner")
    pv_interest_and_buy_count = pv_interest_and_buy.count()

    funnel_data = [
        {"stage": "浏览(pv)", "user_count": pv_count, "conversion_rate": 100.0},
        {"stage": "加购/收藏(cart+fav)", "user_count": pv_and_interest_count,
         "conversion_rate": round(pv_and_interest_count / pv_count * 100, 2) if pv_count > 0 else 0},
        {"stage": "购买(buy)", "user_count": pv_interest_and_buy_count,
         "conversion_rate": round(pv_interest_and_buy_count / pv_count * 100, 2) if pv_count > 0 else 0},
    ]

    df_funnel = spark.createDataFrame(funnel_data)
    print("[漏斗] 严格递进漏斗:")
    df_funnel.show()
    save_fn(df_funnel, "conversion_funnel.json", "转化漏斗分析")

    # ====================================================================
    # 复合漏斗：浏览 → 加购/收藏 → 购买
    # ====================================================================
    pv_user_set = df.filter(col("behavior") == "pv") \
        .select("user_id").distinct()
    interest_user_set = df.filter(col("behavior").isin("cart", "fav")) \
        .select("user_id").distinct()
    buy_user_set = df.filter(col("behavior") == "buy") \
        .select("user_id").distinct()

    pv_count = pv_user_set.count()
    interest_count = interest_user_set.count()
    buy_count = buy_user_set.count()

    # 严格漏进：只统计完成前一步的用户中进入下一步的人数
    pv_to_interest = pv_user_set.join(
        interest_user_set, on="user_id", how="inner"
    )
    pv_to_interest_count = pv_to_interest.count()

    # 购买阶段：只统计同时有 pv + (cart或fav) + buy 的用户（三步全通）
    pv_interest_to_buy = pv_to_interest.join(
        buy_user_set, on="user_id", how="inner"
    )
    pv_interest_to_buy_count = pv_interest_to_buy.count()

    funnel_detail = [
        {
            "stage": "浏览(pv)",
            "user_count": pv_count,
            "conversion_rate": 100.0,
            "step_conversion_rate": 100.0
        },
        {
            "stage": "加购/收藏(cart+fav)",
            "user_count": pv_to_interest_count,
            "conversion_rate": round(pv_to_interest_count / pv_count * 100, 2) if pv_count > 0 else 0,
            "step_conversion_rate": round(pv_to_interest_count / pv_count * 100, 2) if pv_count > 0 else 0
        },
        {
            "stage": "购买(buy)",
            "user_count": pv_interest_to_buy_count,
            "conversion_rate": round(pv_interest_to_buy_count / pv_count * 100, 2) if pv_count > 0 else 0,
            "step_conversion_rate": round(pv_interest_to_buy_count / pv_to_interest_count * 100, 2) if pv_to_interest_count > 0 else 0
        }
    ]

    df_funnel_detail = spark.createDataFrame(funnel_detail)
    print("[漏斗] 复合转化漏斗:")
    df_funnel_detail.show(truncate=False)
    save_fn(df_funnel_detail, "conversion_funnel_detail.json", "复合转化漏斗")
