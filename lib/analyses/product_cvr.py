"""
2.8 商品转化率分析
输出: item_cvr_top.json, item_low_cvr.json, category_cvr.json
"""

MODULE_ID = "2.8"
MODULE_NAME = "商品转化率分析"
MODULE_GROUP = "product"
MODULE_ENABLED = True
OUTPUT_FILES = ["item_cvr_top.json", "item_low_cvr.json", "category_cvr.json"]

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, when, desc, asc, round as _round
)


def run(df: DataFrame, config: dict, save_fn) -> None:
    """
    执行商品转化率分析

    转化率 = 购买数 / 浏览数
    找出"高转化"和"高流量低转化"的商品

    参数:
        df: 清洗后的数据集
        config: 配置字典
        save_fn: JSON保存函数
    """
    top_n_cvr = config["analysis"]["top_n"]["cvr_item"]
    top_n_low = config["analysis"]["top_n"]["low_cvr_item"]
    min_pv = config["analysis"]["min_pv_for_cvr"]

    # ====================================================================
    # 商品维度：统计各行为的独立用户数 + 转化率
    # ====================================================================
    df_item = df.groupBy("item_id").agg(
        countDistinct(when(col("behavior") == "pv", col("user_id"))).alias("pv"),
        countDistinct(when(col("behavior") == "fav", col("user_id"))).alias("fav"),
        countDistinct(when(col("behavior") == "cart", col("user_id"))).alias("cart"),
        countDistinct(when(col("behavior") == "buy", col("user_id"))).alias("buy"),
        countDistinct("user_id").alias("total_uv")
    )

    # 计算转化率：购买用户数 / (浏览+加购+收藏)用户数，过滤低活跃商品
    df_item_cvr = df_item \
        .withColumn(
            "interest_uv",
            col("pv") + col("cart") + col("fav")
        ) \
        .withColumn(
            "pv_to_buy_rate",
            when(col("interest_uv") > 0, _round(col("buy") / col("interest_uv") * 100, 2))
            .otherwise(0.0)
        ) \
        .filter(col("interest_uv") >= min_pv)

    # 高转化商品 TOP-N
    df_cvr_top = df_item_cvr.orderBy(desc("pv_to_buy_rate")).limit(top_n_cvr)
    print(f"[转化率] 高转化商品 TOP{top_n_cvr}:")
    df_cvr_top.show(10, truncate=False)
    save_fn(df_cvr_top, "item_cvr_top.json", f"高转化商品TOP{top_n_cvr}")

    # 高流量低转化商品：浏览量前20%中转化率最低的TOP-N
    pv_threshold = df_item_cvr.approxQuantile("pv", [0.8], 0.01)[0]
    df_low_cvr = df_item_cvr.filter(col("pv") >= pv_threshold) \
        .orderBy(asc("pv_to_buy_rate")) \
        .limit(top_n_low)
    print(f"[转化率] 高流量低转化商品 (PV>={int(pv_threshold)}):")
    df_low_cvr.show(10, truncate=False)
    save_fn(df_low_cvr, "item_low_cvr.json", f"高流量低转化商品TOP{top_n_low}")

    # ====================================================================
    # 类目转化率（按独立用户数计算）
    # ====================================================================
    df_cat_cvr = df.groupBy("category_id").agg(
        countDistinct(when(col("behavior") == "pv", col("user_id"))).alias("pv"),
        countDistinct(when(col("behavior") == "fav", col("user_id"))).alias("fav"),
        countDistinct(when(col("behavior") == "cart", col("user_id"))).alias("cart"),
        countDistinct(when(col("behavior") == "buy", col("user_id"))).alias("buy"),
        countDistinct("user_id").alias("total_uv")
    ).withColumn(
        "interest_uv",
        col("pv") + col("cart") + col("fav")
    ).withColumn(
        "pv_to_buy_rate",
        when(col("interest_uv") > 0, _round(col("buy") / col("interest_uv") * 100, 2))
        .otherwise(0.0)
    ).orderBy(desc("pv_to_buy_rate"))

    print("[转化率] 类目转化率:")
    df_cat_cvr.show(20, truncate=False)
    save_fn(df_cat_cvr, "category_cvr.json", "类目转化率分析")
