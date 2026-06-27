"""
2.7 商品/类目热度排行
输出: item_hot_rank.json, category_hot_rank.json
"""

MODULE_ID = "2.7"
MODULE_NAME = "商品/类目热度排行"
MODULE_GROUP = "product"
MODULE_ENABLED = True
OUTPUT_FILES = ["item_hot_rank.json", "category_hot_rank.json"]

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, desc


def run(df: DataFrame, config: dict, save_fn) -> None:
    """
    执行商品/类目热度排行

    参数:
        df: 清洗后的数据集
        config: 配置字典
        save_fn: JSON保存函数
    """
    top_n_item = config["analysis"]["top_n"]["item"]
    top_n_cat = config["analysis"]["top_n"]["category"]

    # ====================================================================
    # 商品维度：按商品ID统计各行为次数
    # ====================================================================
    df_item = df.groupBy("item_id").agg(
        count(when(col("behavior") == "pv", 1)).alias("pv"),
        count(when(col("behavior") == "fav", 1)).alias("fav"),
        count(when(col("behavior") == "cart", 1)).alias("cart"),
        count(when(col("behavior") == "buy", 1)).alias("buy"),
        count("*").alias("total")
    ).orderBy(desc("total")).limit(top_n_item)

    print(f"[商品热度] TOP{top_n_item}:")
    df_item.show(10, truncate=False)
    save_fn(df_item, "item_hot_rank.json", f"商品热度排行TOP{top_n_item}")

    # ====================================================================
    # 类目维度：按类目ID统计各行为次数
    # ====================================================================
    df_cat = df.groupBy("category_id").agg(
        count(when(col("behavior") == "pv", 1)).alias("pv"),
        count(when(col("behavior") == "fav", 1)).alias("fav"),
        count(when(col("behavior") == "cart", 1)).alias("cart"),
        count(when(col("behavior") == "buy", 1)).alias("buy"),
        count("*").alias("total")
    ).orderBy(desc("total")).limit(top_n_cat)

    print(f"[类目热度] TOP{top_n_cat}:")
    df_cat.show(10, truncate=False)
    save_fn(df_cat, "category_hot_rank.json", f"类目热度排行TOP{top_n_cat}")
