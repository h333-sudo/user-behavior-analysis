"""
2.10 行为分布概览
输出: behavior_distribution.json
"""

MODULE_ID = "2.10"
MODULE_NAME = "行为分布概览"
MODULE_GROUP = "conversion"
MODULE_ENABLED = True
OUTPUT_FILES = ["behavior_distribution.json"]

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, round as _round, lit, desc


def run(df: DataFrame, config: dict, save_fn) -> None:
    """
    执行行为分布概览

    统计四种行为（pv, fav, cart, buy）的总量及占比

    参数:
        df: 清洗后的数据集
        config: 配置字典
        save_fn: JSON保存函数
    """
    total = df.count()

    df_dist = df.groupBy("behavior").agg(
        count("*").alias("count")
    ).withColumn(
        "percentage",
        _round(col("count") / lit(total) * 100, 2)
    ).orderBy(desc("count"))

    print("[概览] 行为分布:")
    df_dist.show(truncate=False)
    save_fn(df_dist, "behavior_distribution.json", "行为分布概览")
