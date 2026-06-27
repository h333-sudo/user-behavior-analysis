"""
2.1 每日UV（独立访客数）和PV（页面浏览量）统计
输出: daily_uv_pv.json
"""

MODULE_ID = "2.1"
MODULE_NAME = "每日UV/PV统计"
MODULE_GROUP = "user"
MODULE_ENABLED = True
OUTPUT_FILES = ["daily_uv_pv.json"]

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, when


def run(df: DataFrame, config: dict, save_fn) -> DataFrame:
    """
    执行每日UV/PV统计

    参数:
        df: 清洗后的数据集（需包含 date, user_id, behavior 字段）
        config: 配置字典
        save_fn: JSON保存函数 save_fn(df, filename, description)

    返回:
        DataFrame: 每日UV/PV统计结果（供后续使用）
    """
    df_daily = df.groupBy("date").agg(
        countDistinct("user_id").alias("uv"),
        count(when(col("behavior") == "pv", 1)).alias("pv"),
        count("*").alias("total_behavior")
    ).orderBy("date")

    df_daily.show(10, truncate=False)
    save_fn(df_daily, "daily_uv_pv.json", "每日UV/PV统计")

    return df_daily
