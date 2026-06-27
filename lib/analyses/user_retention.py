"""
2.2 日留存率分析（1日/3日/7日留存）
输出: retention_rate.json
"""

MODULE_ID = "2.2"
MODULE_NAME = "日留存率分析"
MODULE_GROUP = "user"
MODULE_ENABLED = True
OUTPUT_FILES = ["retention_rate.json"]

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, min as _min, datediff, when, lit, round as _round
)


def run(df: DataFrame, config: dict, save_fn) -> DataFrame:
    """
    执行日留存率分析

    留存率 = 第N天仍活跃的用户数 / 第1天的用户数

    参数:
        df: 清洗后的数据集
        config: 配置字典
        save_fn: JSON保存函数

    返回:
        DataFrame: 留存率统计结果
    """
    # 每个用户最早访问日期
    df_user_first = df.groupBy("user_id").agg(
        _min("date").alias("first_date")
    )

    # 每个用户的所有活跃日期（去重）
    df_user_dates = df.select("user_id", "date").distinct()

    # 关联得到首次访问日期 + 每次活跃日期
    df_retention = df_user_dates.join(df_user_first, on="user_id", how="inner")

    # 计算活跃日期与首次访问日期的天数差
    df_retention = df_retention.withColumn(
        "day_diff",
        datediff(col("date"), col("first_date"))
    )

    # 每日新增用户数
    df_new_users = df_user_first.groupBy("first_date").agg(
        countDistinct("user_id").alias("new_users")
    ).orderBy("first_date")

    # 计算第1天、第3天、第7天的留存
    retention_days = [1, 3, 7]
    df_result = df_new_users

    for day in retention_days:
        df_day_n = df_retention.filter(col("day_diff") == day) \
            .groupBy("first_date") \
            .agg(countDistinct("user_id").alias(f"retained_day{day}"))

        df_result = df_result.join(df_day_n, on="first_date", how="left")

        df_result = df_result.withColumn(
            f"retention_rate_day{day}",
            _round(
                col(f"retained_day{day}") / col("new_users") * 100, 2
            )
        ).fillna(0, subset=[f"retained_day{day}", f"retention_rate_day{day}"])

    df_result = df_result.orderBy("first_date")
    df_result.show(10, truncate=False)
    save_fn(df_result, "retention_rate.json", "日留存率分析")

    return df_result
