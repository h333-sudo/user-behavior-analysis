"""
2.3 RFM用户价值分层（加权评分模型重构）
输出: rfm_segments.json, rfm_score_distribution.json
"""

MODULE_ID = "2.3"
MODULE_NAME = "RFM用户价值分层"
MODULE_GROUP = "user"
MODULE_ENABLED = True
OUTPUT_FILES = ["rfm_segments.json", "rfm_score_distribution.json"]

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, when, lit, datediff, min as _min, max as _max,
    round as _round, desc
)


def _score_recency(days, thresholds):
    """
    根据Recency（距最后日期的天数）打分

    阈值配置示例: [1, 3, 5, 7]
    含义: 0-1天=5分, 2-3天=4分, 4-5天=3分, 6-7天=2分, 8+天=1分
    """
    t = thresholds
    return (
        when(days <= t[0], 5)
        .when(days <= t[1], 4)
        .when(days <= t[2], 3)
        .when(days <= t[3], 2)
        .otherwise(1)
    )


def _score_frequency(freq, thresholds):
    """
    根据Frequency（购买次数）打分

    阈值配置示例: [1, 2, 4, 9]
    含义: 1次=1分, 2次=2分, 3-4次=3分, 5-9次=4分, 10+次=5分
    """
    t = thresholds
    return (
        when(freq <= t[0], 1)
        .when(freq <= t[1], 2)
        .when(freq <= t[2], 3)
        .when(freq <= t[3], 4)
        .otherwise(5)
    )


def run(df: DataFrame, config: dict, save_fn) -> DataFrame:
    """
    执行RFM用户价值分层

    加权评分模型:
      R_score × r_weight + F_score × f_weight = 总分
      总分 ≥ 4.0 → 高价值用户
      总分 ≥ 3.0 → 潜力用户
      总分 ≥ 2.0 → 一般用户
      总分 ≥ 1.0 → 预流失用户
      总分 < 1.0 → 流失用户

    参数:
        df: 清洗后的数据集
        config: 配置字典
        save_fn: JSON保存函数

    返回:
        DataFrame: 用户RFM明细（含评分和分层）
    """
    rfm_cfg = config["analysis"]["rfm"]
    r_weight = rfm_cfg["r_weight"]
    f_weight = rfm_cfg["f_weight"]
    r_thresholds = rfm_cfg["r_thresholds"]
    f_thresholds = rfm_cfg["f_thresholds"]
    seg_thresholds = rfm_cfg["segment_thresholds"]

    # 只分析有购买行为的用户
    df_buy = df.filter(col("behavior") == "buy")

    # 计算基准日期（数据集中最晚日期）
    max_date = df.agg(_max("date")).collect()[0][0]
    print(f"[RFM] 数据集最晚日期: {max_date}")

    # 计算每个购买用户的R值和F值
    df_rfm = df_buy.groupBy("user_id").agg(
        datediff(lit(max_date), _max("date")).alias("recency"),
        count("*").alias("frequency")
    )

    # 打分
    df_rfm = df_rfm \
        .withColumn("r_score", _score_recency(col("recency"), r_thresholds)) \
        .withColumn("f_score", _score_frequency(col("frequency"), f_thresholds))

    # 加权总分
    df_rfm = df_rfm.withColumn(
        "total_score",
        _round(col("r_score") * r_weight + col("f_score") * f_weight, 2)
    )

    # 分层
    st = seg_thresholds
    df_rfm = df_rfm.withColumn(
        "user_segment",
        when(col("total_score") >= st["high_value"], "高价值用户")
        .when(col("total_score") >= st["potential"], "潜力用户")
        .when(col("total_score") >= st["normal"], "一般用户")
        .when(col("total_score") >= st["pre_churn"], "预流失用户")
        .otherwise("流失用户")
    )

    # 输出1: 各分层统计
    df_segments = df_rfm.groupBy("user_segment").agg(
        count("*").alias("user_count")
    ).orderBy(desc("user_count"))

    print("[RFM] 用户分层统计:")
    df_segments.show(10, truncate=False)
    save_fn(df_segments, "rfm_segments.json", "RFM用户分层统计")

    # 输出2: 评分分布（各分数段人数）
    df_score_dist = df_rfm.groupBy("total_score").agg(
        count("*").alias("user_count")
    ).orderBy("total_score")

    print("[RFM] 评分分布:")
    df_score_dist.show(20, truncate=False)
    save_fn(df_score_dist, "rfm_score_distribution.json", "RFM评分分布")

    # 输出3: 保存完整RFM明细（供用户画像等后续分析使用）
    output_path = config["data"]["output_path"]
    df_rfm.write.mode("overwrite").parquet(f"{output_path}/rfm_detail")

    return df_rfm
