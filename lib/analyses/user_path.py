"""
2.4 用户行为路径分析（增强版：会话级分析）
输出: behavior_paths.json, behavior_paths_3step.json, session_stats.json
"""

MODULE_ID = "2.4"
MODULE_NAME = "用户行为路径分析"
MODULE_GROUP = "user"
MODULE_ENABLED = True
OUTPUT_FILES = ["behavior_paths.json", "behavior_paths_3step.json", "session_stats.json"]

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, lag, count, countDistinct, concat_ws, when, sum as _sum,
    round as _round, desc, avg
)


def run(df: DataFrame, config: dict, save_fn) -> None:
    """
    执行用户行为路径分析

    增强点：
    1. 引入会话概念（两次行为间隔>30分钟视为新会话）
    2. 以会话为单位提取行为序列
    3. 统计会话级指标

    参数:
        df: 清洗后的数据集
        config: 配置字典
        save_fn: JSON保存函数
    """
    gap_minutes = config["analysis"]["session_gap_minutes"]
    gap_seconds = gap_minutes * 60

    # ====================================================================
    # 会话分割
    # ====================================================================
    window_user = Window.partitionBy("user_id").orderBy("timestamp")

    # 计算与上一条行为的时间差（秒）
    df_session = df.withColumn(
        "prev_timestamp", lag("timestamp", 1).over(window_user)
    ).withColumn(
        "time_diff", col("timestamp") - col("prev_timestamp")
    )

    # 标记新会话开始（第一条行为 或 时间差超过阈值）
    df_session = df_session.withColumn(
        "is_new_session",
        when(col("prev_timestamp").isNull(), 1)
        .when(col("time_diff") > gap_seconds, 1)
        .otherwise(0)
    )

    # 生成会话ID（累计求和）
    df_session = df_session.withColumn(
        "session_id_part", _sum("is_new_session").over(window_user)
    )
    # 组合 user_id + session_id 作为唯一会话标识
    df_session = df_session.withColumn(
        "session_id",
        concat_ws("_", col("user_id").cast("string"), col("session_id_part").cast("string"))
    )

    # ====================================================================
    # 会话级统计
    # ====================================================================
    df_session_stats = df_session.groupBy("session_id").agg(
        count("*").alias("session_length"),
        countDistinct("behavior").alias("unique_behaviors"),
    )

    # 全局会话统计
    total_sessions = df_session_stats.count()
    avg_length = df_session_stats.agg(
        avg("session_length")
    ).collect()[0][0]

    # 会话长度分布
    df_length_dist = df_session_stats.groupBy("session_length").agg(
        count("*").alias("session_count")
    ).orderBy("session_length")

    session_summary = [
        {"metric": "total_sessions", "value": float(total_sessions)},
        {"metric": "avg_session_length", "value": round(avg_length, 2) if avg_length else 0.0},
        {"metric": "gap_minutes", "value": float(gap_minutes)},
    ]
    # 构建DataFrame输出
    spark = df.sparkSession
    df_summary = spark.createDataFrame(session_summary)
    df_summary.show(truncate=False)
    save_fn(df_summary, "session_stats.json", "会话统计概览")

    # ====================================================================
    # 两步行为路径（基于会话内，按 session 分组避免跨会话拼接）
    # ====================================================================
    window_session = Window.partitionBy("user_id", "session_id").orderBy("timestamp")

    df_path = df_session.withColumn(
        "prev_behavior", lag("behavior", 1).over(window_session)
    ).filter(col("prev_behavior").isNotNull())

    df_path = df_path.withColumn(
        "path_segment",
        concat_ws(" → ", col("prev_behavior"), col("behavior"))
    )

    df_path_stats = df_path.groupBy("path_segment").agg(
        count("*").alias("path_count")
    ).orderBy(desc("path_count"))

    print("[路径] 两步行为路径 TOP20:")
    df_path_stats.show(20, truncate=False)
    save_fn(df_path_stats, "behavior_paths.json", "用户行为路径分析")

    # ====================================================================
    # 三步行为路径（基于会话内）
    # ====================================================================
    df_path3 = df_session \
        .withColumn("prev1", lag("behavior", 1).over(window_session)) \
        .withColumn("prev2", lag("behavior", 2).over(window_session)) \
        .filter(col("prev1").isNotNull() & col("prev2").isNotNull()) \
        .withColumn("path3", concat_ws(" → ", col("prev2"), col("prev1"), col("behavior")))

    df_path3_stats = df_path3.groupBy("path3").agg(
        count("*").alias("count")
    ).orderBy(desc("count"))

    print("[路径] 三步行为路径 TOP20:")
    df_path3_stats.show(20, truncate=False)
    save_fn(df_path3_stats, "behavior_paths_3step.json", "三步行为路径分析")
