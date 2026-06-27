"""
JSON输出工具模块
功能：将DataFrame收集到Driver端，序列化为标准JSON单文件写入HDFS
"""

import json
from pyspark.sql import DataFrame


def save_json(df: DataFrame, output_path: str, filename: str, description: str = ""):
    """
    将DataFrame转为JSON写入HDFS（单文件输出）

    注意：此方法会将所有数据collect到Driver端，仅适用于聚合后的少量结果。

    参数:
        df: 待保存的DataFrame（应为聚合后的小结果集）
        output_path: HDFS输出目录路径
        filename: 文件名（不含路径）
        description: 打印描述信息
    """
    spark = df.sparkSession
    sc = spark.sparkContext

    # 收集到Driver端并转为字典列表
    rows = df.collect()
    data = [row.asDict() for row in rows]

    # 序列化为JSON字符串
    json_str = json.dumps(data, ensure_ascii=False, indent=2, default=str)

    # 写入HDFS（单文件输出）
    full_path = f"{output_path}/{filename}"
    sc.parallelize([json_str]) \
        .coalesce(1) \
        .saveAsTextFile(full_path)

    print(f"  ✓ {description or filename} → {full_path} ({len(data)} 条记录)")
