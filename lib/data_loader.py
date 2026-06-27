"""
数据加载与质量校验模块
功能：读取Parquet数据、执行质量校验、输出校验报告
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, countDistinct


def load_data(spark, config: dict) -> DataFrame:
    """
    读取清洗后的Parquet数据

    参数:
        spark: SparkSession实例
        config: 配置字典

    返回:
        DataFrame: 缓存后的数据集
    """
    clean_path = config["data"]["clean_path"]
    df = spark.read.parquet(clean_path)
    df.cache()
    return df


def validate_data(df: DataFrame, config: dict) -> dict:
    """
    执行数据质量校验，返回校验报告

    校验项：
    1. 总行数
    2. 空值检查（各字段的null数量）
    3. 行为类型枚举校验（是否只有pv/buy/cart/fav）
    4. 时间戳范围校验
    5. 重复行检查

    参数:
        df: 待校验的DataFrame
        config: 配置字典

    返回:
        dict: 校验报告
    """
    report = {}

    # 1. 总行数
    total_count = df.count()
    report["total_rows"] = total_count

    # 2. 空值检查（isnan只对数值类型有效）
    numeric_types = ("double", "float", "decimal")
    null_counts = {}
    for field in df.columns:
        field_type = dict(df.dtypes)[field]
        condition = col(field).isNull()
        if field_type in numeric_types:
            condition = condition | isnan(col(field))
        null_count = df.filter(condition).count()
        null_counts[field] = null_count
    report["null_counts"] = null_counts
    report["has_nulls"] = any(v > 0 for v in null_counts.values())

    # 3. 行为类型枚举校验
    valid_behaviors = {"pv", "buy", "cart", "fav"}
    actual_behaviors = [
        row["behavior"] for row in
        df.select("behavior").distinct().collect()
    ]
    invalid_behaviors = set(actual_behaviors) - valid_behaviors
    report["behavior_types"] = sorted(actual_behaviors)
    report["invalid_behaviors"] = list(invalid_behaviors)

    # 4. 时间戳范围校验
    ts_start = config["filter"]["timestamp_start"]
    ts_end = config["filter"]["timestamp_end"]
    out_of_range = df.filter(
        (col("timestamp") < ts_start) | (col("timestamp") > ts_end)
    ).count()
    report["out_of_range_count"] = out_of_range

    # 5. 重复行检查
    distinct_count = df.dropDuplicates().count()
    report["duplicate_rows"] = total_count - distinct_count

    return report


def print_validation_report(report: dict):
    """打印格式化的数据质量校验报告"""
    print("\n" + "=" * 60)
    print("数据质量校验报告")
    print("=" * 60)
    print(f"  总行数:         {report['total_rows']:,}")

    # 空值报告
    if report["has_nulls"]:
        print("  ⚠ 存在空值:")
        for field, cnt in report["null_counts"].items():
            if cnt > 0:
                print(f"    - {field}: {cnt} 个空值")
    else:
        print("  ✓ 无空值")

    # 行为类型
    print(f"  行为类型:       {report['behavior_types']}")
    if report["invalid_behaviors"]:
        print(f"  ⚠ 异常行为类型: {report['invalid_behaviors']}")
    else:
        print("  ✓ 行为类型合法")

    # 时间范围
    if report["out_of_range_count"] > 0:
        print(f"  ⚠ 超出时间范围: {report['out_of_range_count']} 行")
    else:
        print("  ✓ 时间范围合规")

    # 重复行
    if report["duplicate_rows"] > 0:
        print(f"  ⚠ 重复行:       {report['duplicate_rows']} 行")
    else:
        print("  ✓ 无重复行")

    print("=" * 60 + "\n")
