"""
===============================================================================
任务1：数据清洗脚本
===============================================================================
功能：
  1. 从HDFS读取原始CSV文件（无表头）
  2. 定义Schema（5个字段）
  3. 过滤时间范围：仅保留 2017-11-25 00:00:00 ~ 2017-12-03 23:59:59（北京时间）的数据
  4. 删除完全重复行
  5. 将时间戳转为 yyyy-MM-dd HH:mm:ss 格式，并提取"小时"字段
  6. 以Parquet格式覆盖写入HDFS

输入：/data/raw/user.csv
输出：/data/clean/ （Parquet格式）

运行方式：
  spark-submit --master yarn task1_data_cleaning.py
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType
)
from pyspark.sql.functions import (
    col, from_unixtime, hour, to_timestamp, lit
)

# ===========================================================================
# 第一步：创建SparkSession
# ===========================================================================
# appName会显示在YARN的ResourceManager界面上，方便识别任务
spark = SparkSession.builder \
    .appName("UserBehavior_DataCleaning") \
    .getOrCreate()

# ===========================================================================
# 第二步：定义Schema
# ===========================================================================
# 原始CSV共5列，无表头，按顺序分别为：
#   用户ID(整数) | 商品ID(整数) | 商品类目ID(整数) | 行为类型(字符串) | 时间戳(整数)
schema = StructType([
    StructField("user_id",     IntegerType(), False),  # 用户ID
    StructField("item_id",     IntegerType(), False),  # 商品ID
    StructField("category_id", IntegerType(), False),  # 商品类目ID
    StructField("behavior",    StringType(),  False),  # 行为类型: pv/buy/cart/fav
    StructField("timestamp",   IntegerType(),  False),  # Unix时间戳（秒）
])

# ===========================================================================
# 第三步：读取原始CSV
# ===========================================================================
# header=False：CSV文件没有表头行
# schema：使用自定义schema，避免Spark自动推断类型
# 使用全量数据 user.csv
df_raw = spark.read.csv(
    "/data/raw/user.csv",
    header=False,
    schema=schema
)

# 打印基本信息，方便调试
print("=" * 60)
print("[任务1] 原始数据总行数：", df_raw.count())
print("[任务1] Schema信息：")
df_raw.printSchema()
print("=" * 60)

# ===========================================================================
# 第四步：过滤时间范围
# ===========================================================================
# 数据集说明：时间范围为 2017-11-25 ~ 2017-12-03
# 时间戳为Unix时间戳（秒），需要转换为北京时间（UTC+8）进行过滤
#
# 北京时间 2017-11-25 00:00:00 = Unix 1511539200
# 北京时间 2017-12-03 23:59:59 = Unix 1512316799
#
# 计算过程（UTC时间）：
#   2017-11-25 00:00:00 CST = 2017-11-24 16:00:00 UTC = 1511539200
#   2017-12-03 23:59:59 CST = 2017-12-03 15:59:59 UTC = 1512316799

TS_START = 1511539200  # 2017-11-25 00:00:00 北京时间
TS_END   = 1512316799  # 2017-12-03 23:59:59 北京时间

df_filtered = df_raw.filter(
    (col("timestamp") >= TS_START) & (col("timestamp") <= TS_END)
)

# 验证过滤结果
print("[任务1] 过滤时间范围后行数：", df_filtered.count())

# ===========================================================================
# 第五步：删除完全重复行
# ===========================================================================
# dropDuplicates() 会删除所有字段完全相同的重复行
df_deduped = df_filtered.dropDuplicates()

count_after_dedup = df_deduped.count()
print("[任务1] 去重后行数：", count_after_dedup)

# ===========================================================================
# 第六步：时间戳转换 + 提取小时字段
# ===========================================================================
# 1. 将Unix时间戳转为 "yyyy-MM-dd HH:mm:ss" 字符串（from_unixtime默认使用会话时区）
# 2. 提取小时字段（0~23），用于后续分析用户活跃时段
#
# 注意：from_unixtime 使用Spark会话的时区设置。
#       如果集群默认是UTC，需要在Spark配置中设置 spark.sql.session.timeZone=Asia/Shanghai
#       或者手动偏移8小时。这里我们采用配置时区的方式。

spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")

df_final = df_deduped \
    .withColumn(
        "datetime",
        from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    ) \
    .withColumn(
        "hour",
        hour(from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    )

# 打印样例数据，验证转换结果
print("[任务1] 转换后的样例数据：")
df_final.show(10, truncate=False)

# ===========================================================================
# 第七步：以Parquet格式写入HDFS
# ===========================================================================
# mode("overwrite")：覆盖写入，如果目标路径已存在则删除重建
# partitionBy：按日期分区，方便后续按天查询（可选，提升查询效率）
df_final \
    .withColumn("date", col("datetime").substr(1, 10)) \
    .write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet("/data/clean/")

print("=" * 60)
print("[任务1] 数据清洗完成！")
print(f"[任务1] 最终数据行数：{count_after_dedup}")
print("[任务1] 输出路径：/data/clean/ （Parquet格式，按date分区）")
print("=" * 60)

# 关闭Spark会话
spark.stop()
