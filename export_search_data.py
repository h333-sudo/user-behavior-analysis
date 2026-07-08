"""导出搜索数据：商品详情、类目详情、类目商品列表"""
import json
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when, round as _round, desc, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("export_search").master("local[2]").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")
df = spark.read.parquet("hdfs:///data/clean/")
df.cache()
print(f"总行数: {df.count():,}")

# 1. 商品详情 TOP200
print("=== 导出商品详情 ===")
item_stats = df.groupBy("item_id").agg(
    count(when(col("behavior")=="pv",1)).alias("pv"),
    count(when(col("behavior")=="fav",1)).alias("fav"),
    count(when(col("behavior")=="cart",1)).alias("cart"),
    count(when(col("behavior")=="buy",1)).alias("buy"),
    count("*").alias("total"),
    countDistinct(when(col("behavior")=="pv",col("user_id"))).alias("pv_uv"),
    countDistinct(when(col("behavior").isin("pv","cart","fav"),col("user_id"))).alias("interest_uv"),
    countDistinct(when(col("behavior")=="buy",col("user_id"))).alias("buy_uv")
).withColumn("cvr", when(col("interest_uv")>0, _round(col("buy_uv")/col("interest_uv")*100,2)).otherwise(0.0)
).orderBy(desc("total")).limit(200)
items = [r.asDict() for r in item_stats.collect()]
with open("/home/hadoop/webapp/data/item_details.json","w") as f:
    json.dump(items, f, ensure_ascii=False)
print(f"  item_details.json: {len(items)} items")

# 2. 类目详情 TOP100
print("=== 导出类目详情 ===")
cat_stats = df.groupBy("category_id").agg(
    count(when(col("behavior")=="pv",1)).alias("pv"),
    count(when(col("behavior")=="fav",1)).alias("fav"),
    count(when(col("behavior")=="cart",1)).alias("cart"),
    count(when(col("behavior")=="buy",1)).alias("buy"),
    count("*").alias("total"),
    countDistinct(when(col("behavior")=="pv",col("user_id"))).alias("pv_uv"),
    countDistinct(when(col("behavior").isin("pv","cart","fav"),col("user_id"))).alias("interest_uv"),
    countDistinct(when(col("behavior")=="buy",col("user_id"))).alias("buy_uv")
).withColumn("cvr", when(col("interest_uv")>0, _round(col("buy_uv")/col("interest_uv")*100,2)).otherwise(0.0)
).orderBy(desc("total")).limit(100)
cats = [r.asDict() for r in cat_stats.collect()]
with open("/home/hadoop/webapp/data/category_details.json","w") as f:
    json.dump(cats, f, ensure_ascii=False)
print(f"  category_details.json: {len(cats)} categories")

# 3. 每个类目下的商品TOP20
print("=== 导出类目商品列表 ===")
cat_items = df.groupBy("category_id","item_id").agg(
    count(when(col("behavior")=="pv",1)).alias("pv"),
    count(when(col("behavior")=="fav",1)).alias("fav"),
    count(when(col("behavior")=="cart",1)).alias("cart"),
    count(when(col("behavior")=="buy",1)).alias("buy"),
    count("*").alias("total"),
    countDistinct(when(col("behavior").isin("pv","cart","fav"),col("user_id"))).alias("interest_uv"),
    countDistinct(when(col("behavior")=="buy",col("user_id"))).alias("buy_uv")
).withColumn("cvr", when(col("interest_uv")>0, _round(col("buy_uv")/col("interest_uv")*100,2)).otherwise(0.0)
)
w = Window.partitionBy("category_id").orderBy(desc("total"))
ranked = cat_items.withColumn("rank", row_number().over(w)).filter(col("rank")<=20)
result = {}
for row in ranked.collect():
    d = row.asDict()
    cid = d["category_id"]
    if cid not in result:
        result[cid] = []
    result[cid].append({k: v for k, v in d.items() if k != "rank"})
with open("/home/hadoop/webapp/data/category_items.json","w") as f:
    json.dump(result, f, ensure_ascii=False)
print(f"  category_items.json: {len(result)} categories with items")

spark.stop()
print("Done!")
