"""
轻量Flask后端 — 按需查询Parquet数据
端口：5000
启动：python3 app.py

v2: 统一数据API + 日期范围过滤
"""

import os
import json
from flask import Flask, jsonify, request
from flask_cors import cross_origin
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, when, round as _round, desc
)

app = Flask(__name__)

# ===================== 全局SparkSession =====================
spark = SparkSession.builder \
    .appName("UserBehavior_API") \
    .master("local[2]") \
    .getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")

# 读取数据（启动时加载一次，缓存到内存）
CLEAN_PATH = "/home/hadoop/webapp/data_clean/"
try:
    df = spark.read.parquet(CLEAN_PATH)
    df.cache()
    df.count()  # 触发缓存
    print(f"[API] 数据加载完成: {CLEAN_PATH}")
except Exception as e:
    print(f"[API] 警告: 无法加载 {CLEAN_PATH}, 尝试HDFS路径")
    df = spark.read.parquet("hdfs:///data/clean/")
    df.cache()
    df.count()
    print("[API] 数据从HDFS加载完成")

# 可用日期列表（统一为字符串格式 yyyy-MM-dd）
ALL_DATES = sorted([
    r["date"] if isinstance(r["date"], str) else r["date"].strftime("%Y-%m-%d")
    for r in df.select("date").distinct().collect()
])
print(f"[API] 可用日期: {ALL_DATES}")

# RFM明细（如果存在）
try:
    df_rfm = spark.read.parquet("hdfs:///data/output/rfm_detail")
    df_rfm.cache()
except:
    df_rfm = None
    print("[API] RFM明细数据不存在，分层查询功能不可用")

# JSON文件目录
DATA_DIR = "/home/hadoop/webapp/data/"


# ===================== 工具函数 =====================

def _filter_dates(query_df):
    """从请求参数中读取日期范围，过滤DataFrame"""
    ds = request.args.get("date_start")
    de = request.args.get("date_end")
    if ds:
        query_df = query_df.filter(col("date") >= ds)
    if de:
        query_df = query_df.filter(col("date") <= de)
    return query_df


# ===================== API 端点 =====================

@app.route("/api/dates")
@cross_origin()
def api_dates():
    """返回数据集中所有可用日期"""
    return jsonify(ALL_DATES)


@app.route("/api/data/<filename>")
@cross_origin()
def api_data(filename):
    """
    通用JSON数据端点
    支持 ?date_start=&date_end= 按日期过滤（仅对含date字段的文件有效）
    """
    # 安全检查：只允许读取 .json 文件
    if not filename.endswith(".json"):
        return jsonify({"error": "只支持 .json 文件"}), 400

    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        return jsonify({"error": f"文件不存在: {filename}"}), 404

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return jsonify({"error": f"读取失败: {str(e)}"}), 500

    # 按日期过滤
    ds = request.args.get("date_start")
    de = request.args.get("date_end")
    if (ds or de) and isinstance(data, list) and len(data) > 0:
        if "date" in data[0] or "first_date" in data[0]:
            date_key = "first_date" if "first_date" in data[0] else "date"
            if ds:
                data = [r for r in data if r.get(date_key, "") >= ds]
            if de:
                data = [r for r in data if r.get(date_key, "") <= de]

    return jsonify(data)


@app.route("/api/overview")
@cross_origin()
def api_overview():
    """全局概览指标（支持日期过滤）"""
    qdf = _filter_dates(df)
    total = qdf.count()
    uv = qdf.select("user_id").distinct().count()
    pv_uv = qdf.filter(col("behavior") == "pv").select("user_id").distinct().count()
    interest_uv = qdf.filter(col("behavior").isin("pv", "cart", "fav")).select("user_id").distinct().count()
    buy_uv = qdf.filter(col("behavior") == "buy").select("user_id").distinct().count()
    return jsonify({
        "total_behavior": total,
        "uv": uv,
        "pv": pv_uv,
        "buy": buy_uv,
        "cvr": round(buy_uv / interest_uv * 100, 2) if interest_uv > 0 else 0
    })


@app.route("/api/categories")
@cross_origin()
def api_categories():
    """类目列表 + 各行为统计（支持日期过滤）"""
    qdf = _filter_dates(df)
    result = qdf.groupBy("category_id").agg(
        count(when(col("behavior") == "pv", 1)).alias("pv"),
        count(when(col("behavior") == "fav", 1)).alias("fav"),
        count(when(col("behavior") == "cart", 1)).alias("cart"),
        count(when(col("behavior") == "buy", 1)).alias("buy"),
        count("*").alias("total"),
        countDistinct(when(col("behavior") == "pv", col("user_id"))).alias("pv_uv"),
        countDistinct(when(col("behavior").isin("pv", "cart", "fav"), col("user_id"))).alias("interest_uv"),
        countDistinct(when(col("behavior") == "buy", col("user_id"))).alias("buy_uv")
    ).withColumn(
        "cvr", when(col("interest_uv") > 0, _round(col("buy_uv") / col("interest_uv") * 100, 2)).otherwise(0.0)
    ).orderBy(desc("total"))

    rows = [r.asDict() for r in result.collect()]
    return jsonify(rows)


@app.route("/api/category/<int:cat_id>/items")
@cross_origin()
def api_category_items(cat_id):
    """指定类目下的商品排行（支持日期过滤）"""
    limit = request.args.get("limit", 20, type=int)
    qdf = _filter_dates(df).filter(col("category_id") == cat_id)

    result = qdf.groupBy("item_id").agg(
        count(when(col("behavior") == "pv", 1)).alias("pv"),
        count(when(col("behavior") == "fav", 1)).alias("fav"),
        count(when(col("behavior") == "cart", 1)).alias("cart"),
        count(when(col("behavior") == "buy", 1)).alias("buy"),
        count("*").alias("total"),
        countDistinct(when(col("behavior").isin("pv", "cart", "fav"), col("user_id"))).alias("interest_uv"),
        countDistinct(when(col("behavior") == "buy", col("user_id"))).alias("buy_uv")
    ).withColumn(
        "cvr", when(col("interest_uv") > 0, _round(col("buy_uv") / col("interest_uv") * 100, 2)).otherwise(0.0)
    ).orderBy(desc("total")).limit(limit)

    rows = [r.asDict() for r in result.collect()]
    return jsonify({"category_id": cat_id, "items": rows})


@app.route("/api/item/<int:item_id>")
@cross_origin()
def api_item_detail(item_id):
    """单个商品的详细行为分析（支持日期过滤）"""
    df_item = _filter_dates(df).filter(col("item_id") == item_id)

    # 各行为统计
    stats = df_item.groupBy("behavior").agg(
        count("*").alias("count")
    ).collect()
    behavior_stats = {r["behavior"]: r["count"] for r in stats}

    # 按日期的趋势
    daily = df_item.groupBy("date", "behavior").agg(
        count("*").alias("count")
    ).orderBy("date").collect()
    daily_data = [r.asDict() for r in daily]

    # 按小时分布
    hourly = df_item.groupBy("hour").agg(
        count("*").alias("count")
    ).orderBy("hour").collect()
    hourly_data = [r.asDict() for r in hourly]

    # 独立用户数
    uv = df_item.filter(col("behavior") == "pv").select("user_id").distinct().count()
    interest_uv = df_item.filter(col("behavior").isin("pv", "cart", "fav")).select("user_id").distinct().count()
    buy_uv = df_item.filter(col("behavior") == "buy").select("user_id").distinct().count()

    return jsonify({
        "item_id": item_id,
        "uv": uv,
        "behavior_stats": behavior_stats,
        "cvr": round(buy_uv / interest_uv * 100, 2) if interest_uv > 0 else 0,
        "daily_trend": daily_data,
        "hourly_dist": hourly_data
    })


@app.route("/api/segments")
@cross_origin()
def api_segments():
    """RFM分层列表（不支持日期过滤，RFM是全局统计）"""
    if df_rfm is None:
        return jsonify({"error": "RFM数据不可用"}), 404

    result = df_rfm.groupBy("user_segment").agg(
        count("*").alias("user_count")
    ).orderBy(desc("user_count"))

    rows = [r.asDict() for r in result.collect()]
    return jsonify(rows)


@app.route("/api/segment/<segment_name>/profile")
@cross_origin()
def api_segment_profile(segment_name):
    """指定用户分层的画像（支持日期过滤）"""
    if df_rfm is None:
        return jsonify({"error": "RFM数据不可用"}), 404

    # 获取该分层的用户ID列表
    user_ids = df_rfm.filter(col("user_segment") == segment_name) \
        .select("user_id").distinct()

    # 关联原始数据（带日期过滤）
    df_seg = _filter_dates(df).join(user_ids, on="user_id", how="inner")

    # 活跃时段分布
    hourly = df_seg.groupBy("hour").agg(
        count("*").alias("behavior_count")
    ).orderBy("hour").collect()

    # 偏好类目 TOP20
    category = df_seg.groupBy("category_id").agg(
        count("*").alias("behavior_count"),
        countDistinct("user_id").alias("user_count")
    ).orderBy(desc("behavior_count")).limit(20).collect()

    # 行为分布
    behavior = df_seg.groupBy("behavior").agg(
        count("*").alias("count")
    ).collect()

    return jsonify({
        "segment": segment_name,
        "user_count": user_ids.count(),
        "hourly": [r.asDict() for r in hourly],
        "top_categories": [r.asDict() for r in category],
        "behavior_dist": [r.asDict() for r in behavior]
    })


@app.route("/api/search")
@cross_origin()
def api_search():
    """通用搜索：按item_id或category_id查询"""
    item_id = request.args.get("item_id", type=int)
    cat_id = request.args.get("category_id", type=int)

    if item_id:
        return api_item_detail(item_id)
    elif cat_id:
        return api_category_items(cat_id)
    else:
        return jsonify({"error": "请提供 item_id 或 category_id 参数"}), 400


# ===================== 启动 =====================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
