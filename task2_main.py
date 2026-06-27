"""
===============================================================================
任务2：核心统计分析
===============================================================================
功能：编排分析模块，支持选择性运行
入口：
  spark-submit task2_main.py                    # 按 config + MODULE_ENABLED 决定
  spark-submit task2_main.py --only 2.1 2.8     # 强制只跑指定
  spark-submit task2_main.py --skip 2.4 2.6     # 强制跳过指定
  spark-submit task2_main.py --enable 2.4       # 强制启用（覆盖 config disabled）
  spark-submit task2_main.py --group user       # 只跑某分组
  spark-submit task2_main.py --list             # 列出所有模块 + 状态
  spark-submit task2_main.py --explore          # 只跑数据探查
===============================================================================
"""

import sys
import time
import argparse
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from lib.data_loader import load_data, validate_data, print_validation_report
from lib.json_writer import save_json
from lib.analyses import discover_analyses


def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def list_analyses(all_analyses):
    """打印所有可用分析模块及状态"""
    print("\n可用分析模块：")
    print(f"  {'状态':<6} {'编号':<6} {'名称':<20} {'分组':<10} {'输出文件'}")
    print("  " + "-" * 70)

    groups = {}
    for a in all_analyses:
        groups.setdefault(a["group"], []).append(a)

    for group, items in groups.items():
        for a in items:
            status = "[ON] " if a["enabled"] else "[OFF]"
            reason = f"  ← {a['enabled_reason']}" if a["enabled_reason"] else ""
            outputs = ", ".join(a["outputs"])
            print(f"  {status} {a['id']:<6} {a['name']:<20} {group:<10} {outputs}{reason}")
        print()


def resolve_analyses(all_analyses, args):
    """根据 CLI 参数确定要运行的分析列表"""
    # --only：强制只跑指定（忽略 enabled 状态）
    if args.only:
        selected = set(args.only)
        result = [a for a in all_analyses if a["id"] in selected]
        not_found = selected - {a["id"] for a in result}
        if not_found:
            print(f"警告：未找到编号 {not_found}")
        return result

    # --explore：只跑数据探查
    if args.explore:
        return [a for a in all_analyses if a["group"] == "explore" and a["enabled"]]

    # --group：只跑某分组（仍受 enabled 过滤）
    if args.group:
        return [a for a in all_analyses if a["group"] == args.group and a["enabled"]]

    # 默认：只跑 enabled 的模块
    result = [a for a in all_analyses if a["enabled"]]

    # --skip：从结果中移除（强制跳过）
    if args.skip:
        excluded = set(args.skip)
        result = [a for a in result if a["id"] not in excluded]

    # --enable：强制启用指定模块（即使 config disabled）
    if args.enable:
        forced = set(args.enable)
        for a in all_analyses:
            if a["id"] in forced and a["id"] not in {x["id"] for x in result}:
                result.append(a)
        result.sort(key=lambda x: x["id"])

    return result


def main():
    parser = argparse.ArgumentParser(description="淘宝用户行为分析")
    parser.add_argument("--list", action="store_true", help="列出所有可用分析")
    parser.add_argument("--only", nargs="+", help="只运行指定编号 (如 --only 2.1 2.8)")
    parser.add_argument("--skip", nargs="+", help="跳过指定编号 (如 --skip 2.4 2.6)")
    parser.add_argument("--enable", nargs="+", help="强制启用指定编号 (覆盖 config disabled)")
    parser.add_argument("--group", choices=["explore", "user", "product", "conversion"],
                        help="只运行指定分组")
    parser.add_argument("--explore", action="store_true", help="只运行数据探查")
    parser.add_argument("--config", default="config.yaml", help="配置文件路径")
    args = parser.parse_args()

    # 加载配置
    config = load_config(args.config)

    # 发现所有模块
    all_analyses = discover_analyses(config)

    if args.list:
        list_analyses(all_analyses)
        return

    # 确定要运行的模块
    to_run = resolve_analyses(all_analyses, args)
    if not to_run:
        print("没有要运行的分析")
        return

    print(f"\n将运行 {len(to_run)} 个分析：")
    for a in to_run:
        print(f"  [{a['id']}] {a['name']}")

    output_path = config["data"]["output_path"]
    spark_conf = config["spark"]

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName(spark_conf["app_name"]) \
        .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", spark_conf["timezone"])

    # 加载数据
    print("\n" + "=" * 60)
    print("加载数据...")
    print("=" * 60)
    df = load_data(spark, config)
    total_count = df.count()
    print(f"数据总行数: {total_count:,}")

    report = validate_data(df, config)
    print_validation_report(report)

    if "date" not in df.columns:
        df = df.withColumn("date", col("datetime").substr(1, 10))

    def save_fn(dataframe, filename, description=""):
        save_json(dataframe, output_path, filename, description)

    # 执行分析
    print("\n" + "=" * 60)
    print("开始执行分析任务")
    print("=" * 60)

    total_start = time.time()
    results = {}

    for a in to_run:
        name = f"{a['id']} {a['name']}"
        print(f"\n{'─' * 50}")
        print(f"[{name}] 开始执行...")
        print(f"{'─' * 50}")

        start = time.time()
        try:
            a["run"](df, config, save_fn)
            elapsed = time.time() - start
            results[name] = {"status": "✓ 成功", "time": f"{elapsed:.1f}s"}
            print(f"[{a['id']}] ✓ 完成 ({elapsed:.1f}s)")
        except Exception as e:
            elapsed = time.time() - start
            results[name] = {"status": f"✗ 失败: {e}", "time": f"{elapsed:.1f}s"}
            print(f"[{a['id']}] ✗ 失败 ({elapsed:.1f}s): {e}")

    # 执行报告
    total_elapsed = time.time() - total_start

    print("\n" + "=" * 60)
    print("执行报告")
    print("=" * 60)
    for name, result in results.items():
        print(f"  {result['status']}  {result['time']:>8s}  {name}")
    print(f"{'─' * 50}")
    print(f"  总耗时: {total_elapsed:.1f}s")
    print(f"  输出路径: {output_path}/")
    print("=" * 60)

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
