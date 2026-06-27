#!/bin/bash
# ============================================================================
# spark-submit 提交脚本（重构版）
# ============================================================================
# 用途：将PySpark任务提交到YARN集群运行
# 使用方式：
#   chmod +x submit.sh
#   ./submit.sh task1    # 提交任务1（数据清洗）
#   ./submit.sh task2    # 提交任务2（核心统计）
#   ./submit.sh export   # 导出JSON到前端目录
#   ./submit.sh all      # 一键全流程
# ============================================================================

# ===================== 自动检测环境 =====================

# 脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Spark Master（YARN模式）
MASTER="yarn"

# 部署模式：client（调试用，可看print输出） 或 cluster（生产环境）
DEPLOY_MODE="client"

# 自动检测Python路径（优先conda环境，回退到系统python3）
if command -v python3 &>/dev/null; then
    PYTHON_BIN="$(which python3)"
elif command -v python &>/dev/null; then
    PYTHON_BIN="$(which python)"
else
    echo "[ERROR] 未找到Python解释器，请安装Python3或配置conda环境"
    exit 1
fi
echo "[环境] Python: ${PYTHON_BIN}"

# 设置PYSPARK_PYTHON环境变量
export PYSPARK_PYTHON="${PYTHON_BIN}"

# Spark资源配置（适配小集群：每节点2GB内存）
SPARK_CONF=(
    "--conf spark.sql.session.timeZone=Asia/Shanghai"
    "--conf spark.sql.shuffle.partitions=20"
    "--conf spark.executor.memory=1024m"
    "--conf spark.executor.cores=1"
    "--conf spark.executor.instances=2"
    "--conf spark.driver.memory=1024m"
    "--conf spark.executor.memoryOverhead=256"
    "--conf spark.dynamicAllocation.enabled=false"
)

# ===================== 任务分发逻辑 =====================

run_task1() {
    echo "============================================================"
    echo "  提交任务1：数据清洗"
    echo "  时间：$(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"

    spark-submit \
        --master ${MASTER} \
        --deploy-mode ${DEPLOY_MODE} \
        --name "UserBehavior_DataCleaning" \
        "${SPARK_CONF[@]}" \
        "${SCRIPT_DIR}/task1_data_cleaning.py"

    local exit_code=$?
    echo "[任务1] 执行完毕，退出码：${exit_code}"
    return ${exit_code}
}

run_task2() {
    echo "============================================================"
    echo "  提交任务2：核心统计分析（模块化架构）"
    echo "  时间：$(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"

    # 需要将lib目录和config.yaml一起提交
    # 在client模式下，Spark会使用本地文件系统，无需额外打包
    cd "${SCRIPT_DIR}"

    spark-submit \
        --master ${MASTER} \
        --deploy-mode ${DEPLOY_MODE} \
        --name "UserBehavior_Analysis" \
        "${SPARK_CONF[@]}" \
        "${SCRIPT_DIR}/task2_main.py"

    local exit_code=$?
    echo "[任务2] 执行完毕，退出码：${exit_code}"
    return ${exit_code}
}

run_export() {
    echo "============================================================"
    echo "  导出JSON结果到前端目录"
    echo "  时间：$(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"

    LOCAL_OUTPUT="${SCRIPT_DIR}/task3_visualization/data"
    HDFS_OUTPUT="/data/output"
    mkdir -p "${LOCAL_OUTPUT}"

    # JSON文件列表（与task2_main.py的输出一一对应）
    JSON_FILES=(
        "daily_uv_pv"
        "retention_rate"
        "rfm_segments"
        "rfm_score_distribution"
        "behavior_paths"
        "behavior_paths_3step"
        "session_stats"
        "user_lifecycle"
        "first_purchase_gap"
        "active_days_distribution"
        "hourly_cvr"
        "weekday_vs_weekend"
        "daily_funnel_trend"
        "item_hot_rank"
        "category_hot_rank"
        "item_cvr_top"
        "item_low_cvr"
        "category_cvr"
        "conversion_funnel"
        "conversion_funnel_detail"
        "behavior_distribution"
    )

    success=0
    fail=0
    for fname in "${JSON_FILES[@]}"; do
        echo -n "[导出] ${fname} ... "
        hdfs dfs -test -d "${HDFS_OUTPUT}/${fname}"
        if [ $? -ne 0 ]; then
            echo "跳过（HDFS不存在）"
            continue
        fi
        hdfs dfs -getmerge "${HDFS_OUTPUT}/${fname}" "${LOCAL_OUTPUT}/${fname}.json"
        if [ $? -eq 0 ]; then
            size=$(wc -c < "${LOCAL_OUTPUT}/${fname}.json")
            echo "OK (${size} bytes)"
            ((success++))
        else
            echo "FAILED"
            ((fail++))
        fi
    done

    echo ""
    echo "[导出] 完成：${success} 成功，${fail} 失败"
    echo "[导出] 文件位于：${LOCAL_OUTPUT}/"
}

# ===================== 主入口 =====================

case "$1" in
    task1)
        run_task1
        ;;
    task2)
        run_task2
        ;;
    export)
        run_export
        ;;
    all)
        run_task1 && run_task2 && run_export
        ;;
    *)
        echo "用法：$0 {task1|task2|export|all}"
        echo ""
        echo "  task1   - 执行数据清洗（原始CSV → Parquet）"
        echo "  task2   - 执行核心统计分析（10大维度，模块化架构）"
        echo "  export  - 从HDFS导出JSON到前端目录"
        echo "  all     - 依次执行 task1 → task2 → export"
        echo ""
        echo "示例："
        echo "  ./submit.sh task1"
        echo "  ./submit.sh task2"
        echo "  ./submit.sh export"
        echo "  ./submit.sh all"
        exit 1
        ;;
esac
