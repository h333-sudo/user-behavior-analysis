#!/bin/bash
# ====================================================================
# 一键运行全量数据分析 — 自动处理所有异常情况
# ====================================================================
# 用法: bash run_all.sh [task1|task2|export|all]
# ====================================================================

set -e

SPARK_SUBMIT="/usr/local/spark/bin/spark-submit"
SPARK_CONF="--master local[2] --conf spark.sql.session.timeZone=Asia/Shanghai --conf spark.driver.memory=2g --conf spark.local.dir=/tmp/spark --conf spark.hadoop.dfs.replication=1 --conf spark.sql.shuffle.partitions=8"
WORK_DIR="$HOME"
HDFS_CLEAN="/data/clean"
HDFS_OUTPUT="/data/output"
WEBAPP_DATA="$HOME/webapp/data"
MIN_DISK_GB=5         # 本地磁盘最低可用空间(GB)
MIN_HDFS_GB=5         # HDFS最低可用空间(GB)

# 颜色输出
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn()  { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[✗]${NC} $1"; }

# ====================================================================
# 通用检查函数
# ====================================================================
kill_stale_spark() {
    local count=$(ps aux | grep SparkSubmit | grep -v grep | wc -l)
    if [ "$count" -gt 0 ]; then
        warn "发现 $count 个残留 Spark 进程，正在清理..."
        ps aux | grep SparkSubmit | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null
        sleep 3
        info "残留进程已清理"
    fi
}

check_disk_space() {
    local avail_kb=$(df -k / | tail -1 | awk '{print $4}')
    local avail_gb=$((avail_kb / 1024 / 1024))
    if [ "$avail_gb" -lt "$MIN_DISK_GB" ]; then
        error "本地磁盘可用空间不足: ${avail_gb}GB < ${MIN_DISK_GB}GB"
        warn "正在清理临时文件..."
        rm -rf /tmp/spark-* /tmp/blockmgr-* 2>/dev/null
        avail_kb=$(df -k / | tail -1 | awk '{print $4}')
        avail_gb=$((avail_kb / 1024 / 1024))
        if [ "$avail_gb" -lt "$MIN_DISK_GB" ]; then
            error "清理后空间仍不足 (${avail_gb}GB)，请手动释放空间后重试"
            return 1
        fi
    fi
    info "本地磁盘可用: ${avail_gb}GB"
}

check_hdfs_space() {
    local hdfs_avail=$(hdfs dfsadmin -report 2>/dev/null | grep "DFS Remaining" | head -1 | awk '{print $3}')
    local hdfs_gb=$((hdfs_avail / 1024 / 1024 / 1024))
    if [ "$hdfs_gb" -lt "$MIN_HDFS_GB" ]; then
        error "HDFS 可用空间不足: ${hdfs_gb}GB < ${MIN_HDFS_GB}GB"
        return 1
    fi
    info "HDFS 可用: ${hdfs_gb}GB"
}

clean_hdfs_dir() {
    local dir=$1
    # 彻底删除目录再重建，避免残留空目录导致 saveAsTextFile 报 FileAlreadyExistsException
    if hdfs dfs -test -d "$dir" 2>/dev/null; then
        local count=$(hdfs dfs -ls "$dir" 2>/dev/null | grep -v "^Found" | wc -l)
        warn "清理旧数据 $dir ($count 个条目)..."
        hdfs dfs -rm -r -f "$dir" 2>/dev/null
        sleep 2
        # 确认删除干净
        if hdfs dfs -test -d "$dir" 2>/dev/null; then
            error "清理失败: $dir 仍存在"
            return 1
        fi
    fi
    # 重建目录
    hdfs dfs -mkdir -p "$dir" 2>/dev/null
    info "已清理并重建 $dir"
}

wait_for_task() {
    local name=$1
    local timeout_min=${2:-60}
    local start_time=$(date +%s)
    local end_time=$((start_time + timeout_min * 60))

    info "等待 $name 完成 (超时 ${timeout_min}分钟)..."

    while [ $(date +%s) -lt $end_time ]; do
        local running=$(ps aux | grep SparkSubmit | grep -v grep | wc -l)
        if [ "$running" -eq 0 ]; then
            info "$name 已完成"
            return 0
        fi
        sleep 30
    done

    error "$name 超时 (${timeout_min}分钟)"
    ps aux | grep SparkSubmit | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null
    return 1
}

# ====================================================================
# Task1: 数据清洗
# ====================================================================
run_task1() {
    echo ""
    echo "============================================================"
    echo "  Task1: 数据清洗 (3.67GB CSV → Parquet)"
    echo "============================================================"

    kill_stale_spark
    check_disk_space || return 1
    check_hdfs_space || return 1

    # 清理旧的清洗数据
    clean_hdfs_dir "$HDFS_CLEAN"

    # 检查源数据
    if ! hdfs dfs -test -e /data/raw/user.csv; then
        error "源数据 /data/raw/user.csv 不存在!"
        return 1
    fi
    local src_size=$(hdfs dfs -du -s -h /data/raw/user.csv | awk '{print $1}')
    info "源数据大小: $src_size"

    # 运行 task1
    info "启动 task1..."
    cd "$WORK_DIR"
    PYSPARK_PYTHON=python3 $SPARK_SUBMIT $SPARK_CONF task1_data_cleaning.py > task1_run.log 2>&1 &
    local pid=$!

    wait_for_task "task1" 60
    local exit_code=$?

    # 检查输出
    if hdfs dfs -test -e "$HDFS_CLEAN/_SUCCESS"; then
        local out_size=$(hdfs dfs -du -s -h "$HDFS_CLEAN" | awk '{print $1}')
        info "task1 输出: $out_size"
        return 0
    else
        error "task1 输出不完整，查看日志: task1_run.log"
        tail -10 task1_run.log
        return 1
    fi
}

# ====================================================================
# Task2: 核心统计分析
# 用法:
#   bash run_all.sh task2                   # 按 config + MODULE_ENABLED
#   bash run_all.sh task2:2.1,2.8           # --only 指定模块
#   bash run_all.sh task2:skip:2.4,2.6      # --skip 跳过
#   bash run_all.sh task2:enable:2.4        # --enable 强制启用
# ====================================================================
run_task2() {
    local task_arg=$1

    echo ""
    echo "============================================================"
    echo "  Task2: 核心统计分析"
    echo "============================================================"

    kill_stale_spark
    check_disk_space || return 1
    check_hdfs_space || return 1

    # 检查 task1 输出
    if ! hdfs dfs -test -e "$HDFS_CLEAN/_SUCCESS"; then
        error "task1 输出不存在，请先运行 task1"
        return 1
    fi

    # 清理旧的分析结果（必须彻底删除，saveAsTextFile 不允许目录已存在）
    clean_hdfs_dir "$HDFS_OUTPUT" || return 1

    # 二次确认输出目录为空
    local leftover=$(hdfs dfs -ls "$HDFS_OUTPUT" 2>/dev/null | grep -v "^Found" | wc -l)
    if [ "$leftover" -gt 0 ]; then
        error "输出目录未清理干净，仍有 $leftover 个条目"
        return 1
    fi

    # 解析模块选择参数（task2:2.1,2.8 / task2:skip:2.4,2.6 / task2:enable:2.4）
    local cli_args=""
    if [[ "$task_arg" == *":"* ]]; then
        local mode_part="${task_arg#*:}"
        if [[ "$mode_part" == skip:* ]]; then
            local ids="${mode_part#skip:}"
            cli_args="--skip $(echo "$ids" | tr ',' ' ')"
        elif [[ "$mode_part" == enable:* ]]; then
            local ids="${mode_part#enable:}"
            cli_args="--enable $(echo "$ids" | tr ',' ' ')"
        else
            cli_args="--only $(echo "$mode_part" | tr ',' ' ')"
        fi
    fi

    # 运行 task2
    info "启动 task2 ${cli_args:+(参数: $cli_args)}..."
    cd "$WORK_DIR"
    PYSPARK_PYTHON=python3 $SPARK_SUBMIT $SPARK_CONF task2_main.py $cli_args > task2_run.log 2>&1 &

    wait_for_task "task2" 60
    local exit_code=$?

    # 检查输出
    local output_count=$(hdfs dfs -ls "$HDFS_OUTPUT" 2>/dev/null | grep -v "^Found" | wc -l)
    if [ "$output_count" -gt 0 ]; then
        info "task2 输出: $output_count 个文件"

        # 检查每个分析是否成功
        echo ""
        info "分析结果检查:"
        for f in daily_uv_pv.json retention_rate.json rfm_segments.json conversion_funnel.json behavior_distribution.json; do
            if hdfs dfs -test -e "$HDFS_OUTPUT/$f"; then
                info "  ✓ $f"
            else
                warn "  ✗ $f 缺失"
            fi
        done
        return 0
    else
        error "task2 没有输出，查看日志: task2_run.log"
        grep -E "✗ 失败|ERROR" task2_run.log | head -10
        return 1
    fi
}

# ====================================================================
# Export: 导出数据到 webapp
# ====================================================================
run_export() {
    echo ""
    echo "============================================================"
    echo "  Export: 导出 JSON 到 webapp"
    echo "============================================================"

    # 检查 task2 输出
    local output_count=$(hdfs dfs -ls "$HDFS_OUTPUT" 2>/dev/null | grep -v "^Found" | wc -l)
    if [ "$output_count" -eq 0 ]; then
        error "HDFS 没有分析结果，请先运行 task2"
        return 1
    fi
    info "HDFS 输出: $output_count 个文件"

    # 清理旧的 webapp 数据
    if [ -d "$WEBAPP_DATA" ]; then
        local old_count=$(ls "$WEBAPP_DATA"/*.json 2>/dev/null | wc -l)
        if [ "$old_count" -gt 0 ]; then
            warn "清理旧 webapp 数据 ($old_count 个文件)..."
            rm -f "$WEBAPP_DATA"/*.json
        fi
    fi
    mkdir -p "$WEBAPP_DATA"

    # 导出所有 JSON
    info "开始导出..."
    local success=0
    local fail=0

    for dir in $(hdfs dfs -ls "$HDFS_OUTPUT" 2>/dev/null | grep "^d" | awk '{print $NF}'); do
        local name=$(basename "$dir")
        hdfs dfs -getmerge "$dir" "$WEBAPP_DATA/$name" 2>/dev/null
        if [ $? -eq 0 ] && [ -s "$WEBAPP_DATA/$name" ]; then
            success=$((success + 1))
        else
            fail=$((fail + 1))
            warn "  导出失败: $name"
        fi
    done

    # 导出非目录的 JSON 文件
    for f in $(hdfs dfs -ls "$HDFS_OUTPUT" 2>/dev/null | grep "^-" | awk '{print $NF}'); do
        local name=$(basename "$f")
        hdfs dfs -get "$f" "$WEBAPP_DATA/$name" 2>/dev/null
        if [ $? -eq 0 ]; then
            success=$((success + 1))
        else
            fail=$((fail + 1))
        fi
    done

    info "导出完成: 成功 $success, 失败 $fail"
    info "数据目录: $WEBAPP_DATA"
    ls -lh "$WEBAPP_DATA"/*.json 2>/dev/null | awk '{print "  " $NF " (" $5 ")"}'
}

# ====================================================================
# 主流程
# ====================================================================
main() {
    local mode=${1:-all}

    echo "============================================================"
    echo "  淘宝用户行为分析 — 全量数据运行"
    echo "  模式: $mode"
    echo "  时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"

    case $mode in
        task1)
            run_task1
            ;;
        task2|task2:*)
            run_task2 "$mode"
            ;;
        export)
            run_export
            ;;
        all)
            run_task1 && run_task2 "task2" && run_export
            ;;
        *)
            error "未知模式: $mode"
            echo "用法: bash run_all.sh [task1|task2|task2:2.1,2.8|task2:skip:2.4|task2:enable:2.4|export|all]"
            exit 1
            ;;
    esac

    echo ""
    echo "============================================================"
    echo "  运行结束: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"
}

main "$@"
