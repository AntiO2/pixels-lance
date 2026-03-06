#!/usr/bin/env bash

# 并行拉取多表数据的 Bash 脚本（支持 HyBench、TPC-CH 等）
# 用法: ./scripts/fetch_all_tables.sh [schema_type] [output_mode] [bucket_num] [timeout]
# 例如: ./scripts/fetch_all_tables.sh hybench store 4 300
#      ./scripts/fetch_all_tables.sh chbenchmark store 4 300

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# 支持的 schema 类型
SCHEMA_TYPE="${1:-hybench}"
OUTPUT_MODE="${2:-store}"
BUCKET_NUM="${3:-4}"
TIMEOUT="${4:-300}" # 默认超时时间 300 秒（5分钟）

# 根据 schema 类型设置表列表和 RPC schema 名称
case "$SCHEMA_TYPE" in
    hybench)
        SCHEMA="pixels_bench"
        SCHEMA_FILE="config/schema_hybench.yaml"
        TABLES=(
            "customer"
            "company"
            "savingaccount"
            "checkingaccount"
            "transfer"
            "checking"
            "loanapps"
            "loantrans"
        )
        ;;
    chbenchmark|tpch)
        SCHEMA="pixels_bench"  # RPC 端 schema 名称（TPC-CH 使用 pixels_bench）
        SCHEMA_FILE="config/schema_chbenchmark.yaml"
        TABLES=(
            "warehouse"
            "district"
            "customer"
            "history"
            "neworder"
            "order"
            "orderline"
            "item"
            "stock"
            "nation"
            "supplier"
            "region"
        )
        ;;
    *)
        echo "Error: Unknown schema type '$SCHEMA_TYPE'"
        echo "Supported types: hybench, chbenchmark, tpch"
        exit 1
        ;;
esac

echo "======================================================"
echo "并行拉取 $SCHEMA_TYPE 所有表数据"
echo "Schema Type: $SCHEMA_TYPE"
echo "Schema File: $SCHEMA_FILE"
echo "RPC Schema: $SCHEMA"
echo "Output Mode: $OUTPUT_MODE"
echo "Buckets: $BUCKET_NUM (0-$((BUCKET_NUM - 1)))"
echo "Tables: ${#TABLES[@]} 个"
echo "Total Tasks: $((${#TABLES[@]} * BUCKET_NUM))"
echo "Timeout: ${TIMEOUT}s per task"
echo "Repo Root: $REPO_ROOT"
echo "======================================================"

start_time=$(date +%s)

# 函数：拉取单个表的单个bucket
fetch_table_bucket() {
    local table=$1
    local bucket_id=$2
    local cli_py="$REPO_ROOT/src/pixels_lance/cli.py"
    local log_file="/tmp/fetch_${table}_${bucket_id}.log"

    echo "[${table}[B${bucket_id}]] 开始拉取数据..."

    if timeout "${TIMEOUT}" python3 "$cli_py" \
        --config "$REPO_ROOT/config/config.yaml" \
        --schema-file "$REPO_ROOT/$SCHEMA_FILE" \
        --schema "$SCHEMA" \
        --table "$table" \
        --bucket-id "$bucket_id" \
        --output "$OUTPUT_MODE" >"$log_file" 2>&1; then
        echo "[${table}[B${bucket_id}]] 成功"
        return 0
    else
        local exit_code=$?
        if [ "$exit_code" -eq 124 ]; then
            echo "[${table}[B${bucket_id}]] 超时 (>${TIMEOUT}s)"
        else
            echo "[${table}[B${bucket_id}]] 失败"
            tail -5 "$log_file" 2>/dev/null || true
        fi
        return 1
    fi
}

# 函数：拉取单个表的所有 bucket（表内并行）
fetch_table_all_buckets() {
    local table=$1
    local pids=()

    for ((bucket_id = 0; bucket_id < BUCKET_NUM; bucket_id++)); do
        fetch_table_bucket "$table" "$bucket_id" &
        pids+=("$!")
    done

    # 等待当前表的所有 bucket 任务完成
    for pid in "${pids[@]}"; do
        wait "$pid" || true
    done
}

export -f fetch_table_bucket
export -f fetch_table_all_buckets
export SCHEMA OUTPUT_MODE REPO_ROOT BUCKET_NUM

# 并行执行：不同表并行；同一表内 bucket 也并行
successful=0
failed=0

for table in "${TABLES[@]}"; do
    fetch_table_all_buckets "$table" &
done

# 等待所有后台任务完成
wait

# 检查结果
for table in "${TABLES[@]}"; do
    for ((bucket_id = 0; bucket_id < BUCKET_NUM; bucket_id++)); do
        if [ -f "/tmp/fetch_${table}_${bucket_id}.log" ]; then
            if grep -q "Successfully stored" "/tmp/fetch_${table}_${bucket_id}.log" 2>/dev/null || \
                grep -q "Print mode" "/tmp/fetch_${table}_${bucket_id}.log" 2>/dev/null; then
                ((successful++))
            else
                ((failed++))
            fi
        fi
    done
done

end_time=$(date +%s)
elapsed=$((end_time - start_time))
total_tasks=$((${#TABLES[@]} * BUCKET_NUM))

echo ""
echo "======================================================"
echo "拉取结果汇总"
echo "======================================================"
echo "总耗时: ${elapsed}s"
echo "成功: ${successful}/${total_tasks}"
echo "失败: ${failed}/${total_tasks}"
echo "======================================================"

# 清理临时文件
rm -f /tmp/fetch_*.log

exit $failed
