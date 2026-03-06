#!/bin/bash

# 并行拉取 HyBench 所有表数据的 Bash 脚本
# 用法: ./fetch_all_tables.sh [output_mode] [bucket_num] [timeout]
# 例如: ./fetch_all_tables.sh store 4 300

set -e

SCHEMA="pixels_bench"
SCHEMA_FILE="config/schema_hybench.yaml"
OUTPUT_MODE="${1:-store}"
BUCKET_NUM="${2:-4}"
TIMEOUT="${3:-300}"  # 默认超时时间 300 秒（5分钟）

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

echo "======================================================"
echo "并行拉取 HyBench 所有表数据"
echo "Schema: $SCHEMA"
echo "Output Mode: $OUTPUT_MODE"
echo "Buckets: $BUCKET_NUM (0-$((BUCKET_NUM-1)))"
echo "Tables: ${#TABLES[@]} 个"
echo "Total Tasks: $((${#TABLES[@]} * BUCKET_NUM))"
echo "Timeout: ${TIMEOUT}s per task"
echo "======================================================"

start_time=$(date +%s)

# 函数：拉取单个表的单个bucket
fetch_table_bucket() {
    local table=$1
    local bucket_id=$2
    echo "[${table}[B${bucket_id}]] 开始拉取数据..."
    
    if timeout "${TIMEOUT}" PYTHONPATH=src python3 src/pixels_lance/cli.py \
        --schema "$SCHEMA" \
        --table "$table" \
        --bucket-id "$bucket_id" \
        --output "$OUTPUT_MODE" > /tmp/fetch_${table}_${bucket_id}.log 2>&1; then
        echo "[${table}[B${bucket_id}]] ✓ 成功"
        return 0
    else
        local exit_code=$?
        if [ $exit_code -eq 124 ]; then
            echo "[${table}[B${bucket_id}]] ✗ 超时 (>${TIMEOUT}s)"
        else
            echo "[${table}[B${bucket_id}]] ✗ 失败"
            tail -5 /tmp/fetch_${table}_${bucket_id}.log 2>/dev/null || true
        fi
        return 1
    fi
}

export -f fetch_table_bucket
export SCHEMA OUTPUT_MODE TIMEOUT

# 并行执行（无最大任务数限制，所有任务同时启动）
successful=0
failed=0

for table in "${TABLES[@]}"; do
    for ((bucket_id=0; bucket_id<BUCKET_NUM; bucket_id++)); do
        fetch_table_bucket "$table" "$bucket_id" &
    done
done

# 等待所有后台任务完成
wait

# 检查结果
for table in "${TABLES[@]}"; do
    for ((bucket_id=0; bucket_id<BUCKET_NUM; bucket_id++)); do
        if [ -f "/tmp/fetch_${table}_${bucket_id}.log" ]; then
            if grep -q "Successfully stored" /tmp/fetch_${table}_${bucket_id}.log 2>/dev/null || \
               grep -q "Print mode" /tmp/fetch_${table}_${bucket_id}.log 2>/dev/null; then
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
