#!/bin/bash

# 并行拉取 HyBench 所有表数据的 Bash 脚本
# 用法: ./fetch_all_tables.sh [output_mode] [bucket_id]
# 例如: ./fetch_all_tables.sh store 0

set -e

SCHEMA="pixels_bench"
SCHEMA_FILE="config/schema_hybench.yaml"
OUTPUT_MODE="${1:-store}"
BUCKET_ID="${2:-0}"
MAX_JOBS=4

TABLES=(
    "customer"
    "company"
    "savingAccount"
    "checkingAccount"
    "transfer"
    "checking"
    "loanapps"
    "loantrans"
)

echo "======================================================"
echo "并行拉取 HyBench 所有表数据"
echo "Schema: $SCHEMA"
echo "Output Mode: $OUTPUT_MODE"
echo "Bucket ID: $BUCKET_ID"
echo "Tables: ${#TABLES[@]} 个"
echo "======================================================"

start_time=$(date +%s)

# 函数：拉取单个表
fetch_table() {
    local table=$1
    echo "[${table}] 开始拉取数据..."
    
    if PYTHONPATH=src python3 src/pixels_lance/cli.py \
        --schema "$SCHEMA" \
        --table "$table" \
        --bucket-id "$BUCKET_ID" \
        --output "$OUTPUT_MODE" > /tmp/fetch_${table}.log 2>&1; then
        echo "[${table}] ✓ 成功"
        return 0
    else
        echo "[${table}] ✗ 失败"
        tail -5 /tmp/fetch_${table}.log
        return 1
    fi
}

export -f fetch_table
export SCHEMA BUCKET_ID OUTPUT_MODE

# 并行执行
successful=0
failed=0

for table in "${TABLES[@]}"; do
    # 等待直到活跃任务数小于 MAX_JOBS
    while [ $(jobs -r | wc -l) -ge $MAX_JOBS ]; do
        sleep 0.1
    done
    
    fetch_table "$table" &
done

# 等待所有后台任务完成
wait

# 检查结果
for table in "${TABLES[@]}"; do
    if [ -f "/tmp/fetch_${table}.log" ]; then
        if grep -q "Successfully stored" /tmp/fetch_${table}.log 2>/dev/null || \
           grep -q "Print mode" /tmp/fetch_${table}.log 2>/dev/null; then
            ((successful++))
        else
            ((failed++))
        fi
    fi
done

end_time=$(date +%s)
elapsed=$((end_time - start_time))

echo ""
echo "======================================================"
echo "拉取结果汇总"
echo "======================================================"
echo "总耗时: ${elapsed}s"
echo "成功: ${successful}/${#TABLES[@]}"
echo "失败: ${failed}/${#TABLES[@]}"
echo "======================================================"

# 清理临时文件
rm -f /tmp/fetch_*.log

exit $failed
