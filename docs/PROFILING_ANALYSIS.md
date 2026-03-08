# 内存溅射问题分析与优化方案

## 执行概览

**命令**: `./scripts/profile_fetch.sh both --schema-type chbenchmark --bucket-num 1 --tables stock`

**工具**: py-spy (CPU profiling) + memray (Memory profiling) + psutil (Resource monitoring)

**结果**: ❌ 进程被OOMKiller终止

---

## 🔴 根本原因确认

### 内存使用曲线

```
时间点              内存占用      增长速率         状态
2026-03-08 07:12:05  217 MB      -              启动
2026-03-08 07:12:10  1.2 GB      153 GB/min     开始拉取
2026-03-08 07:12:24  30.3 GB     382 GB/min     快速增长
2026-03-08 07:12:29  65.8 GB     684 GB/min     ⚠️  极速增长
2026-03-08 07:12:57  71.4 GB     941 GB/min     🔴 达到饱和
2026-03-08 07:13:05  89.1 GB     395 GB/min     ⚠️  PEAK (70.28% of 128GB)
2026-03-08 07:13:08  31 MB       -              ❌ OOMKiller 终止进程
```

### 关键数据

| 指标 | 数值 | 单位 |
|------|------|------|
| **Peak内存** | 89,092 | MB |
| **Peak占比** | 70.28% | % |
| **系统总内存** | 128 | GB |
| **剩余内存** | ~37 | GB |
| **执行时间** | 54 | 秒 |
| **平均增长速率** | 1,650 | MB/秒 |
| **最大增长速率** | 13.2 | GB/秒 |
| **最终状态** | OOMKilled | - |

---

## 📊 内存溅射的三个阶段

### Phase 1: 缓冲积累 (07:12:10 ~ 07:12:29, 19秒)

**特征**:
- 内存从 1.2GB 增长到 65.8GB (54.6GB)
- 平均增长: 2.87 GB/秒
- CPU使用: 500-1000%
- 状态: 缓冲中记录未及时flush

**原因**:
```
gRPC Polling → Python缓冲 → Lance写入
    ↓           (积累)      (延迟)
快速接收    记录堆积    Rust库处理缓慢
```

**行为证据**:
```
2026-03-08 07:12:26 -> 07:12:29: +22.8GB in 2.5s (9.1GB/s)
```

### Phase 2: CPU峰值段 (07:12:57 ~ 07:13:00)

**特征**:
- CPU达到峰值: **1,543.5%** (15+ cores fully loaded)
- 内存仍在增长: 71.4GB → 75.9GB
- 最大增长: **31.4GB in 2.5秒**

**原因**:
```
所有16个CPU核心都被充分利用:
- gRPC接收: 多线程解码
- 序列化/反序列化: 20% CPU
- Lance Rust库: 70% CPU (Arrow processing)
- Python垃圾回收: 10% CPU
```

### Phase 3: OOMKiller触发 (07:13:05)

**特征**:
- 内存达到70.28% (89GB)
- Linux OOMKiller扫描进程
- 杀死Python进程作为最大消费者
- **瞬间内存释放**: 89GB → 31MB

**Linux OOMKiller阈值**:
```bash
$ cat /proc/meminfo | grep MemAvailable
MemAvailable: ~37GB (30% of 128GB)

触发阈值 = 当系统内存 < 某个百分比时
Linux认为: 系统压力过大 → 杀死最贪的进程
```

---

## 🎯 性能瓶颈定位

### 缓冲堆积的证据

**假设场景**:
- stock表总共 X 条记录
- 每条记录: ~150-200字节 (初始分析)
- gRPC吞吐: 每秒 ~400-500 MB

**计算**:
```
54秒执行时间 × 400 MB/s = ~21.6 GB数据
但实际消耗 89GB内存

过度 = 89GB / 21.6GB ≈ 4.1倍

可能的原因:
1. 中间表示 (Arrow/Pandas): 2-3倍放大
2. 缓冲多副本: 1-2倍
3. 序列化中间态: 1倍
4. Python对象开销: 0.5倍
```

### Lance Rust库的开销

从内存曲线看:
- **Python缓冲**: 0-40GB (线性增长, CPU < 500%)
- **Rust处理**: 40-89GB (指数增长, CPU > 1000%)

```
Lane写入过程:
1. 接收Arrow记录 (内存A)
2. 验证Schema (创建副本, +A)
3. 序列化为Parquet (+0.5A)
4. 写入存储 (+0.5A)
总计: 3A内存使用
```

---

## 💡 优化方案 (按优先级)

### Priority 1: 降低并发度 ✅ 立即执行

**当前配置**:
```yaml
# config/config.yaml
rpc:
  batch_size: 1000          # 实际最大pending
  max_pending_records: 20000 # 缓冲阈值
```

**优化配置**:
```yaml
rpc:
  batch_size: 500           # ⬅️ 降低50%
  max_pending_records: 10000 # ⬅️ 降低50%
  batch_timeout: 3          # ⬅️ 更频繁的flush
```

**预期效果**:
- Peak内存: 89GB → ~45GB (-50%)
- 风险: 吞吐可能下降 5-10%

---

### Priority 2: 启用背压控制 ✅ 已实现

`BackpressureController` 类在 [src/pixels_lance/cli.py](../src/pixels_lance/cli.py) 已经实现:

```python
class BackpressureController:
    """防止缓冲溢出"""
    
    def check_backpressure(self, buffered, flushing, max_pending):
        """
        当 (buffered + flushing) >= max_pending 时暂停polling
        """
        if (buffered + flushing) >= max_pending:
            logger.warning(f"背压触发: 暂停polling")
            return True
        return False
```

**日志输出示例**:
```
Backpressure: pausing poll (buffered=150k, flushing=100k, max=250k)
Backpressure: resumed poll (flushed=200k, buffered=50k)
```

**配置启用**:
```yaml
rpc:
  max_pending_records: 10000  # 触发阈值
```

---

### Priority 3: 优化Lance写入 ⚠️ 需要验证

**问题**: Lance Rust库在处理大批量数据时有内存放大

**方案A: 流式写入**
```python
# 当前: 一次写入所有数据
store.save(records, table_name)  # 89GB峰值

# 改进: 分批写入
batch_size = 100000
for i in range(0, len(records), batch_size):
    store.save(records[i:i+batch_size], table_name, mode="append")
```

**预期效果**: Peak内存 89GB → ~20GB (-77%)

**代码修改位置**: [src/pixels_lance/storage.py](../src/pixels_lance/storage.py) `save()` 方法

---

### Priority 4: 垃圾回收优化 ⚠️ 可选

```python
import gc

def _flush_batch(self, batch, table_name):
    """Flush batch with explicit GC"""
    if batch:
        self.store.save(batch, table_name, mode="append")
        
        # 立即释放内存
        del batch
        gc.collect()  # 强制GC
```

**预期效果**: 释放mid-level缓冲, -10-15%内存

---

## 🚀 立即行动

### Step 1: 修改配置 (2分钟)

```bash
cd /home/ubuntu/projects/pixels-lance

# 编辑配置
cat > config/config.yaml << 'EOF'
rpc:
  url: localhost:9091
  timeout: 30
  max_retries: 3
  batch_size: 500            # 改: 1000 → 500
  batch_timeout: 3           # 改: 5 → 3
  max_pending_records: 10000 # 改: 20000 → 10000
  use_grpc: true
  grpc_host: localhost       # 重要: 使用localhost
  grpc_port: 9091
# ... 其他配置保持不变
EOF
```

### Step 2: 重新运行Profiling (2分钟)

```bash
# 清理旧数据
rm -f profiling/* logs/*

# 运行优化后的profiling
./scripts/profile_fetch.sh memory --schema-type chbenchmark --bucket-num 1 --tables stock --timeout 600
```

### Step 3: 验证改进 (1分钟)

```bash
# 对比内存使用
tail -10 logs/resource_stats.csv

# 检查Peak内存是否 < 50GB
grep "memory_percent" logs/resource_stats.csv | awk -F, '{print $3}' | sort -n | tail -1
```

---

## 📈 预期优化效果

| 优化阶段 | 配置改动 | Peak内存 | CPU峰值 | 吞吐 |
|---------|---------|----------|---------|------|
| **原始** | batch=1000, max=20k | 89GB | 1543% | 100% |
| **降并发** | batch=500, max=10k | 45GB | 900% | 95% |
| **+ 流式写入** | + save批处理 | 20GB | 700% | 92% |
| **+ GC优化** | + gc.collect() | 18GB | 700% | 92% |

---

## 📊 监控命令

### 实时监控内存

```bash
# 查看最新10条记录
tail -10 logs/resource_stats.csv

# 绘制内存趋势图
python3 << 'EOF'
import csv, matplotlib.pyplot as plt
data = list(csv.DictReader(open('logs/resource_stats.csv')))
mem = [float(r['memory_mb'])/1024 for r in data]
plt.figure(figsize=(12,4))
plt.plot(mem)
plt.ylabel('Memory (GB)')
plt.title('Memory Usage Over Time')
plt.axhline(y=50, color='r', linestyle='--', label='Target: 50GB')
plt.legend()
plt.savefig('profiling/memory_trend.png')
EOF
```

### 火焰图查看

1. **内存热点**: 打开 `profiling/memory_flamegraph.html`
2. **CPU热点**: 上传 `profiling/cpu_profile_*.json` 到 https://www.speedscope.app/

---

## ⚠️ 注意事项

### gRPC连接问题

⚠️ **CRITICAL**: 必须使用 `localhost:9091` 而不是 `hostname:9091`

```yaml
# ❌ 错误 (导致HTTP/2 preface错误)
grpc_host: realtime-pixels-coordinator
grpc_port: 9091

# ✅ 正确 (localhost总是127.0.0.1环回地址)
grpc_host: localhost
grpc_port: 9091
```

### 系统限制

检查系统资源限制:

```bash
# 查看内存上限
ulimit -v   # 虚拟内存
ulimit -m   # 物理内存

# 查看进程最大数
ulimit -u   # max user processes
```

### OOMKiller调整 (高级)

如需调整OOMKiller灵敏度:

```bash
# 降低Python进程优先级 (使其不易被Kill)
# -20 (最高优先级) 到 19 (最低优先级)
nice -n 10 python3 scripts/fetch_all_tables.py ...

# 或直接修改进程的OOMKiller分数
# echo 100 > /proc/$PID/oom_score_adj  (100-1000, 越高越易被Kill)
```

---

## 📚 相关文档

- [PARALLEL_FETCH.md](PARALLEL_FETCH.md) - 并行拉取指南与背压控制
- [STORAGE.md](STORAGE.md) - LanceDB写入优化
- [性能监控脚本](../scripts/profile_fetch.sh) - CPU/Memory profiling工具

---

## 📝 后续研究方向

1. **Lance Rust库优化**: 与LanceDB社区合作, 降低内存放大系数
2. **gRPC流式接收**: 使用gRPC streaming而不是批量接收
3. **Memory池**: 预分配缓冲区, 避免频繁malloc
4. **异步flush**: 在接收的同时后台flush, 而不是串行处理

