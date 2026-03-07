# 并行拉取指南

本文档介绍如何使用多表并行拉取提升数据同步效率。

---

## 快速开始

### 最简单的命令

```bash
# Hybench 导入（推荐，Python 脚本）
python3 scripts/fetch_all_tables.py

# TPC-CH 导入
python3 scripts/fetch_all_tables.py --schema-type chbenchmark

# 或使用 Bash 脚本
./scripts/fetch_all_tables.sh hybench store 4 300
./scripts/fetch_all_tables.sh chbenchmark store 4 300
```

---

## 为什么需要并行拉取？

在 HyBench 等基准测试中，通常需要同步多个表：

| 表名 | 字段数 | 平均大小 |
|------|--------|----------|
| customer | 18 | 114 bytes |
| company | 15 | 209 bytes |
| savingAccount | 6 | 32 bytes |
| checkingAccount | 6 | 32 bytes |
| transfer | 7 | 46 bytes |

**串行拉取问题：**
- 总耗时 = 表1 + 表2 + 表3 + ...
- CPU/网络利用率低

**并行拉取优势：**
- 总耗时 ≈ MAX(表1, 表2, 表3, ...)
- 充分利用多核 CPU 和网络带宽

---

## 方式一：使用 scripts/fetch_all_tables.sh

### 命令语法

```bash
./scripts/fetch_all_tables.sh [schema_type] [output_mode] [bucket_num] [timeout]
```

### 参数说明

| 参数 | 类型 | 说明 | 示例 |
|------|------|------|------|
| **schema_type** | 必需* | Benchmark 类型 | `hybench`, `chbenchmark`, `tpch` |
| **output_mode** | 必需* | 数据输出方式 | `store`, `print` |
| **bucket_num** | 必需* | Bucket数量，和pixels.properties保持一致 | `4`, `8`, `16` |
| **timeout** | 可选 | 单个任务超时时间（秒） | `300`, `600`, `3600` |

*如果不指定则使用默认值：`hybench`, `store`, `4`, `300`

### 输出模式详解

| 模式 | 说明 | 使用场景 |
|------|------|----------|
| **store** | 直接存储到 LanceDB | 正常数据导入，需要持久化 |
| **print** | 打印到标准输出（JSON） | 调试、测试、查看样本数据 |

### 常见用例

#### 1. Hybench 导入（完整）

```bash
# 默认配置（store 模式，4 分片，300s 超时）
./scripts/fetch_all_tables.sh hybench store 4 300

# 增加分片数（加快速度）
./scripts/fetch_all_tables.sh hybench store 8 300

# 增加超时时间（网络慢或数据量大）
./scripts/fetch_all_tables.sh hybench store 4 600
```

**输出示例：**
```
======================================================
并行拉取 hybench 所有表数据
Schema Type: hybench
Schema File: config/schema_hybench.yaml
RPC Schema: pixels_bench
Output Mode: store
Buckets: 4 (0-3)
Tables: 8 个
Total Tasks: 32 任务
Timeout: 300s per task
======================================================
[customer[B0]] 开始拉取数据...
[customer[B1]] 开始拉取数据...
[customer[B2]] 开始拉取数据...
[customer[B3]] 开始拉取数据...
[company[B0]] 开始拉取数据...
...
[customer[B0]] 成功
[customer[B1]] 成功
...
```

#### 2. TPC-CH 导入（完整）

```bash
# 标准配置
./scripts/fetch_all_tables.sh chbenchmark store 4 300

# 或别名方式
./scripts/fetch_all_tables.sh tpch store 4 300
```

**支持的表（共 12 个）：**
```
warehouse, district, customer, history, neworder, order,
orderline, item, stock, nation, supplier, region
```

#### 3. 调试模式（仅打印数据）

```bash
# 打印前 10 条记录（不存储）
./scripts/fetch_all_tables.sh hybench print 1 300

# 用于快速验证数据格式
./scripts/fetch_all_tables.sh chbenchmark print 2 300
```

### 分片参数（bucket_num）

**含义：** 数据被分成 N 个分片（bucket），并行拉取

```
总记录数 = N * bucket_size
并行度 = bucket_num
总耗时 ≈ 单个 bucket 耗时
```

**推荐值：**
- 小数据集（<1GB）：`bucket_num=2` 或 `4`
- 中等数据集（1-100GB）：`bucket_num=4` 或 `8`
- 大数据集（>100GB）：`bucket_num=8` 或 `16`

**性能对比：**
```
bucket_num=1: 串行，慢
bucket_num=4: 4 个并行任务
bucket_num=8: 8 个并行任务，需要更多 CPU/内存
bucket_num=16: 极限并行，可能导致 RPC 限流
```

### 超时参数（timeout）

**含义：** 单个 task（一个表的一个分片）的最大执行时间

```
timeout = 300s → 如果任务超过 5 分钟就认为失败
```

**选择建议：**
- 开发/测试：`300` 秒
- 生产（网络好）：`300-600` 秒
- 网络慢或数据大：`600-1800` 秒

### 监控进度

```bash
# 实时查看临时日志
ls -lh /tmp/fetch_*.log | wc -l

# 查看特定表的进度
tail -f /tmp/fetch_customer_0.log

# 统计成功的任务
grep -r "Successfully" /tmp/fetch_*.log | wc -l

# 统计失败的任务
grep -r "Failed\|Error\|超时" /tmp/fetch_*.log | wc -l
```

---

## 方式二：使用 Python 脚本

### 命令语法

```bash
python3 scripts/fetch_all_tables.py [--schema-type TYPE] [--output-mode MODE] [--bucket-num N] [--tables T1 T2 ...] [--execution-mode thread|process] [--timeout SECONDS]
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| **--schema-type** | 可选 | `hybench` | Benchmark 类型：`hybench`, `chbenchmark`, `tpch` |
| **--output-mode** | 可选 | `store` | 输出方式：`store`（存储）, `print`（打印） |
| **--bucket-num** | 可选 | `4` | 每张表的分片数；每个 `(table, bucket)` 任务都会并行执行 |
| **--tables** | 可选 | 全部 | 仅导入指定表（用空格分隔） |
| **--execution-mode** | 可选 | `process` | 并行模式：`process`（多进程）或 `thread`（多线程） |
| **--timeout** | 可选 | `300` | 每个任务的超时时间（秒） |

### 常见用例

#### 1. Hybench 导入（推荐用法）

```bash
# 默认配置（自动并行所有 `(table, bucket)` 任务）
python3 scripts/fetch_all_tables.py

# 显式使用多进程（默认即为 process）
python3 scripts/fetch_all_tables.py --execution-mode process

# 切换为多线程
python3 scripts/fetch_all_tables.py --execution-mode thread

# 增加分片数（任务数 = 表数 × bucket_num）
python3 scripts/fetch_all_tables.py --bucket-num 8

# 自定义超时时间（600秒 = 10分钟）
python3 scripts/fetch_all_tables.py --timeout 600

# 仅导入特定表
python3 scripts/fetch_all_tables.py --tables customer company transfer
```

#### 2. TPC-CH 导入

```bash
# 默认配置
python3 scripts/fetch_all_tables.py --schema-type chbenchmark

# 或使用别名
python3 scripts/fetch_all_tables.py --schema-type tpch

# 快速并行
python3 scripts/fetch_all_tables.py --schema-type chbenchmark --bucket-num 4 --timeout 2000
```

#### 3. 调试模式（打印数据）

```bash
# 打印数据（不存储）
python3 scripts/fetch_all_tables.py --output-mode print

# 仅调试某个表
python3 scripts/fetch_all_tables.py --output-mode print --tables customer

# 快速验证格式
python3 scripts/fetch_all_tables.py --output-mode print --bucket-num 1
```

### Python vs Bash 脚本对比

| 特性 | Bash | Python |
|------|------|--------|
| 参数风格 | 位置参数 | 命名参数（更易懂） |
| 并行方式 | 纯 Bash 后台任务 | ProcessPoolExecutor / ThreadPoolExecutor |
| 并行粒度 | 表 + bucket 全并行 | 表 + bucket 全并行 |
| 错误处理 | 基础 | ✓ 更详细 |
| 易用性 | 简单 | ✓ 推荐 |

### 性能建议

```bash
# 小集群（<4 核）
python3 scripts/fetch_all_tables.py --bucket-num 2

# 中型集群（4-8 核）
python3 scripts/fetch_all_tables.py --bucket-num 4

# 大型集群（>8 核）
python3 scripts/fetch_all_tables.py --bucket-num 8

# 网络好的极限并行
python3 scripts/fetch_all_tables.py --bucket-num 16
```

---

## Bash vs Python 方式选择

**使用 Bash 脚本（fetch_all_tables.sh）：**
- ✓ 简单快速
- ✓ 无 Python 环境依赖
- ✗ 参数固定的位置顺序
- ✗ 并行度有限制

**使用 Python 脚本（fetch_all_tables.py）：**  
- ✓ 参数更灵活（命名参数）
- ✓ 更好的并行性能
- ✓ 更详细的进度和错误信息
- ✓ 支持表级选择
- ✗ 需要 Python 3.8+

**推荐：优先使用 Python 脚本

---

## 方式三：自定义并行脚本

### 基础版本

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from pixels_lance.grpc_fetcher import PixelsGrpcFetcher, RowRecordBinaryExtractor
from pixels_lance.parser import DataParser
from pixels_lance.storage import LanceDBStore

# 定义表列表
TABLES = ["customer", "company", "savingAccount", "checkingAccount"]

def fetch_table(table_name):
    """拉取单个表"""
    try:
        # 1. 连接 gRPC
        fetcher = PixelsGrpcFetcher(host="localhost", port=6688)
        fetcher.connect()
        
        # 2. 拉取数据
        row_records = fetcher.poll_events("tpch", table_name, buckets=[0])
        binary_data = RowRecordBinaryExtractor.extract_records_binary(row_records)
        
        # 3. 解析
        parser = DataParser("config/schema_hybench.yaml", table_name)
        parsed = parser.parse_batch(binary_data)
        
        # 4. 存储
        store = LanceDBStore()
        store.upsert(parsed, table_name, pk=parser.schema.primary_key)
        
        return f"{table_name}: {len(parsed)} 条记录"
    
    except Exception as e:
        return f"{table_name} 失败: {str(e)}"

# 并行执行
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(fetch_table, t): t for t in TABLES}
    
    for future in as_completed(futures):
        print(future.result())
```

### 高级版本（带进度条）

```python
from tqdm import tqdm

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(fetch_table, t): t for t in TABLES}
    
    # 使用 tqdm 显示进度
    for future in tqdm(as_completed(futures), total=len(TABLES)):
        result = future.result()
        tqdm.write(result)
```

---

## 性能调优

### 1. 调整并发数

```python
# 根据 CPU 核心数和网络带宽调整
ThreadPoolExecutor(max_workers=8)  # 8 个并发
```

**建议值：**
- CPU 核心数 × 2
- 网络带宽限制时降低

### 2. 批量大小

```python
store.upsert(
    parsed,
    table_name,
    pk=["custID"],
    batch_size=500  # 调整批量大小
)
```

**建议：**
- 小表（<1000 条）：batch_size=100
- 大表（>10000 条）：batch_size=1000

### 3. gRPC 连接池

复用 gRPC 连接：

```python
# 全局连接（单例模式）
class FetcherPool:
    _fetcher = None
    
    @classmethod
    def get_fetcher(cls):
        if cls._fetcher is None:
            cls._fetcher = PixelsGrpcFetcher("localhost", 6688)
            cls._fetcher.connect()
        return cls._fetcher
```

---

## 性能对比

---

## 注意事项

### 1. 资源限制

避免过高并发导致：
- 内存不足
- 网络拥塞
- gRPC 连接数过多

### 2. 错误处理

并行拉取时，单个表失败不应影响其他表：

```python
def fetch_table(table_name):
    try:
        # ... 拉取逻辑
        return True, f"{table_name} 成功"
    except Exception as e:
        logger.error(f"表 {table_name} 失败: {e}")
        return False, f"{table_name} 失败: {e}"
```

### 3. S3 写入冲突

并发写入同一 S3 bucket 时，LanceDB 会自动处理版本冲突，但可能影响性能。

**建议：** 不同表使用不同的表名（避免冲突）

---

## 相关文档

- [快速开始](QUICKSTART.md) - 单表拉取教程
- [存储机制](STORAGE.md) - 了解 Upsert 操作
- [S3 配置](S3_SETUP.md) - 配置对象存储
