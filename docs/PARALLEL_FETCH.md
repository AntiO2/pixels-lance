# 并行拉取指南

本文档介绍如何使用多表并行拉取提升数据同步效率。

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

### 1. 编辑配置

修改 `scripts/fetch_all_tables.sh` 中的表列表：

```bash
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
```

### 2. 运行脚本

```bash
chmod +x scripts/fetch_all_tables.sh
./scripts/fetch_all_tables.sh
```

### 3. 查看输出

脚本会为每个表创建单独的日志文件：

```
logs/
├── customer.log
├── company.log
├── savingAccount.log
└── ...
```

### 4. 监控进度

```bash
# 实时查看某个表的进度
tail -f logs/customer.log

# 统计完成情况
grep "成功存储" logs/*.log
```

---

## 方式二：使用 Python 脚本

### 使用 scripts/fetch_all_tables.py

```bash
python scripts/fetch_all_tables.py
```

### 脚本特点

- 自动并发拉取所有表
- 实时进度显示
- 错误自动重试
- 统计报告输出

**示例输出：**
```
开始并行拉取 8 个表...
[customer] 完成 (100 条记录, 2.3s)
[company] 完成 (50 条记录, 1.8s)
[transfer] 失败 (连接超时)
...
总耗时: 5.6s
成功: 7/8
失败: 1/8
```

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

以 8 个表为例（本地测试）：

| 模式 | 总耗时 | CPU 利用率 |
|------|--------|-----------|
| 串行 | ~24s | 25% |
| 2 并发 | ~12s | 50% |
| 4 并发 | ~6s | 90% |
| 8 并发 | ~4s | 95% |

**结论：** 并发数 = CPU 核心数时性价比最高

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
