# 快速开始

本文档介绍 Pixels Lance 的核心概念和使用方法。

---

## 工作流程

```
Pixels gRPC → 拉取二进制 → 解析数据 → 存储到 LanceDB
    ↓              ↓            ↓              ↓
GrpcFetcher   RowRecord   DataParser   LanceDBStore
```

---

## 支持的数据类型

| 类型 | 字节数 | 说明 | 示例 |
|------|--------|------|------|
| **整数** | | | |
| `int8` | 1 | 有符号 8 位 | -128 ~ 127 |
| `int32` | 4 | 有符号 32 位 | -2,147,483,648 ~ 2,147,483,647 |
| `int64` | 8 | 有符号 64 位 | 超大整数 |
| `uint32` | 4 | 无符号 32 位 | 0 ~ 4,294,967,295 |
| **浮点** | | | |
| `float32` | 4 | IEEE 754 单精度 | 3.14 |
| `float64` | 8 | IEEE 754 双精度 | 3.141592653589793 |
| **字符串** | | | |
| `varchar` | 可变 | 变长字符串 | "Hello" |
| `char` | 固定 | 定长字符串 | "ABC  " |
| **日期时间** | | | |
| `date` | 4 | 自 1970-01-01 的天数 | 2024-03-06 |
| `timestamp` | 8 | Epoch 毫秒数 | 1709740800000 |
| **其他** | | | |
| `boolean` | 1 | 布尔值 | true/false |
| `decimal` | 可变 | 高精度小数 | 123.456 |

---

## CLI 使用

### 基础命令

```bash
# 拉取 customer 表的所有数据(必须指定 bucket-id)
pixels-lance --schema tpch --table customer --bucket-id 0

# 指定多个 bucket（分区）
pixels-lance --schema tpch --table customer --bucket-id 0 --bucket-id 1 --bucket-id 2

# 仅解析，不存储（调试模式）
pixels-lance --schema tpch --table customer --bucket-id 0 --dry-run
```

### 自定义配置

```bash
# 使用自定义配置文件
pixels-lance --config my-config.yaml --schema-file my-schema.yaml \
  --schema tpch --table customer
```

---

## Python API 使用

### 完整示例

```python
from pixels_lance.grpc_fetcher import PixelsGrpcFetcher, RowRecordBinaryExtractor
from pixels_lance.parser import DataParser
from pixels_lance.storage import LanceDBStore

# 1. 连接 Pixels gRPC 服务
fetcher = PixelsGrpcFetcher(host="localhost", port=6688)
fetcher.connect()

# 2. 拉取指定表的变更数据
row_records = fetcher.poll_events(
    schema_name="tpch",
    table_name="customer",
    buckets=[0, 1]  # 可选：指定 bucket IDs
)

# 3. 提取二进制数据
binary_data = RowRecordBinaryExtractor.extract_records_binary(row_records)

# 4. 解析二进制为 Python 字典
parser = DataParser(
    schema_path="config/schema_hybench.yaml",
    table_name="customer"
)
parsed_records = parser.parse_batch(binary_data)

# 5. 存储到 LanceDB（自动 upsert）
store = LanceDBStore(db_path="s3://my-bucket/lancedb")
store.upsert(
    parsed_records,
    table_name="customer",
    pk=["custID"]  # 主键
)

print(f"✓ 成功存储 {len(parsed_records)} 条记录")
```

### 查询存储的数据

```python
import lance

# 打开 LanceDB 表
dataset = lance.dataset("s3://my-bucket/lancedb/customer.lance")

# 查询前 10 条
df = dataset.to_table(limit=10).to_pandas()
print(df)

# 过滤查询
df_filtered = dataset.to_table(
    filter="age > 50",
    limit=100
).to_pandas()
```

---

## 配置文件说明

### `config/config.yaml`

```yaml
rpc:
  use_grpc: true
  grpc_host: localhost
  grpc_port: 6688

lancedb:
  db_path: ./lancedb          # 本地存储
  # db_path: s3://bucket/path # S3 存储
  mode: append                # append | overwrite
  
  # S3 配置（如使用 S3）
  storage_options:
    region: ${AWS_REGION}
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
  
  # 可选代理
  proxy: ${HTTP_PROXY:-}
```

### `config/schema_hybench.yaml`

定义表的二进制结构：

```yaml
customer:
  fields:
    - name: custID
      type: int32
      size: 4
      offset: 0
      nullable: false
    
    - name: name
      type: varchar
      size: 15
      offset: 8
      nullable: true
    
    - name: age
      type: int32
      size: 4
      offset: 29
  
  primary_key: [custID]
  record_size: 114  # 总字节数
```

### `config/.env`

敏感信息配置：

```bash
# AWS 凭证
AWS_REGION=us-east-2
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...

# 可选代理
HTTP_PROXY=http://proxy.example.com:8080
```

---

## 调试技巧

### 1. 查看解析结果

```bash
# 使用 --dry-run 仅打印，不存储
pixels-lance --schema tpch --table customer --dry-run
```

### 2. 检查配置加载

```python
from pixels_lance.config import ConfigManager

config = ConfigManager()
print(config.config.dict())
```

### 3. 验证 Schema

```python
from pixels_lance.parser import DataParser

parser = DataParser(
    schema_path="config/schema_hybench.yaml",
    table_name="customer"
)
print(f"表名: {parser.table_name}")
print(f"字段数: {len(parser.schema.fields)}")
print(f"主键: {parser.schema.primary_key}")
```

---

## 下一步

- [S3 存储配置](S3_SETUP.md) - 配置 AWS S3 或 MinIO
- [存储机制](STORAGE.md) - 了解 Upsert 和主键管理
- [并行拉取](PARALLEL_FETCH.md) - 多表并行拉取优化
