# 存储机制说明

本文档介绍 Pixels Lance 的 LanceDB 存储机制、Upsert 操作和主键管理。

---

## LanceDB 简介

**LanceDB** 是一个基于 Lance 格式的列式数据库，特点：

- 列式存储，查询性能高
- 支持嵌入向量（embedding）
- 与 Arrow/Pandas 无缝集成
- 支持 S3、GCS 等对象存储
- 零依赖部署

---

## Upsert 操作

### 什么是 Upsert？

**Upsert = Update + Insert**
- 如果主键存在 → 更新（Update）
- 如果主键不存在 → 插入（Insert）

### 为什么需要 Upsert？

在 CDC（Change Data Capture）场景中，数据可能重复拉取：

```
拉取 1: {custID: 1, name: "Alice", age: 30}
拉取 2: {custID: 1, name: "Alice", age: 31}  # 同一用户，年龄更新
```

使用 Upsert 可以：
- 自动去重
- 保留最新数据
- 避免主键冲突

---

## 主键（Primary Key）

### 定义主键

在 `config/schema_hybench.yaml` 中定义：

```yaml
customer:
  fields:
    - name: custID
      type: int32
      size: 4
      offset: 0
  
  primary_key: [custID]  # ✨ 主键定义
```

### 复合主键

支持多字段组合主键：

```yaml
transfer:
  fields:
    - name: fromAccountID
      type: int32
    - name: toAccountID
      type: int32
    - name: transID
      type: int32
  
  primary_key: [fromAccountID, toAccountID, transID]  # 复合主键
```

---

## 存储模式

### 1. Append 模式（默认）

追加模式，**保留所有历史数据**：

```yaml
lancedb:
  mode: append
```

**特点：**
- 保留完整历史
- 支持时间序列分析
- 可能产生重复数据（需配合 Upsert）

### 2. Overwrite 模式

覆盖模式，**每次全量替换**：

```yaml
lancedb:
  mode: overwrite
```

**特点：**
- 数据始终最新
- 丢失历史数据
- 仅适用于全量同步

---

## 使用示例

### Python API - Upsert

```python
from pixels_lance.storage import LanceDBStore

store = LanceDBStore(db_path="s3://my-bucket/lancedb")

# 准备数据
records = [
    {"custID": 1, "name": "Alice", "age": 30},
    {"custID": 2, "name": "Bob", "age": 25},
]

# Upsert 操作（基于主键）
store.upsert(
    records,
    table_name="customer",
    pk=["custID"]  # 主键字段
)
```

### 批量 Upsert

```python
# 大批量数据（自动分批处理）
large_records = [...]  # 10000 条记录

store.upsert(
    large_records,
    table_name="customer",
    pk=["custID"],
    batch_size=1000  # 每批 1000 条
)
```

---

## 查询数据

### 使用 Lance SDK

```python
import lance

# 打开表
dataset = lance.dataset("s3://my-bucket/lancedb/customer.lance")

# 基础查询
df = dataset.to_table(limit=100).to_pandas()

# 过滤查询
df_filtered = dataset.to_table(
    filter="age > 30 AND name LIKE 'A%'",
    limit=100
).to_pandas()

# 排序
df_sorted = dataset.to_table(
    order_by="age DESC",
    limit=10
).to_pandas()
```

### 使用测试脚本

修改 `tests/test_query_customer.py` 中的 `QUERY_TABLE`：

```python
# 查询 customer 表
QUERY_TABLE = "customer"

# 查询 transfer 表
# QUERY_TABLE = "transfer"
```

运行：
```bash
python tests/test_query_customer.py
```

---

## 数据版本管理

LanceDB 支持表版本：

```python
import lance

dataset = lance.dataset("s3://my-bucket/lancedb/customer.lance")

# 查看当前版本
print(f"当前版本: {dataset.version}")

# 回退到历史版本
old_dataset = lance.dataset(
    "s3://my-bucket/lancedb/customer.lance",
    version=5  # 回退到版本 5
)
```

---

## 注意事项

### 1. 主键必须唯一

确保 Schema 定义的主键在数据中唯一：

```yaml
primary_key: [custID]  # custID 必须唯一
```

### 2. Upsert 性能

- 小批量频繁 Upsert：适合实时更新
- 大批量 Upsert：适合批量同步

**建议：** 使用 `batch_size` 控制批量大小（默认 1000）

### 3. S3 延迟

S3 操作有网络延迟，建议：
- 使用批量操作
- 启用代理加速（如需要）
- 选择就近 Region

---

## 相关文档

- [S3 配置](S3_SETUP.md) - 配置对象存储
- [快速开始](QUICKSTART.md) - 基础使用教程
- [并行拉取](PARALLEL_FETCH.md) - 多表并行优化
