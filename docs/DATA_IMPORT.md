# 数据导入指南

本文档说明如何使用 Pixels Lance 的统一数据导入工具 `import_data.py` 将各种格式的测试数据导入到 LanceDB。

## 概述

`import_data.py` 是一个通用的数据导入工具，支持以下数据格式：

| 格式 | 说明 | 文件结构 | 分隔符 |
|------|------|---------|--------|
| **TBL** | TPC-CH 标准格式 | 单目录，多个 `*.tbl` 文件 | 竖线 (`\|`) |
| **单个 CSV** | Hybench 标准格式 | 单目录，多个 `*.csv` 文件 | 逗号 (`,`) |
| **分片 CSV** | 大规模数据集 | 多个子目录，每个表一个，各包含多个 `*_part_*.csv` | 逗号 (`,`) |

工具会**自动检测**数据格式，并使用 `schema.yaml` 中定义的类型信息进行严格的类型转换。

## 快速开始

### 基本命令

```bash
# 导入所有表
python3 scripts/import_data.py --schema <schema_file> --data <data_directory>

# 导入特定表
python3 scripts/import_data.py --schema <schema_file> --data <data_directory> --table <table_name>
```

### 常见用例

#### 1. TPC-CH 基准测试 (.tbl 文件)

```bash
# 导入 TPC-CH 所有表
python3 scripts/import_data.py \
  --schema config/schema_chbenchmark.yaml \
  --data /home/ubuntu/disk2/ch1

# 导入特定表
python3 scripts/import_data.py \
  --schema config/schema_chbenchmark.yaml \
  --data /home/ubuntu/disk2/ch1 \
  --table customer

# 导入WH10000
python3 scripts/import_data.py \
  --schema config/schema_chbenchmark.yaml \
  --data /home/ubuntu/disk5/ch10k_pixels \
  --workers 16
```

**输出示例：**
```
2026-03-06 07:18:00 - INFO - Detected format: tbl
2026-03-06 07:18:00 - INFO - Importing warehouse from WAREHOUSE.tbl (pipe-delimited)...
2026-03-06 07:18:00 - INFO - Parsed 1 records, storing to LanceDB...
2026-03-06 07:18:01 - INFO - ✓ Imported 1 records for 'warehouse'
...
============================================================
Import Summary
============================================================
  chbenchmark            :     1 records
  customer               :   100 records
  district               :     2 records
  ...
============================================================
Total imported: 1000000 records
============================================================
```

#### 2. Hybench 基准测试（单个 CSV 文件）

```bash
# 导入所有表
python3 scripts/import_data.py \
  --schema config/schema_hybench.yaml \
  --data ~/disk1/Data_10x

# 导入特定表
python3 scripts/import_data.py \
  --schema config/schema_hybench.yaml \
  --data ~/disk1/Data_10x \
  --table customer
```

**支持的表：**
- company
- customer
- savingAccount
- checkingAccount
- transfer
- checking
- loanApps
- loanTrans

#### 3. 分片 CSV 文件（大规模数据）

```bash
# 导入所有表（使用 4 个并行工作线程）
python3 scripts/import_data.py \
  --schema config/schema_hybench.yaml \
  --data ~/disk2/Data_pixels_100x \
  --workers 4

# 导入特定表（使用 8 个并行工作线程）
python3 scripts/import_data.py \
  --schema config/schema_hybench.yaml \
  --data ~/disk2/Data_pixels_100x \
  --table customer \
  --workers 8
```

**数据结构示例：**
```
Data_pixels_100x/
├── customer/
│   ├── customer_part_00001.csv
│   ├── customer_part_00002.csv
│   └── ...
├── company/
│   ├── company_part_00001.csv
│   └── ...
└── transfer/
    └── ...
```

## 功能特性

### 自动格式检测

工具会自动识别数据格式：

1. **TBL 格式**: 目录中存在 `*.tbl` 文件
2. **单个 CSV**: 目录中存在 `*.csv` 文件，但没有子目录
3. **分片 CSV**: 目录中存在子目录，每个子目录包含 `*.csv` 文件

### 严格类型检查

所有字段按照 `schema.yaml` 中的定义进行解析：

```yaml
fields:
  - name: o_carrier_id
    type: int32          # 类型定义
    nullable: true       # 可空标记
```

即使第一批数据全是 NULL，后续批次的值也会正确解析为 `int32` 类型。

### 并行处理

对于分片 CSV 数据，使用 `ThreadPoolExecutor` 并行读取分片：

```bash
# 使用 8 个线程并行读取
python3 scripts/import_data.py --data ... --workers 8
```

### 主键支持

所有导入的表都自动配置**Lance Unenforced Primary Key** 元数据，定义在 `schema.yaml` 的 `primary_key` 字段中：

```yaml
# 示例：region 表
warehouse:
  table_name: warehouse
  primary_key: w_id      # 单字段主键
  fields:
    - name: w_id
      type: int32
      nullable: false    # 主键字段必须非空
```

**主键元数据特性：**

1. **Unenforced**: Lance 不强制唯一性约束，但记录逻辑行标识
2. **Merge-Insert 支持**: 用于 `merge_insert()` 操作的去重
3. **复合主键**: 支持多字段主键（按定义顺序）

**示例 - Merge-Insert 去重：**

```python
import lancedb
db = lancedb.connect("lancedb/hybench")
table = db.open_table("warehouse")

# 导入更新数据
new_data = [
    {"w_id": 1, "w_name": "UPDATED_NAME", ...},  # 更新现有
    {"w_id": 99, "w_name": "NEW_WAREHOUSE", ...} # 新增
]

# 按主键去重合并
table.merge_insert("w_id")\
    .when_matched_update_all()\
    .when_not_matched_insert_all()\
    .execute(new_data)
```

**主键约束验证：**

导入时验证主键字段满足 Lance 要求：
- ✓ 主键字段必须非空（`nullable: false`）
- ✓ 主键字段必须是基本类型（int/string/float 等）
- ✓ 主键字段不能在复杂类型内（list/map）

若 schema 中主键字段标记为可空，导入器会自动修正并发出警告。

### 智能去重

导入前会自动去除主键重复的记录，使用最后出现的版本：

```python
# 若在同一批次中有重复，自动保留最后的
records = [
    {"id": 1, "name": "Alice", ...},
    {"id": 1, "name": "Bob", ...},     # 删除，因为id重复
]
# 结果：仅保留 Bob 的记录
```

### 错误处理

- **解析错误**: 单个字段解析失败时记录警告，使用 NULL 值继续
- **存储错误**: 失败会重试最多 3 次（指数退避）
- **部分导入**: 某个表导入失败不影响其他表

**错误日志示例：**
```
2026-03-06 07:27:46 - WARNING - Failed to parse timestamp value '2026-01-19': ...
2026-03-06 07:31:32 - ERROR - Error storing customer in LanceDB: ...
```

## 参数说明

```
选项:
  --schema SCHEMA_FILE    必需，schema YAML 文件路径
  --data DATA_DIR         必需，数据源目录路径
  --table TABLE_NAME      可选，仅导入指定表（不指定则导入所有）
  --workers N             可选，分片数据的并行工作线程数（默认: 4）
```

## 支持的数据类型

导入器支持以下数据类型的自动转换：

| 类型 | 解析方式 | 示例 |
|------|---------|------|
| `int8/16/32/64` | 整数解析 | `"123"` → 123 |
| `uint8/16/32/64` | 无符号整数解析 | `"255"` → 255 |
| `float32/64` | 浮点数解析 | `"3.14"` → 3.14 |
| `boolean` | 0/1 转换 | `"1"` → true |
| `date` | 多格式日期解析 | `"2026-03-06"` → date(2026, 3, 6) |
| `timestamp` | 多格式时间戳解析 | `"2026-03-06 07:30:00.123"` → datetime(...) |
| `varchar/char/string` | 字符串 | `"Alice"` → "Alice" |
| `binary/bytes/varbinary` | 十六进制字符串 | `"48656C6C6F"` → "48656C6C6F" |

## 日期/时间格式支持

导入器支持多种日期和时间戳格式：

```python
# Date 字段
"2026-03-06"              ✓
"2026-03-06 12:30:45"     ✓
"2026-03-06 12:30:45.123" ✓

# Timestamp 字段
"2026-03-06"              ✓
"2026-03-06 12:30:45"     ✓
"2026-03-06 12:30:45.123" ✓
```

## Schema 配置

导入器根据 `schema.yaml` 进行类型转换。确保 schema 定义正确：

### TPC-CH Schema 示例

```yaml
# config/schema_chbenchmark.yaml
tables:
  warehouse:
    table_name: warehouse
    primary_key: w_id
    fields:
      - name: w_id
        type: int32
        nullable: false
      - name: w_name
        type: varchar
        size: 10
        nullable: false
```

### Hybench Schema 示例

```yaml
# config/schema_hybench.yaml
schemas:
  - table_name: customer
    pk: [custID]
    fields:
      - name: custID
        type: int32
        nullable: false
      - name: name
        type: varchar
        nullable: true
```

## 性能优化

### 对于大型数据集

1. **增加并行工作线程**：
   ```bash
   python3 import_data.py --data ... --workers 8
   ```

2. **分表导入**：
   ```bash
   # 只导入大表
   python3 import_data.py --data ... --table customer
   python3 import_data.py --data ... --table transfer
   ```

3. **监控进度**：
   导入器每处理 10,000 条记录输出一次日志

### 内存使用

- 每批（各个分片或单个表）的数据完全加载到内存
- 对于 100GB 的分片数据，每个分片通常 100-200MB
- 总内存占用 ≈ 最大单个分片大小 × 工作线程数

## 故障排除

### 问题：找不到表

```
ERROR - Table directory not found for 'customer'
```

**解决方案**：
- 检查表名拼写（区分大小写敏感性）
- 检查数据目录中是否存在相应文件或子目录

### 问题：模式不匹配

```
ERROR - No schema found for table 'customer'
```

**解决方案**：
- 检查 `schema.yaml` 中是否定义了该表
- 确保表名在 schema 中正确定义

### 问题：类型解析失败

```
WARNING - Failed to parse int32 value '12.5': ...
```

**解决方案**：
- 检查数据中的格式是否与 schema 匹配
- 考虑在 schema 中将字段标记为 `nullable: true`

## 工作流示例

### 完整的 TPC-CH 导入工作流

```bash
#!/bin/bash

# 1. 验证数据
ls -lh /home/ubuntu/disk2/ch1/ | head

# 2. 导入所有表
cd /home/ubuntu/projects/pixels-lance
source .venv/bin/activate

python3 scripts/import_data.py \
  --schema config/schema_chbenchmark.yaml \
  --data /home/ubuntu/disk2/ch1 \
  2>&1 | tee import_tpch.log

# 3. 检查结果
tail -20 import_tpch.log

# 4. 查询验证
python3 -c "
from src.pixels_lance.storage import LanceDBStore
from src.pixels_lance.config import ConfigManager

store = LanceDBStore()
# 查询 warehouse 表
result = store.query('warehouse', limit=5)
print(result)
"
```

## 常见问题 (FAQ)

**Q: 支持多少条记录？**  
A: 理论上无限制，受限于磁盘空间和内存。实际测试支持数十亿条记录。

**Q: 可以追加导入吗？**  
A: 是的，使用主键进行 merge_insert，自动更新已存在的记录。

**Q: 支持 UTF-8 以外的编码吗？**  
A: 目前仅支持 UTF-8，可修改 `import_data.py` 中的 `encoding='utf-8'` 来支持其他编码。

**Q: 如何跳过某些表？**  
A: 在 `schema.yaml` 中注释掉不需要的表定义。

**Q: 导入失败可以重试吗？**  
A: 可以，直接重新运行相同命令，导入器会检测现有数据并进行 merge_insert。

## 相关文档

- [Schema 定义指南](SCHEMA.md)
- [LanceDB 存储配置](../STORAGE.md)
- [CDC 数据处理](../PARALLEL_FETCH.md)
