# 并行拉取 HyBench 所有表

本项目提供两种方式来并行拉取 HyBench 的所有 8 个表的多个 bucket 数据。

## 表列表

HyBench 包含以下 8 个表：
- `customer` - 客户表
- `company` - 公司表
- `savingAccount` - 储蓄账户表
- `checkingAccount` - 活期账户表
- `transfer` - 转账表
- `checking` - 检查表
- `loanapps` - 贷款申请表
- `loantrans` - 贷款交易表

## 方式 1：Python 脚本 (推荐)

### 使用方式

```bash
# 默认：存储到 LanceDB，拉取所有表的 4 个 bucket（0-3），使用 8 个并发工作进程
python3 fetch_all_tables.py

# 拉取 8 个 bucket（0-7）
python3 fetch_all_tables.py --bucket-num 8

# 打印到控制台
python3 fetch_all_tables.py --output-mode print

# 自定义 bucket 数量
python3 fetch_all_tables.py --bucket-num 2

# 自定义工作进程数（建议为 bucket_num × 2 或更高）
python3 fetch_all_tables.py --bucket-num 4 --workers 16

# 只拉取特定表的所有 bucket
python3 fetch_all_tables.py --tables customer company --bucket-num 4

# 组合使用
python3 fetch_all_tables.py --output-mode store --bucket-num 4 --workers 8 --tables customer company
```

### 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--output-mode` | store | 输出模式：`print` 打印到屏幕，`store` 存储到 LanceDB |
| `--bucket-num` | 4 | 要拉取的 bucket 数量（从 0 到 bucket-num-1）|
| `--workers` | 8 | 最大并发工作进程数 |
| `--schema` | pixels_bench | Schema 名称 |
| `--tables` | 所有 8 个 | 要拉取的表列表 |

**重要说明**：
- 总任务数 = 表数量 × bucket数量（例如：8 个表 × 4 个 bucket = 32 个并发任务）
- 每个表的每个 bucket 都会启动一个独立的拉取进程
- `--workers` 参数控制最大并发数，建议设为 `bucket_num × 2` 或更高以充分利用并发

### 输出示例

```
======================================================================
Parallel HyBench Table Fetch
Schema: pixels_bench
Output Mode: store
Buckets: 4 (0-3)
Tables: 8
Total Tasks: 32 (tables × buckets)
Max Workers: 8
Project Root: /home/antio2/projects/pixels-lance
======================================================================
[customer[B0]            ] ✓ Success (Successfully stored 250 records)
[customer[B1]            ] ✓ Success (Successfully stored 245 records)
[company[B0]             ] ✓ Success (Successfully stored 125 records)
[savingAccount[B0]       ] ✓ Success (Successfully stored 500 records)
...
======================================================================
Results Summary
Total Time: 45.3s
Successful: 32/32
Failed: 0/32
======================================================================
```

## 方式 2：Bash 脚本

### 使用方式

```bash
# 默认：存储到 LanceDB，拉取 4 个 bucket（0-3），超时 300 秒
./fetch_all_tables.sh

# 打印到控制台，拉取 4 个 bucket
./fetch_all_tables.sh print

# 自定义 bucket 数量（拉取 8 个 bucket）
./fetch_all_tables.sh store 8

# 自定义超时时间（600 秒，即 10 分钟）
./fetch_all_tables.sh store 4 600

# 打印到屏幕，拉取 2 个 bucket，超时 120 秒
./fetch_all_tables.sh print 2 120
```

### 参数说明

| 位置 | 默认值 | 说明 |
|------|--------|------|
| 第 1 个 | store | 输出模式：`print` 或 `store` |
| 第 2 个 | 4 | Bucket 数量（拉取 0 到 N-1）|
| 第 3 个 | 300 | 每个任务的超时时间（秒）|

**重要说明**：
- Bash 脚本**不限制最大并发任务数**，所有任务同时启动
- 使用 `timeout` 命令防止单个任务无限期运行
- 超时的任务将被终止并标记为失败（退出码 124）

## 并发配置

### 工作进程数选择

脚本会为**每个表的每个 bucket** 启动一个独立的拉取进程：

- **总任务数** = 表数量 × bucket 数量
  - 8 个表 × 4 个 bucket = **32 个任务**
  - 8 个表 × 8 个 bucket = **64 个任务**

- **推荐 workers 配置**：
  - 4 个 bucket：`--workers 8` 或 `--workers 16`
  - 8 个 bucket：`--workers 16` 或 `--workers 32`
  - 原则：workers >= bucket_num × 2

### 性能特征

典型的拉取时间（单个表单个 bucket，store 模式）：
- customer: 2-4s (每个 bucket)
- company: 1-2s (每个 bucket)
- savingAccount: 3-5s (每个 bucket)
- checkingAccount: 2-3s (每个 bucket)
- transfer: 1s (每个 bucket)
- checking: 1s (每个 bucket)
- loanapps: 1s (每个 bucket)
- loantrans: 1-2s (每个 bucket)

**总时间（顺序拉取 8 表 × 4 bucket）**：约 60-80 秒
**总时间（8 并发拉取 8 表 × 4 bucket）**：约 10-15 秒
**总时间（16 并发拉取 8 表 × 4 bucket）**：约 6-10 秒（推荐）

## 常见用途

### 1. 完整数据导入到 LanceDB（所有 bucket）

```bash
# 拉取所有表的 4 个 bucket（默认），使用 16 个并发
python3 fetch_all_tables.py --workers 16

# 拉取所有表的 8 个 bucket，使用 32 个并发
python3 fetch_all_tables.py --bucket-num 8 --workers 32
```

### 2. 测试所有表的数据格式

```bash
# 先打印输出验证格式是否正确（拉取 2 个 bucket 测试）
python3 fetch_all_tables.py --output-mode print --bucket-num 2 --workers 4
```

### 3. 增量更新特定表的所有 bucket

```bash
# 只更新部分表的所有 bucket
python3 fetch_all_tables.py --tables customer company --bucket-num 4 --workers 8
```

### 4. 快速测试单个 bucket

```bash
# 只测试 bucket 0（每个表）
python3 fetch_all_tables.py --bucket-num 1 --workers 8
```

## 故障排除

### 某个表的某个 bucket 拉取失败

检查日志输出，可能的原因：
- RPC 连接超时（增加 `--workers` 提高并发处理能力）
- 表名拼写错误
- Schema 配置不正确
- 特定 bucket 没有数据或服务端错误

### 性能不理想

- **增加并发数**：`--workers` 应至少为 `bucket_num × 2`
- **减少 bucket 数**：先测试少量 bucket（如 `--bucket-num 2`）
- 检查网络连接质量
- 监控服务器资源使用情况

### 内存占用过高

- 减少 `--workers` 数量（例如从 32 降至 16）
- 减少 `--bucket-num`（从 8 降至 4）
- 分批拉取：先拉取部分表，再拉取其他表

## 实现细节

### Python 脚本优势

- ✓ 跨平台（Linux、macOS、Windows）
- ✓ 灵活的参数配置
- ✓ 更好的错误处理和日志输出
- ✓ 支持部分表的选择性拉取
- ✓ 自动提取记录计数信息
- ✓ **每个表的每个 bucket 独立拉取**（真正的并行化）

### Bash 脚本优势

- ✓ 轻量级，没有 Python 依赖
- ✓ 易于与其他 shell 脚本集成
- ✓ 后台任务管理简单
- ✓ **每个表的每个 bucket 独立拉取**（真正的并行化）
- ✓ **无并发数限制**（所有任务同时启动，最大化并行度）
- ✓ **内置超时保护**（防止任务无限期运行）

### 并行化策略

两个脚本都采用**任务级并行化**：
- 任务单位：(表名, bucket_id)
- 每个任务启动独立的 CLI 进程
- **Python 脚本**：ThreadPoolExecutor 管理并发（可限制最大并发数）
- **Bash 脚本**：所有任务同时启动（无并发数限制，使用 timeout 保护）
- 示例：8 个表 × 4 个 bucket = 32 个独立任务同时执行

## 配置文件

脚本使用以下配置文件：
- `config/config.yaml` - RPC 和 LanceDB 配置
- `config/schema_hybench.yaml` - 所有表的 Schema 定义
- `.env` - 敏感信息（如 RPC URL）

## 相关命令

### 查看单个表的单个 bucket 数据（带格式化输出）

```bash
PYTHONPATH=src python3 src/pixels_lance/cli.py \
    --schema pixels_bench \
    --table customer \
    --bucket-id 0 \
    --output print
```

### 查看 LanceDB 中的数据（所有 bucket 混合）

```bash
python3 -c "
import lancedb
db = lancedb.connect('lancedb')
print(db['customer'].search().limit(5).to_pandas())
"
```

### 统计各表的总记录数（所有 bucket）

```bash
python3 -c "
import lancedb
db = lancedb.connect('lancedb')
for table_name in db.table_names():
    count = len(db[table_name])
    print(f'{table_name}: {count} records')
"
```

### 查看特定 bucket 的数据（如果记录中包含 bucket 字段）

```bash
python3 -c "
import lancedb
db = lancedb.connect('lancedb')
# 假设有 bucket_id 字段
result = db['customer'].search().where('bucket_id = 0').limit(10).to_pandas()
print(result)
"
```
