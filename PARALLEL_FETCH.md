# 并行拉取 HyBench 所有表

本项目提供两种方式来并行拉取 HyBench 的所有 8 个表。

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
# 默认：存储到 LanceDB，使用 4 个并发工作进程
python3 fetch_all_tables.py

# 打印到控制台
python3 fetch_all_tables.py --output-mode print

# 自定义 bucket ID
python3 fetch_all_tables.py --bucket-id 1

# 自定义工作进程数
python3 fetch_all_tables.py --workers 8

# 只拉取特定表
python3 fetch_all_tables.py --tables customer company savingAccount

# 组合使用
python3 fetch_all_tables.py --output-mode store --bucket-id 0 --workers 2 --tables customer company
```

### 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--output-mode` | store | 输出模式：`print` 打印到屏幕，`store` 存储到 LanceDB |
| `--bucket-id` | 0 | RPC 请求的 bucket ID |
| `--workers` | 4 | 最大并发工作进程数 |
| `--schema` | pixels_bench | Schema 名称 |
| `--tables` | 所有 8 个 | 要拉取的表列表 |

### 输出示例

```
======================================================================
Parallel HyBench Table Fetch
Schema: pixels_bench
Output Mode: store
Bucket ID: 0
Tables: 8
Max Workers: 4
Project Root: /home/antio2/projects/pixels-lance
======================================================================
[customer            ] ✓ Success (Successfully stored 1000 records)
[company             ] ✓ Success (Successfully stored 500 records)
[savingAccount       ] ✓ Success (Successfully stored 2000 records)
...
======================================================================
Results Summary
Total Time: 45.3s
Successful: 8/8
Failed: 0/8
======================================================================
```

## 方式 2：Bash 脚本

### 使用方式

```bash
# 默认：存储到 LanceDB，bucket ID 为 0
./fetch_all_tables.sh

# 打印到控制台
./fetch_all_tables.sh print

# 自定义 bucket ID
./fetch_all_tables.sh store 1

# 打印到屏幕，bucket ID 为 2
./fetch_all_tables.sh print 2
```

### 参数说明

| 位置 | 默认值 | 说明 |
|------|--------|------|
| 第 1 个 | store | 输出模式：`print` 或 `store` |
| 第 2 个 | 0 | Bucket ID |

## 并发配置

### 工作进程数选择

- **4 个进程（默认）**：适合大多数情况，平衡并发和资源使用
- **2 个进程**：用于资源有限的环境或低负载测试
- **8+ 个进程**：用于高并发场景，需要充足的网络和 I/O 资源

### 性能特征

典型的拉取时间（单个表，store 模式）：
- customer: 10-15s
- company: 5-8s
- savingAccount: 15-20s
- checkingAccount: 8-12s
- transfer: 3-5s
- checking: 3-5s
- loanapps: 3-5s
- loantrans: 5-8s

**总时间（顺序）**：约 60-80 秒
**总时间（4 并发）**：约 20-30 秒（取决于服务器性能）

## 常见用途

### 1. 完整数据导入到 LanceDB

```bash
# 清空并重新导入所有表
python3 fetch_all_tables.py --output-mode store --workers 4
```

### 2. 测试所有表的数据格式

```bash
# 先打印输出验证格式是否正确
python3 fetch_all_tables.py --output-mode print --workers 2
```

### 3. 增量更新特定表

```bash
# 只更新部分表
python3 fetch_all_tables.py --tables customer company transfer --workers 2
```

### 4. 不同 bucket 的数据拉取

```bash
# 拉取 bucket 0 的所有表
python3 fetch_all_tables.py --bucket-id 0

# 拉取 bucket 1 的所有表
python3 fetch_all_tables.py --bucket-id 1
```

## 故障排除

### 某个表拉取失败

检查日志输出，可能的原因：
- RPC 连接超时（增加 `--workers` 减少并发）
- 表名拼写错误
- Schema 配置不正确

### 性能不理想

- 增加 `--workers` 数量
- 检查网络连接质量
- 监控服务器资源使用情况

### 内存占用过高

- 减少 `--workers` 数量（从 4 降至 2）
- 使用 `--tables` 参数分批拉取

## 实现细节

### Python 脚本优势

- ✓ 跨平台（Linux、macOS、Windows）
- ✓ 灵活的参数配置
- ✓ 更好的错误处理和日志输出
- ✓ 支持部分表的选择性拉取
- ✓ 自动提取记录计数信息

### Bash 脚本优势

- ✓ 轻量级，没有 Python 依赖
- ✓ 易于与其他 shell 脚本集成
- ✓ 后台任务管理简单

## 配置文件

脚本使用以下配置文件：
- `config/config.yaml` - RPC 和 LanceDB 配置
- `config/schema_hybench.yaml` - 所有表的 Schema 定义
- `.env` - 敏感信息（如 RPC URL）

## 相关命令

### 查看单个表的数据（带格式化输出）

```bash
PYTHONPATH=src python3 src/pixels_lance/cli.py \
    --schema pixels_bench \
    --table customer \
    --bucket-id 0 \
    --output print
```

### 查看 LanceDB 中的数据

```bash
python3 -c "
import lancedb
db = lancedb.connect('lancedb')
print(db['customer'].search().limit(5).to_pandas())
"
```

### 统计各表的记录数

```bash
python3 -c "
import lancedb
db = lancedb.connect('lancedb')
for table_name in db.table_names():
    count = len(db[table_name])
    print(f'{table_name}: {count} records')
"
```
