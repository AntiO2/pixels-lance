# Pixels Lance - 快速开始指南

## 项目介绍

**Pixels Lance** 是一个 Python 框架，用于从 RPC 端点拉取二进制数据，根据可配置的模式解析它，然后存储到 LanceDB。该项目受到 Apache Flink 的更改日志反序列化模式的启发，**现已增强支持多种复杂数据类型**。

## 核心特性

✨ **多数据类型支持**
- 整数：int8/16/32/64, uint8/16/32/64
- 浮点：float32, float64 (Big-Endian IEEE 754)
- 字符串：varchar, char, string (UTF-8)
- 二进制：bytes, binary, varbinary
- 日期/时间：date, time, timestamp, timestamp_with_tz
- 特殊：decimal (精度/小数位), boolean

⚙️ **灵活的配置**
- 分离的配置文件（config.yaml、schema_*.yaml）
- 环境变量支持
- **每个基准一个 schema 配置**（如 schema_customer.yaml、schema_company.yaml 等）

🚀 **开发友好**
- 简洁的 API
- 自动偏移计算
- 详细的错误处理
- JSON 结构化日志

## 项目结构

```
pixels-lance/
├── config/
│   ├── config.yaml                  # 主配置（RPC、LanceDB、批处理）
│   ├── schema_customer.yaml         # ✨ Customer 表模式（114 字节）
│   ├── schema_company.yaml          # ✨ Company 表模式（209 字节）
│   ├── schema_savingAccount.yaml    # ✨ 储蓄账户（32 字节）
│   ├── schema_checkingAccount.yaml  # ✨ 支票账户（32 字节）
│   ├── schema_transfer.yaml         # ✨ 转账记录（46 字节）
│   ├── schema_checking.yaml         # ✨ 检验（42 字节）
│   ├── schema_loanapps.yaml         # ✨ 贷款申请（44 字节）
│   ├── schema_loantrans.yaml        # ✨ 贷款交易（60 字节）
│   └── .env.example                 # 环境变量模板
├── src/pixels_lance/
│   ├── parser.py                    # ✨ 增强的二进制解析器（支持多种类型）
│   ├── fetcher.py                   # RPC 数据拉取器
│   ├── storage.py                   # LanceDB 存储
│   ├── config.py                    # 配置管理
│   ├── logger.py                    # 日志配置
│   └── cli.py                       # 命令行接口
├── tests/
│   └── test_parser.py               # ✨ 增强的单元测试
└── examples.py                      # ✨ 多个 benchmark 使用示例
```

## 快速开始

### 1. 验证项目

```bash
cd /path/to/pixels-lance
python test_setup.py
```

**输出：**
```
✓ Core modules loaded successfully!
✓ Configuration loaded successfully!
✓ Project structure is ready!
```

### 2. 安装依赖

```bash
pip install -e ".[dev]"
```

### 3. 运行单元测试

```bash
pytest tests/ -v
```

## 支持的数据类型和字节序

### 整数类型（小端字节序）

| 类型 | 字节 | 范围 |
|------|------|------|
| int8 | 1 | -128 到 127 |
| int32 | 4 | -2,147,483,648 到 2,147,483,647 |
| int64 | 8 | ±9.22×10¹⁸ |
| uint32 | 4 | 0 到 4,294,967,295 |

### 浮点类型（Big-Endian IEEE 754）

| 类型 | 字节 | 精度 | 示例 |
|------|------|------|------|
| float32 | 4 | 6-7 位 | 3.14 |
| float64 | 8 | 15-17 位 | 3.14159265359 |

### 日期/时间类型

| 类型 | 字节 | 存储格式 | Python 输出 |
|------|------|---------|-----------|
| date | 4 | 自 1970-01-01 的天数 | `datetime.date` |
| time | 4 | 一天内的毫秒数 | `datetime.time` |
| timestamp | 8 | 纪元毫秒 | `datetime.datetime` |

## 预定义的 Benchmark Schemas

项目包含 8 个预定义的 schema（对应 SQL 表）：

| Schema 文件 | 表名 | 字节数 | 字段数 | 描述 |
|-------------|------|--------|--------|------|
| schema_customer.yaml | customer | 114 | 16 | 客户信息（ID、姓名、余额、时间戳等）|
| schema_company.yaml | company | 209 | 15 | 公司信息（ID、名称、类别、员工数等）|
| schema_savingAccount.yaml | savingAccount | 32 | 6 | 储蓄账户（ID、余额、时间戳等）|
| schema_checkingAccount.yaml | checkingAccount | 32 | 6 | 支票账户（ID、余额、时间戳等）|
| schema_transfer.yaml | transfer | 46 | 7 | 转账记录（ID、金额、时间戳等）|
| schema_checking.yaml | checking | 42 | 7 | 检验记录（ID、金额、时间戳等）|
| schema_loanapps.yaml | loanapps | 44 | 7 | 贷款申请（ID、金额、期限等）|
| schema_loantrans.yaml | loantrans | 60 | 10 | 贷款交易（ID、金额、状态等）|

## 使用示例

### 方式 A：加载预定义 Schema

```python
from pixels_lance import DataParser

# 加载 customer 表的 schema
parser = DataParser(schema_path="config/schema_customer.yaml")

# 解析 114 字节的二进制数据
binary_data = b'...'  # 实际的 114 字节数据
result = parser.parse(binary_data)

# 访问字段
print(f"Customer ID: {result['custID']}")          # int32
print(f"Name: {result['name']}")                   # str
print(f"Loan Balance: {result['loan_balance']}")   # float
print(f"Created: {result['created_date']}")        # date 对象
print(f"Updated: {result['last_update_timestamp']}")  # datetime 对象
```

### 方式 B：批量处理

```python
from pixels_lance import DataParser, LanceDBStore

parser = DataParser(schema_path="config/schema_transfer.yaml")
store = LanceDBStore(config_path="config/config.yaml")
store.create_table(table_name="transfer")

# 解析多条记录
binary_data_list = [b'...', b'...', b'...']
records = parser.parse_batch(binary_data_list)

# 存储到 LanceDB
store.save(records, table_name="transfer")
```

### 方式 C：命令行

```bash
# 加载特定的 benchmark
pixels-lance --config config/config.yaml --schema config/schema_customer.yaml

# 使用调试日志
pixels-lance --log-level DEBUG --schema config/schema_company.yaml
```

## Schema 定义详解

每个 schema 文件描述一个 SQL 表的二进制布局：

```yaml
# config/schema_customer.yaml
table_name: customer

fields:
  # 主键
  - name: custID
    type: int32
    size: 4
    offset: 0
    nullable: false
    description: "Customer ID"

  # 字符串字段
  - name: name
    type: varchar
    size: 15
    offset: 14
    charset: utf-8
    nullable: true
    description: "Customer name"

  # 浮点字段（Big-Endian）
  - name: loan_balance
    type: float32
    size: 4
    offset: 74
    nullable: true
    description: "Loan balance"

  # 日期字段
  - name: created_date
    type: date
    size: 4
    offset: 94
    nullable: true
    description: "Creation date"

  # 时间戳字段
  - name: last_update_timestamp
    type: timestamp
    size: 8
    offset: 98
    precision: 3
    nullable: true
    description: "Last update timestamp"

record_size: 114
metadata:
  version: "1.0"
  encoding: "big-endian"
  description: "Binary schema for customer table"
```

## 添加新 Benchmark

### 步骤 1：分析 SQL 定义

```sql
CREATE TABLE my_benchmark (
    id int,
    name varchar(50),
    balance real,
    created_date date
)
```

### 步骤 2：计算偏移

```
id (int32):           offset=0,  size=4
name (varchar[50]):   offset=4,  size=50
balance (float32):    offset=54, size=4
created_date (date):  offset=58, size=4
总大小：62 字节
```

### 步骤 3：创建 schema

```yaml
# config/schema_my_benchmark.yaml
table_name: my_benchmark
fields:
  - name: id
    type: int32
    offset: 0
    size: 4
  - name: name
    type: varchar
    offset: 4
    size: 50
    charset: utf-8
  - name: balance
    type: float32
    offset: 54
    size: 4
  - name: created_date
    type: date
    offset: 58
    size: 4

record_size: 62
```

### 步骤 4：使用

```python
parser = DataParser(schema_path="config/schema_my_benchmark.yaml")
result = parser.parse(binary_data)
```

## 常见数据类型映射（SQL → Schema）

| SQL 类型 | Schema 类型 | 字节 | 备注 |
|---------|-----------|------|------|
| INT | int32 | 4 | 小端 |
| BIGINT | int64 | 8 | 小端 |
| REAL | float32 | 4 | Big-Endian IEEE 754 |
| DOUBLE | float64 | 8 | Big-Endian IEEE 754 |
| VARCHAR(N) | varchar | N | UTF-8 编码 |
| CHAR(N) | char | N | 固定长度字符串 |
| DATE | date | 4 | 自 1970-01-01 的天数 |
| TIMESTAMP | timestamp | 8 | 纪元毫秒（precision: 3） |

## 调试

### 启用调试日志

```bash
# 使用 CLI
pixels-lance --log-level DEBUG --schema config/schema_customer.yaml

# 或编辑 config.yaml
log_level: DEBUG
```

### 常见问题

**问题：某字段解析为 None**
- ✓ 检查字段偏移是否正确
- ✓ 验证二进制数据大小
- ✓ 检查 nullable 设置

**问题：数值显示错误**
- ✓ 确认字节序（小端 vs Big-Endian）
- ✓ 浮点必须用 float32/float64
- ✓ 检查大小是否与类型匹配

**问题：字符串乱码**
- ✓ 验证 charset 设置（通常 utf-8）
- ✓ 检查是否 null 结尾或正确填充
支持多种数据类型，自动计算字段偏移：
- 数值类型：uint8/16/32/64、int*、float*
- 复杂类型：bytes、string

```yaml
fields:
  - name: timestamp
    type: uint64
    # offset 自动计算
  - name: address
    type: bytes
    size: 20
```

### 3. LanceDB存储集成 💾
- 自动表创建和管理
- 支持append和overwrite模式
- 查询接口

### 4. 易于使用的API 🎯
```python
from pixels_lance import RpcFetcher, DataParser, LanceDBStore

# 初始化
fetcher = RpcFetcher(config_path="config/config.yaml")
parser = DataParser(schema_path="config/schema.yaml")
store = LanceDBStore(config_path="config/config.yaml")

# 使用
data = fetcher.fetch("your_rpc_method", {"params": "..."})
parsed = parser.parse(data)
store.save(parsed)
```

## 开始使用

### 第一步：安装依赖
```bash
pip install -e ".[dev]"
```

### 第二步：配置
```bash
# 复制环境变量模板
cp config/.env.example config/.env

# 编辑 .env 文件填入实际值
nano config/.env
```

### 第三步：定义数据模式
编辑 `config/schema.yaml` 定义你的二进制数据结构。

### 第四步：运行
```bash
# 命令行方式
pixels-lance --config config/config.yaml

# 或编程方式（参考 examples.py）
python examples.py
```

## 项目验证
```bash
python test_setup.py
```

## 测试
```bash
pytest tests/ -v
```

## 关键设计原则

1. **配置分离** - 配置、模式和代码分开管理
2. **易于扩展** - 所有主要类都可以继承和定制
3. **最小依赖** - 只依赖必要的库（requests, pydantic, lancedb, pyyaml）
4. **结构化日志** - JSON格式日志便于分析

## 文件说明

- **config.py**: 使用Pydantic验证配置，支持环境变量替换
- **fetcher.py**: RPC JSON-RPC 2.0 协议实现
- **parser.py**: 灵活的二进制数据解析，自动偏移计算
- **storage.py**: LanceDB数据持久化
- **logger.py**: 结构化JSON日志
- **cli.py**: 命令行接口入口

## 扩展指南

### 添加新的数据类型
编辑 `parser.py` 的 `_parse_field()` 方法

### 自定义RPC实现
继承 `RpcFetcher` 类，重写 `fetch()` 方法

### 自定义存储后端
继承 `LanceDBStore` 类，实现 `save()` 和 `query()` 方法

## 依赖

- **requests**: HTTP请求
- **pydantic**: 配置验证
- **pyyaml**: YAML解析
- **python-dotenv**: .env文件支持
- **lancedb**: 向量数据库存储

## 项目已包含的内容

✅ 完整的项目结构
✅ 配置管理系统
✅ 二进制数据解析器
✅ LanceDB集成
✅ 命令行接口
✅ 单元测试框架
✅ 使用示例
✅ AI代理指导说明 (.github/copilot-instructions.md)
✅ 项目验证脚本

## 下一步

1. 安装依赖：`pip install -e ".[dev]"`
2. 编辑配置文件
3. 自定义数据模式
4. 运行项目：`pixels-lance --config config/config.yaml`
5. 查看 `examples.py` 了解更多用法

祝使用愉快！🚀
