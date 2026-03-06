# Pixels Lance

从 Pixels RPC 拉取二进制数据,解析并存储到 Lance 的框架

**Pixels Lance** 是一个用于实时数据同步的 Python 工具，通过 gRPC 从 Pixels 拉取变更数据（CDC），解析复杂的二进制格式，并存储到 LanceDB 列式数据库中。支持本地存储和 AWS S3。

---

## 核心特性

- **多协议支持** - 支持 gRPC (PixelsPollingService) 和 HTTP-JSON RPC
- **智能存储** - 基于主键的 Upsert 操作,自动合并更新
- **云原生** - 支持本地文件系统和 AWS S3/MinIO 对象存储
- **丰富类型** - 支持 20+ 种数据类型(int/float/varchar/timestamp/decimal等)
- **高性能** - 批量处理、可配置并发、自动重试
- **易配置** - YAML 配置 + 环境变量,一键切换代理和存储

---

## 快速开始

### 1. 安装

```bash
git clone https://github.com/AntiO2/pixels-lance.git
cd pixels-lance

# 安装依赖（推荐使用虚拟环境）
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. 配置

复制环境变量模板并填写：
```bash
cp config/.env.example config/.env
# 编辑 config/.env 填写 AWS 凭证（如使用 S3）
```

### 3. 运行

```bash
# 拉取 customer 表数据并存储到 LanceDB
pixels-lance --schema tpch --table customer

# 查询已存储的数据
python tests/test_query_customer.py
```

---

## 文档导航

| 文档 | 说明 |
|------|------|
| [安装指南](docs/INSTALL.md) | 详细安装步骤（自动/手动、多平台） |
| [快速开始](docs/QUICKSTART.md) | 核心概念、数据类型、使用示例 |
| [S3 存储配置](docs/S3_SETUP.md) | AWS S3 和 MinIO 配置教程 |
| [存储机制](docs/STORAGE.md) | Upsert、主键、存储模式说明 |
| [并行拉取](docs/PARALLEL_FETCH.md) | 多表并行拉取和性能优化 |

---

## 使用示例

### CLI 命令行

```bash
# 基础用法:拉取并存储(必须指定 bucket-id)
pixels-lance --schema tpch --table customer --bucket-id 0

# 指定多个 bucket IDs
pixels-lance --schema tpch --table customer --bucket-id 0 --bucket-id 1 --bucket-id 2

# 仅解析和打印（不存储）
pixels-lance --schema tpch --table customer --bucket-id 0 --dry-run
```

### Python API

```python
from pixels_lance.grpc_fetcher import PixelsGrpcFetcher, RowRecordBinaryExtractor
from pixels_lance.parser import DataParser
from pixels_lance.storage import LanceDBStore

# 1. 连接 gRPC 服务
fetcher = PixelsGrpcFetcher(host="localhost", port=6688)
fetcher.connect()

# 2. 拉取数据
row_records = fetcher.poll_events(schema_name="tpch", table_name="customer")
binary_data = RowRecordBinaryExtractor.extract_records_binary(row_records)

# 3. 解析二进制
parser = DataParser(schema_path="config/schema_hybench.yaml", table_name="customer")
parsed_data = parser.parse_batch(binary_data)

# 4. 存储到 LanceDB（自动 upsert）
store = LanceDBStore()
store.upsert(parsed_data, table_name="customer", pk=["custID"])
```

---

## 项目架构

```
pixels-lance/
├── config/                    # 配置文件
│   ├── config.yaml           # 主配置（RPC/LanceDB/代理）
│   ├── schema_hybench.yaml   # 表结构定义（字段类型、偏移量、主键）
│   └── .env                  # 环境变量（AWS 凭证等）
├── src/pixels_lance/         # 核心代码
│   ├── grpc_fetcher.py       # gRPC 客户端
│   ├── parser.py             # 二进制解析器
│   ├── storage.py            # LanceDB 存储（Upsert）
│   └── proto/                # gRPC 协议定义
├── docs/                     # 详细文档
└── tests/                    # 测试和示例
```

---

## 核心配置

### 主配置文件 `config/config.yaml`

```yaml
rpc:
  use_grpc: true               # 使用 gRPC（默认）
  grpc_host: localhost
  grpc_port: 6688

lancedb:
  db_path: s3://my-bucket/lancedb   # 本地: ./lancedb
  mode: append                       # 或 overwrite
  storage_options:                   # S3 配置
    region: ${AWS_REGION}
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
  proxy: ${HTTP_PROXY:-}             # 可选代理
```

### Schema 定义 `config/schema_hybench.yaml`

定义每个表的二进制结构（字段名、类型、偏移量、主键）：

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
    # ... 更多字段
  primary_key: [custID]
  record_size: 114
```

**支持的类型**: int8/16/32/64, float32/64, varchar, timestamp, date, boolean, decimal 等

**注意**: 使用 CLI 时必须通过 `--bucket-id` 参数指定至少一个 bucket ID。

---

## 开发和测试

```bash
# 运行测试
pytest tests/ -v --cov=src

# 代码格式化
black src/ tests/

# 类型检查
mypy src/

# 重新生成 gRPC 代码
python -m grpc_tools.protoc -I proto \
  --python_out=src/pixels_lance/proto \
  --grpc_python_out=src/pixels_lance/proto \
  proto/sink.proto
```

---

## 贡献

欢迎提交 Issue 和 Pull Request！

---

## License

MIT License - 详见 [LICENSE](LICENSE) 文件
