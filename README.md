# Pixels Lance

从RPC拉取二进制数据，解析后存储到LanceDB的Python框架。支持HTTP-JSON RPC和Pixels PixelsPollingService（gRPC）。

## 快速开始

### 安装

```bash
# 克隆项目
git clone https://github.com/AntiO2/pixels-lance.git
cd pixels-lance

# 安装依赖
pip install -r requirements.txt

# 开发模式（包含测试工具和gRPC编译器）
pip install -r requirements-dev.txt
```

### 基本使用

#### HTTP-JSON RPC
```python
from pixels_lance import RpcFetcher, DataParser, LanceDBStore

fetcher = RpcFetcher(config_path="config/config.yaml")
parser = DataParser(schema_path="config/schema_hybench.yaml", table_name="customer")
store = LanceDBStore(config_path="config/config.yaml")

# 拉取、解析、存储
for data in fetcher.fetch_batch([...]):
    parsed = parser.parse_batch(data)
    store.upsert(parsed, table_name="customer", pk=["custID"])
```

#### gRPC (PixelsPollingService)
```python
from pixels_lance.grpc_fetcher import PixelsGrpcFetcher, RowRecordBinaryExtractor
from pixels_lance.parser import DataParser
from pixels_lance.storage import LanceDBStore

fetcher = PixelsGrpcFetcher(host="localhost", port=6688)
fetcher.connect()

# Poll events from Pixels
row_records = fetcher.poll_events(schema_name="tpch", table_name="customer")

# Extract binary and parse
binary_data = RowRecordBinaryExtractor.extract_records_binary(row_records)
parser = DataParser(schema_path="config/schema_hybench.yaml", table_name="customer")
parsed = parser.parse_batch(binary_data)

# Store with upsert
store = LanceDBStore(db_path="./lancedb")
store.upsert(parsed, table_name="customer", pk=["custID"])
```

## 项目结构

```
pixels-lance/
├── src/pixels_lance/          # 源代码
│   ├── __init__.py
│   ├── cli.py                 # 命令行接口
│   ├── fetcher.py             # RPC数据拉取器 (HTTP-JSON & binary extraction)
│   ├── grpc_fetcher.py        # gRPC客户端 (PixelsPollingService)
│   ├── parser.py              # 二进制数据解析器
│   ├── storage.py             # LanceDB存储器 (带merge_insert/upsert)
│   ├── config.py              # 配置管理 (支持gRPC配置)
│   ├── logger.py              # 日志配置
│   └── proto/                 # 生成的gRPC代码
│       ├── sink_pb2.py        # Protobuf消息定义
│       ├── sink_pb2.pyi       # 类型存根
│       └── sink_pb2_grpc.py   # gRPC服务代码
├── proto/
│   └── sink.proto             # Pixels PixelsPollingService 定义
├── config/
│   ├── config.yaml            # 主配置文件 (支持gRPC配置)
│   ├── schema_hybench.yaml    # HyBench基准测试的多表模式
│   └── .env.example           # 环境变量示例
├── tests/                     # 测试目录
├── examples.py                # HTTP-JSON RPC 示例
├── examples_grpc.py           # gRPC 示例
├── pyproject.toml             # 项目配置
└── README.md                  # 本文件
```

## 配置说明

### config/config.yaml
主配置文件，包含RPC连接（HTTP-JSON或gRPC）、LanceDB设置等。

**gRPC配置示例：**
```yaml
rpc:
  use_grpc: true
  grpc_host: localhost
  grpc_port: 6688
  timeout: 30
```

### config/schema_hybench.yaml
定义多个表的二进制数据结构，包括字段名、类型、偏移量和主键。

支持的数据类型：
- `int`, `bigint`, `varchar(N)`, `char(N)`
- `real` (float32), `double` (float64)
- `timestamp`, `date`
- `boolean`

### config/.env
敏感信息（API密钥、gRPC主机等）应存储在.env文件中。

## 开发指南

### 运行测试

```bash
pytest tests/
pytest tests/ -v --cov=src
```

### 重新生成gRPC代码

```bash
python3 -m grpc_tools.protoc -I proto --python_out=src/pixels_lance/proto --pyi_out=src/pixels_lance/proto --grpc_python_out=src/pixels_lance/proto proto/sink.proto
```

### 代码格式化

```bash
black src/ tests/ *.py
flake8 src/ tests/
```

### 类型检查

```bash
mypy src/
```

## 主要模块

- **RpcFetcher**: 连接HTTP-JSON RPC节点，拉取二进制数据；支持gRPC二进制提取
- **PixelsGrpcFetcher**: 连接Pixels PixelsPollingService (gRPC)，获取RowRecord消息
- **RowRecordBinaryExtractor**: 从protobuf RowRecord消息中提取二进制数据
- **DataParser**: 根据schema解析二进制数据，支持多种数据类型
- **LanceDBStore**: 将解析的数据存储到LanceDB，支持merge_insert (upsert)
- **ConfigManager**: 统一管理配置，支持环境变量替换

## gRPC支持

项目集成了 [Pixels PixelsPollingService](https://github.com/AntiO2/pixels/blob/master/proto/sink.proto) 的gRPC定义。

**主要gRPC消息：**
- `PollRequest`: 轮询请求 (schema_name, table_name, buckets)
- `RowRecord`: 行记录 (before/after values, source info, operation type)
- `OperationType`: INSERT, UPDATE, DELETE, SNAPSHOT

使用示例见 `examples_grpc.py`。## 扩展指南

1. **自定义解析器**: 继承 `DataParser` 类并实现自己的 `parse()` 方法
2. **自定义数据源**: 继承 `RpcFetcher` 类并实现自己的 `fetch()` 方法
3. **自定义存储后端**: 继承 `LanceDBStore` 类

## License

MIT
