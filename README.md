# Pixels Lance

从RPC拉取二进制数据，解析后存储到LanceDB的Python项目。

## 快速开始

### 安装

```bash
# 克隆项目
git clone <repo-url>
cd pixels-lance

# 安装依赖
pip install -e .

# 开发模式（包含测试工具）
pip install -e ".[dev]"
```

### 基本使用

```bash
# 使用配置文件运行
pixels-lance --config config/config.yaml

# 或者在Python代码中
from pixels_lance import RpcFetcher, DataParser, LanceDBStore

fetcher = RpcFetcher(config_path="config/config.yaml")
parser = DataParser(schema_path="config/schema.yaml")
store = LanceDBStore(config_path="config/config.yaml")

# 拉取、解析、存储
for data in fetcher.fetch():
    parsed = parser.parse(data)
    store.save(parsed)
```

## 项目结构

```
pixels-lance/
├── src/pixels_lance/          # 源代码
│   ├── __init__.py
│   ├── cli.py                 # 命令行接口
│   ├── fetcher.py             # RPC数据拉取器
│   ├── parser.py              # 数据解析器
│   ├── storage.py             # LanceDB存储器
│   ├── config.py              # 配置管理
│   └── logger.py              # 日志配置
├── config/
│   ├── config.yaml            # 主配置文件
│   ├── schema.yaml            # 数据模式定义
│   └── .env.example           # 环境变量示例
├── tests/                     # 测试目录
├── pyproject.toml             # 项目配置
└── README.md                  # 本文件
```

## 配置说明

### config/config.yaml
主配置文件，包含RPC连接、LanceDB设置等。

### config/schema.yaml
定义二进制数据的解析结构和LanceDB表的列定义。

### config/.env
敏感信息（API密钥、数据库路径等）应存储在.env文件中。

## 开发指南

### 运行测试

```bash
pytest tests/
pytest tests/ -v --cov=src
```

### 代码格式化

```bash
black src/ tests/
flake8 src/ tests/
```

### 类型检查

```bash
mypy src/
```

## 主要模块

- **RpcFetcher**: 连接RPC节点，拉取二进制数据
- **DataParser**: 根据schema解析二进制数据
- **LanceDBStore**: 将解析的数据存储到LanceDB
- **ConfigManager**: 统一管理配置

## 扩展指南

1. **自定义解析器**: 继承 `DataParser` 类并实现自己的 `parse()` 方法
2. **自定义数据源**: 继承 `RpcFetcher` 类并实现自己的 `fetch()` 方法
3. **自定义存储后端**: 继承 `LanceDBStore` 类

## License

MIT
