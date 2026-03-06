# Lance 对象存储配置

本项目使用 **Lance** 列式数据格式存储数据，支持本地文件系统和对象存储（S3、GCS、Azure）。

> **注意**: 本项目使用 `lance` (https://github.com/lancedb/lance) 而不是 `lancedb`。Lance 是底层的列式数据格式，直接提供对象存储支持。

## 快速配置

### 本地存储（默认）

**config/config.yaml**:
```yaml
lancedb:
  db_path: ./lancedb  # 基础路径
  table_name: pixel_data  # 默认表名（通常不使用）
  mode: append
```

**存储结构**：
```
./lancedb/
├── customer.lance          # customer 表数据
├── company.lance           # company 表数据
├── savingAccount.lance     # savingAccount 表数据
└── checkingAccount.lance   # checkingAccount 表数据
```

> **重要**: 
> - `db_path` 是基础目录路径
> - 每个表存储为独立的 `.lance` 文件：`<db_path>/<table_name>.lance`
> - CLI 通过 `--table` 参数指定实际表名，config 中的 `table_name` 仅作为默认值

### AWS S3

**config/config.yaml**:
```yaml
lancedb:
  db_path: s3://my-bucket/lancedb  # S3 URI
  table_name: pixel_data
  mode: append
  storage_options:
    region: ${AWS_REGION:-us-east-1}
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

**config/.env**:
```bash
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### MinIO (S3-compatible)

**config/config.yaml**:
```yaml
lancedb:
  db_path: s3://my-bucket/lancedb
  table_name: pixel_data
  mode: append
  storage_options:
    region: us-east-1
    endpoint: http://localhost:9000  # MinIO endpoint
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

**启动 MinIO**:
```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

### Google Cloud Storage

**config/config.yaml**:
```yaml
lancedb:
  db_path: gs://my-bucket/lancedb  # GCS URI
  table_name: pixel_data
  mode: append
  storage_options:
    service_account: ${GOOGLE_SERVICE_ACCOUNT}
```

**config/.env**:
```bash
GOOGLE_SERVICE_ACCOUNT=/path/to/service-account.json
```

### Azure Blob Storage

**config/config.yaml**:
```yaml
lancedb:
  db_path: az://my-container/lancedb  # Azure URI
  table_name: pixel_data
  mode: append
  storage_options:
    account_name: ${AZURE_STORAGE_ACCOUNT_NAME}
    account_key: ${AZURE_STORAGE_ACCOUNT_KEY}
```

**config/.env**:
```bash
AZURE_STORAGE_ACCOUNT_NAME=mystorageaccount
AZURE_STORAGE_ACCOUNT_KEY=your_account_key_here
```

## 完整配置选项

参考 `config/config.yaml` 中的注释，所有 Lance 支持的 storage_options 都可以配置。

详细文档: https://lancedb.github.io/lance/read_and_write.html#object-store-configuration

### S3 选项
- `region` / `aws_region` - AWS 区域
- `access_key_id` / `aws_access_key_id` - 访问密钥
- `secret_access_key` / `aws_secret_access_key` - 密钥
- `session_token` / `aws_session_token` - 会话令牌
- `endpoint` / `aws_endpoint` - 自定义端点（MinIO 等）
- `s3_express` / `aws_s3_express` - S3 Express One Zone
- `virtual_hosted_style_request` - 虚拟主机样式请求

### GCS 选项
- `service_account` / `google_service_account` - 服务账号 JSON 路径
- `service_account_key` / `google_service_account_key` - 序列化的服务账号密钥

### Azure 选项
- `account_name` / `azure_storage_account_name` - 账户名称
- `account_key` / `azure_storage_account_key` - 账户密钥
- `sas_token` / `azure_storage_sas_token` - 共享访问签名

### 通用选项
- `timeout` / `request_timeout` - 请求超时（默认 30s）
- `connect_timeout` - 连接超时（默认 5s）
- `allow_http` - 允许 HTTP（默认 false）

完整选项列表参考: https://lancedb.github.io/lance/read_and_write.html#object-store-configuration

## 使用示例

```python
from pixels_lance.storage import LanceDBStore

# 使用默认配置（从 config/config.yaml）
store = LanceDBStore()

# 或指定配置文件
store = LanceDBStore(config_path="config/config.yaml")

# 写入数据（存储为 Lance 格式）
data = [{"id": 1, "name": "test"}]
store.save(data, table_name="my_table")

# 查询数据
results = store.query(table_name="my_table", limit=10)
print(results)

# Upsert 操作（需要指定主键）
store.upsert(data, table_name="my_table", key="id")
```

## 故障排除

### S3 连接失败

检查：
1. S3 URI 格式正确：`s3://bucket/path`
2. AWS 凭证已设置（环境变量或 .env 文件）
3. Region 配置正确
4. 网络连接正常

### MinIO 连接失败

检查：
1. MinIO 服务已启动
2. endpoint 包含协议：`http://localhost:9000`
3. region 必须指定（即使是任意值）
4. bucket 已创建

### 权限错误

确保 AWS/GCS/Azure 账号有读写权限：
- S3: `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`
- GCS: Storage Object Admin
- Azure: Blob Data Contributor

## 性能建议

- **本地开发**: 使用本地存储（最快）
- **生产环境**: 使用对象存储（持久化、高可用）
- **高并发**: 增加 `timeout` 和 `connect_timeout`
- **大文件**: 使用 S3 Transfer Acceleration
