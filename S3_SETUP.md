# S3 存储配置快速指南

## 方式 1: AWS S3（推荐）

### 1. 配置环境变量

编辑 `config/.env`：
```bash
# AWS 凭证
AWS_REGION=us-east-2
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

### 2. 修改 config.yaml

```yaml
lancedb:
  db_path: s3://your-bucket-name/lancedb  # 修改为你的 bucket 名称
  table_name: pixel_data
  mode: append
  storage_options:
    region: ${AWS_REGION}
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

### 3. 运行测试

```bash
# 写入数据到 S3
PYTHONPATH=src python3 src/pixels_lance/cli.py \
  --schema pixels_bench \
  --table customer \
  --bucket-id 2 \
  --output store

# 数据将存储在: s3://your-bucket-name/lancedb/customer.lance
```

---

## 方式 2: MinIO (本地 S3)

### 1. 启动 MinIO

```bash
# 使用 Docker
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

访问控制台: http://localhost:9001

### 2. 创建 Bucket

使用 MinIO Console 或命令行：
```bash
# 使用 mc (MinIO Client)
mc alias set myminio http://localhost:9000 minioadmin minioadmin
mc mb myminio/lancedb-bucket
```

### 3. 配置环境变量

`config/.env`:
```bash
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

### 4. 修改 config.yaml

```yaml
lancedb:
  db_path: s3://lancedb-bucket/data
  table_name: pixel_data
  mode: append
  storage_options:
    region: us-east-1  # MinIO 也需要指定 region
    endpoint: http://localhost:9000  # MinIO 端点
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

### 5. 运行测试

```bash
PYTHONPATH=src python3 src/pixels_lance/cli.py \
  --schema pixels_bench \
  --table customer \
  --bucket-id 2 \
  --output store
```

---

## 验证配置

### 方法 1: 使用测试脚本

```bash
python3 -c "
from src.pixels_lance.storage import LanceDBStore
import pyarrow as pa

# 初始化存储
store = LanceDBStore()
print(f'Base path: {store.base_path}')
print(f'Storage options: {store.storage_options}')

# 写入测试数据
test_data = [{'id': 1, 'name': 'test', 'value': 100}]
store.save(test_data, table_name='test_table')
print('✓ 写入成功')

# 查询数据
results = store.query(table_name='test_table', limit=10)
print(f'✓ 查询成功，返回 {len(results)} 条记录')
print(results)
"
```

### 方法 2: 查看 S3 中的文件

**AWS S3:**
```bash
aws s3 ls s3://your-bucket-name/lancedb/ --recursive
```

**MinIO:**
```bash
mc ls myminio/lancedb-bucket/data/ --recursive
```

---

## 常见问题

### 1. 连接超时

**问题**: `Connection timeout`

**解决**:
- 检查网络连接
- 增加 `storage_options` 中的 `timeout`:
  ```yaml
  storage_options:
    timeout: 60s
    connect_timeout: 10s
  ```

### 2. 权限错误

**问题**: `Access Denied`

**解决**:
- 确认 AWS 凭证正确
- 检查 IAM 权限：需要 `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`
- MinIO: 确认用户有读写权限

### 3. Region 错误

**问题**: `Incorrect region`

**解决**:
- 确认 bucket 所在 region
- 修改 `storage_options.region` 为正确的 region

### 4. Endpoint 错误 (MinIO)

**问题**: `Connection refused`

**解决**:
- 确认 MinIO 已启动: `docker ps`
- 检查端口是否正确: `9000` (API), `9001` (Console)
- 使用 `http://localhost:9000` 而不是 `localhost:9000`

---

## 性能优化

### 1. 使用 S3 Transfer Acceleration

```yaml
storage_options:
  region: us-east-1
  # 启用传输加速（需在 S3 bucket 设置中启用）
  use_accelerate_endpoint: true
```

### 2. 调整并发数

```bash
# 增加并发工作进程
python3 fetch_all_tables.py --bucket-num 4 --workers 16
```

### 3. 批量写入

修改 `config.yaml` 中的 `batch_size`:
```yaml
batch_size: 1000  # 增加批次大小
```

---

## 从本地迁移到 S3

```bash
# 1. 备份本地数据
cp -r lancedb lancedb_backup

# 2. 修改 config.yaml 指向 S3
# db_path: s3://your-bucket/lancedb

# 3. 重新拉取数据（会自动写入 S3）
python3 fetch_all_tables.py --bucket-num 4

# 4. 验证 S3 数据
aws s3 ls s3://your-bucket/lancedb/ --recursive
```

---

## 切换回本地存储

只需修改 `config.yaml`:
```yaml
lancedb:
  db_path: ./lancedb
  storage_options: {}  # 清空或删除此行
```
