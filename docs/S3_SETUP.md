# S3 存储配置指南

本文档介绍如何配置 AWS S3 和 MinIO 对象存储。

---

## 方式一：AWS S3

### 1. 准备 S3 Bucket

在 AWS 控制台创建 bucket，或使用 AWS CLI：

```bash
aws s3 mb s3://my-lancedb-bucket --region us-east-2
```

### 2. 配置环境变量

编辑 `config/.env`：

```bash
AWS_REGION=us-east-2
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
```

### 3. 修改配置文件

编辑 `config/config.yaml`：

```yaml
lancedb:
  db_path: s3://my-lancedb-bucket/lancedb
  storage_options:
    region: ${AWS_REGION}
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

### 4. 测试连接

```bash
# 拉取数据并存储到 S3
pixels-lance --schema tpch --table customer

# 查询 S3 中的数据
python tests/test_query_customer.py
```

### 5. 配置代理（可选）

如果需要通过代理访问 S3：

```bash
# 在 config/.env 中添加
HTTP_PROXY=http://proxy.example.com:8080
```

`config/config.yaml` 中添加：
```yaml
lancedb:
  proxy: ${HTTP_PROXY:-}
```

---

## 方式二：MinIO（本地 S3）

### 1. 启动 MinIO

#### Docker 方式

```bash
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  --name minio \
  minio/minio server /data --console-address ":9001"
```

#### 二进制方式

```bash
# 下载
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio

# 启动
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  ./minio server /data --console-address ":9001"
```

访问控制台：http://localhost:9001

### 2. 创建 Bucket

使用 MinIO Console 或 mc 客户端：

```bash
# 安装 mc
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc

# 配置别名
./mc alias set local http://localhost:9000 minioadmin minioadmin

# 创建 bucket
./mc mb local/lancedb-bucket
```

### 3. 配置环境变量

编辑 `config/.env`：

```bash
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

### 4. 修改配置文件

编辑 `config/config.yaml`：

```yaml
lancedb:
  db_path: s3://lancedb-bucket/data
  storage_options:
    region: us-east-1  # MinIO 也需要 region
    endpoint: http://localhost:9000  # ✨ MinIO 端点
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

### 5. 测试连接

```bash
pixels-lance --schema tpch --table customer
```

---

## 验证配置

### 使用诊断脚本

```bash
python tests/debug_config.py
```

**预期输出：**
```
Base path: s3://my-bucket/lancedb
Final storage_options:
  region: us-east-2
  access_key_id: AKIA6DXO...
  secret_access_key: T0LxB+u6...
✓ 配置正确
```

### 手动验证

```python
from pixels_lance.storage import LanceDBStore

store = LanceDBStore()
print(f"存储路径: {store.base_path}")
print(f"配置项: {list(store.storage_options.keys())}")
```

---

## 常见问题

### Q: 无法连接到 S3

**检查项：**
1. AWS 凭证是否正确
2. Bucket 是否存在且有权限
3. 网络是否可达（代理配置）
4. Region 是否正确

```bash
# 测试 AWS CLI 连接
aws s3 ls s3://my-bucket --region us-east-2
```

### Q: MinIO 连接超时

确保 `endpoint` 配置正确：
```yaml
storage_options:
  endpoint: http://localhost:9000  # 不要用 https
```

### Q: 代理设置不生效

检查 `.env` 文件是否被加载：
```python
import os
print(os.getenv('HTTP_PROXY'))  # 应输出代理地址
```

### Q: 禁用代理

在 `config/.env` 中注释或删除 `HTTP_PROXY`：
```bash
# HTTP_PROXY=http://proxy.example.com:8080
```

---

## 相关文档

- [存储机制](STORAGE.md) - 了解 LanceDB 存储原理
- [快速开始](QUICKSTART.md) - 基础使用教程
