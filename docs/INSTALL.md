# 安装指南

本文档提供 Pixels Lance 的详细安装步骤。

---

## 系统要求

- **Python**: 3.8+ 
- **pip**: 20.0+
- **操作系统**: Linux / macOS / Windows

---

## 方式一：自动安装（推荐）

### Linux / macOS

```bash
git clone https://github.com/AntiO2/pixels-lance.git
cd pixels-lance
./scripts/install.sh
source .venv/bin/activate
```

### Windows

```cmd
git clone https://github.com/AntiO2/pixels-lance.git
cd pixels-lance
install.bat
```

---

## 方式二：手动安装

### 1. 创建虚拟环境

```bash
# Linux / macOS
python3 -m venv .venv
source .venv/bin/activate

# Windows
python -m venv .venv
.venv\Scripts\activate
```

### 2. 安装依赖

```bash
pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-dev.txt  # 开发依赖（可选）
```

### 3. 安装项目

```bash
pip install -e .
```

---

## 验证安装

```bash
# 检查 CLI
pixels-lance --help

# 运行测试
pytest tests/ -v

# 检查 Python 模块
python -c "from pixels_lance import DataParser, LanceDBStore; print('OK')"
```

**预期输出：**
```
OK
```

---

## 重新生成 gRPC 代码（可选）

如果修改了 `proto/sink.proto`：

```bash
python -m grpc_tools.protoc -I proto \
  --python_out=src/pixels_lance/proto \
  --pyi_out=src/pixels_lance/proto \
  --grpc_python_out=src/pixels_lance/proto \
  proto/sink.proto
```

---

## 常见问题

### Q: 提示 "No module named 'dotenv'"
```bash
pip install python-dotenv
```

### Q: gRPC 相关错误
```bash
pip install grpcio grpcio-tools protobuf
```

### Q: LanceDB 相关错误
```bash
pip install pylance pyarrow
```

---

## 下一步

- [快速开始](QUICKSTART.md) - 学习核心概念
- [S3 配置](S3_SETUP.md) - 配置云存储
