# Pixels Lance 安装指南

本指南提供了在不同操作系统上安装和配置 Pixels Lance 的详细说明。

## 系统要求

- **Python**: 3.8 或更高版本
- **pip**: 20.0+（通常随 Python 安装）
- **git**: 用于克隆仓库（可选，也可下载 zip 文件）

## 快速开始

### 方式一：使用自动化安装脚本（推荐）

#### Linux / macOS

```bash
# 克隆仓库
git clone https://github.com/AntiO2/pixels-lance.git
cd pixels-lance

# 运行安装脚本
./install.sh

# 激活虚拟环境
source .venv/bin/activate
```

#### Windows

```bash
# 克隆仓库
git clone https://github.com/AntiO2/pixels-lance.git
cd pixels-lance

# 运行安装脚本
install.bat

# 虚拟环境会自动激活
```

### 方式二：手动安装

如果自动化脚本无法工作，请按以下步骤手动安装：

#### 1. 创建虚拟环境

```bash
# Linux / macOS
python3 -m venv .venv
source .venv/bin/activate

# Windows
python -m venv .venv
.venv\Scripts\activate
```

#### 2. 升级 pip

```bash
pip install --upgrade pip
```

#### 3. 安装依赖

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

#### 4. 生成 gRPC 代码（如果需要）

```bash
# Linux / macOS
python -m grpc_tools.protoc -I proto \
  --python_out=src/pixels_lance/proto \
  --pyi_out=src/pixels_lance/proto \
  --grpc_python_out=src/pixels_lance/proto \
  proto/sink.proto

# Windows
python -m grpc_tools.protoc -I proto ^
  --python_out=src\pixels_lance\proto ^
  --pyi_out=src\pixels_lance\proto ^
  --grpc_python_out=src\pixels_lance\proto ^
  proto\sink.proto
```

#### 5. 安装项目

```bash
pip install -e .
```

## 验证安装

### 检查 CLI 可用性

```bash
pixels-lance --help
```

应该看到帮助信息。

### 测试 Python 导入

```bash
python -c "from pixels_lance import DataParser, PixelsGrpcFetcher; print('✓ Import successful')"
```

### 运行测试

```bash
pytest tests/ -v
```

## 配置设置

### 1. 环境变量（可选）

创建 `.env` 文件在项目根目录：

```bash
cp config/.env.example .env
```

编辑 `.env` 配置 gRPC 服务器地址：

```bash
GRPC_HOST=your-pixels-server.com
```

### 2. gRPC 服务器配置

编辑 `config/config.yaml`：

```yaml
rpc:
  use_grpc: true
  grpc_host: localhost  # 改为您的服务器地址
  grpc_port: 6688
```

### 3. LanceDB 配置

修改 `config/config.yaml` 中的 LanceDB 路径：

```yaml
lancedb:
  db_path: /path/to/lancedb  # 改为实际路径
```

## 故障排除

### 问题：Python 版本过低

**错误**: `python: command not found` 或版本低于 3.8

**解决**:
- Linux/macOS: `python3 --version` 检查版本，如果低于 3.8 需要升级
- Windows: 从 [python.org](https://www.python.org) 下载安装 Python 3.8+

### 问题：pip 版本太旧

**错误**: `ERROR: Could not find a version that satisfies the requirement`

**解决**: 升级 pip
```bash
pip install --upgrade pip
```

### 问题：gRPC 编译失败

**错误**: `error: command 'gcc' not found` 或类似编译错误

**解决**: 安装编译工具
- Linux (Ubuntu/Debian): `sudo apt-get install build-essential python3-dev`
- Linux (RHEL/CentOS): `sudo yum install gcc python3-devel`
- macOS: 安装 Xcode Command Line Tools: `xcode-select --install`
- Windows: 安装 [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

### 问题：LanceDB 安装失败

**错误**: `lancedb` 安装超时或网络错误

**解决**: 使用不同的 pip 源
```bash
pip install -i https://pypi.tsinghua.edu.cn/simple -r requirements.txt
```

## 升级项目

如果已有旧版本安装，可以升级到最新版本：

```bash
git pull origin master
pip install -e . --upgrade
```

## 卸载

完全卸载 Pixels Lance：

```bash
# 删除虚拟环境
rm -rf .venv  # Linux/macOS
rmdir /s .venv  # Windows

# 或者
deactivate  # 退出虚拟环境
```

## 开发者设置

如果要参与开发，建议安装额外的工具：

```bash
pip install -r requirements-dev.txt
pip install black flake8 mypy pytest pytest-cov
```

然后设置 pre-commit hooks：

```bash
pip install pre-commit
pre-commit install
```

## 获取帮助

- 遇到问题？检查 [README.md](README.md) 的使用说明
- 查看 [examples_grpc.py](examples_grpc.py) 学习 API 使用
- 提交 Issue：https://github.com/AntiO2/pixels-lance/issues

## 许可证

Pixels Lance 采用 MIT 许可证。详见 [LICENSE](LICENSE) 文件。
