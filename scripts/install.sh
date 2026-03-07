#!/usr/bin/env bash
# scripts/install.sh - Convenience installer for Pixels Lance

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$REPO_ROOT/.venv"
PYTHON=${PYTHON:-python3}

echo "[*] Pixels Lance installer"

# check python availability
if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "Error: $PYTHON not found. Please install Python 3.8+." >&2
    exit 1
fi

# create virtual environment if not present
if [ ! -d "$VENV_DIR" ]; then
    echo "[*] Creating virtual environment at $VENV_DIR"
    sudo apt install python3.10-venv
    "$PYTHON" -m venv "$VENV_DIR"
fi

# activate venv
# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"

echo "[*] Upgrading pip"
pip install --upgrade pip

echo "[*] Installing requirements"
pip install -r "$REPO_ROOT/requirements.txt"
pip install -r "$REPO_ROOT/requirements-dev.txt"

# compile proto if grpcio-tools available
if python -c 'import grpc_tools.protoc' 2>/dev/null; then
    echo "[*] Generating gRPC Python code from proto/sink.proto"
    python -m grpc_tools.protoc -I "$REPO_ROOT/proto" \
        --python_out="$REPO_ROOT/src/pixels_lance/proto" \
        --pyi_out="$REPO_ROOT/src/pixels_lance/proto" \
        --grpc_python_out="$REPO_ROOT/src/pixels_lance/proto" \
        "$REPO_ROOT/proto/sink.proto"

    # ensure proto package marker exists
    touch "$REPO_ROOT/src/pixels_lance/proto/__init__.py"

    # fix grpc generated import style for package context
    PROTO_GRPC_FILE="$REPO_ROOT/src/pixels_lance/proto/sink_pb2_grpc.py"
    if [ -f "$PROTO_GRPC_FILE" ] && grep -q '^import sink_pb2 as sink__pb2' "$PROTO_GRPC_FILE"; then
        echo "[*] Patching sink_pb2_grpc.py import to package-relative form"
        sed -i 's/^import sink_pb2 as sink__pb2$/from . import sink_pb2 as sink__pb2/' "$PROTO_GRPC_FILE"
    fi
else
    echo "[!] grpc_tools.protoc not available, skipping proto compilation" >&2
fi

echo "[*] Installing pixels-lance in editable mode"
pip install -e "$REPO_ROOT"

echo "[*] Installation complete. Activate the environment with:"
echo "    source \"$VENV_DIR/bin/activate\""

echo "[*] You can now run the CLI using 'pixels-lance' or import the package in Python."
