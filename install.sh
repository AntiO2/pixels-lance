#!/usr/bin/env bash
# install.sh - Convenience installer for Pixels Lance
#
# This script sets up a Python virtual environment, installs the required
# dependencies, and generates gRPC Python code from the bundled proto file.
# It is intended for use on Unix-like systems (Linux, macOS).  The script is
# idempotent and can be rerun to refresh the environment.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$REPO_ROOT/.venv"
PYTHON=${PYTHON:-python3}

echo "[*] Pixels Lance installer"

# check python availability
if ! command -v $PYTHON >/dev/null 2>&1; then
    echo "Error: $PYTHON not found. Please install Python 3.8+." >&2
    exit 1
fi

# create virtual environment if not present
if [ ! -d "$VENV_DIR" ]; then
    echo "[*] Creating virtual environment at $VENV_DIR"
    $PYTHON -m venv "$VENV_DIR"
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
else
    echo "[!] grpc_tools.protoc not available, skipping proto compilation" >&2
fi

echo "[*] Installing pixels-lance in editable mode"
pip install -e "$REPO_ROOT"

echo "[*] Installation complete. Activate the environment with:"
echo "    source \"$VENV_DIR/bin/activate\""

echo "[*] You can now run the CLI using 'pixels-lance' or import the package in Python."
