#!/usr/bin/env bash
# Profile fetch_all_tables.py with CPU and memory profiling

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-$REPO_ROOT/profiling}"
VENV_PYTHON="$REPO_ROOT/.venv/bin/python3"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Check venv exists
if [ ! -f "$VENV_PYTHON" ]; then
    echo "Error: Virtual environment not found at $VENV_PYTHON"
    echo "Please run: python3 -m venv $REPO_ROOT/.venv"
    exit 1
fi

PROFILE_TYPE="${1:-cpu}"  # cpu, memory, or both
shift || true

echo "========================================"
echo "Performance Profiling for fetch_all_tables.py"
echo "Profile Type: $PROFILE_TYPE"
echo "Output Dir: $OUTPUT_DIR"
echo "========================================"

case "$PROFILE_TYPE" in
    cpu)
        echo "[*] Running CPU profiling with py-spy..."
        PY_SPY="$REPO_ROOT/.venv/bin/py-spy"
        if [ ! -f "$PY_SPY" ]; then
            echo "Error: py-spy not found at $PY_SPY"
            exit 1
        fi
        $PY_SPY record \
            --format speedscope \
            --output "$OUTPUT_DIR/cpu_profile_$(date +%Y%m%d_%H%M%S).json" \
            --native \
            -- $VENV_PYTHON "$REPO_ROOT/scripts/fetch_all_tables.py" "$@"
        
        echo "[*] CPU profile saved to $OUTPUT_DIR"
        echo "    View at: https://www.speedscope.app/"
        ;;
    
    memory)
        echo "[*] Running memory profiling with memray..."
        $VENV_PYTHON -m memray run \
            --output "$OUTPUT_DIR/memory_profile_$(date +%Y%m%d_%H%M%S).bin" \
            --native \
            "$REPO_ROOT/scripts/fetch_all_tables.py" "$@"
        
        # Generate flamegraph
        LATEST_BIN=$(ls -t "$OUTPUT_DIR"/memory_profile_*.bin | head -1)
        $VENV_PYTHON -m memray flamegraph "$LATEST_BIN" \
            --output "$OUTPUT_DIR/memory_flamegraph.html"
        
        echo "[*] Memory profile saved to $OUTPUT_DIR"
        echo "    Flamegraph: $OUTPUT_DIR/memory_flamegraph.html"
        ;;
    
    both)
        echo "[*] Running both CPU and memory profiling..."
        
        PY_SPY="$REPO_ROOT/.venv/bin/py-spy"
        if [ ! -f "$PY_SPY" ]; then
            echo "Error: py-spy not found at $PY_SPY"
            exit 1
        fi
        
        # CPU profiling with py-spy and memory profiling with memray
        $PY_SPY record \
            --format speedscope \
            --output "$OUTPUT_DIR/cpu_profile_$(date +%Y%m%d_%H%M%S).json" \
            --native \
            -- $VENV_PYTHON -m memray run \
                --output "$OUTPUT_DIR/memory_profile_$(date +%Y%m%d_%H%M%S).bin" \
                --native \
                "$REPO_ROOT/scripts/fetch_all_tables.py" "$@"
        
        # Generate flamegraph
        LATEST_BIN=$(ls -t "$OUTPUT_DIR"/memory_profile_*.bin | head -1)
        $VENV_PYTHON -m memray flamegraph "$LATEST_BIN" \
            --output "$OUTPUT_DIR/memory_flamegraph.html"
        
        echo "[*] Profiles saved to $OUTPUT_DIR"
        echo "    CPU: Open .json at https://www.speedscope.app/"
        echo "    Memory: $OUTPUT_DIR/memory_flamegraph.html"
        ;;
    
    perf)
        echo "[*] Running Linux perf profiling..."
        if ! command -v perf &> /dev/null; then
            echo "Error: perf not installed. Install with: sudo apt install linux-tools-generic"
            exit 1
        fi
        
        sudo perf record \
            -F 99 \
            -g \
            --call-graph dwarf \
            -o "$OUTPUT_DIR/perf_$(date +%Y%m%d_%H%M%S).data" \
            -- $VENV_PYTHON "$REPO_ROOT/scripts/fetch_all_tables.py" "$@"
        
        echo "[*] Perf data saved to $OUTPUT_DIR"
        echo "    Generate flamegraph with: perf script | stackcollapse-perf.pl | flamegraph.pl > flame.svg"
        ;;
    
    *)
        echo "Unknown profile type: $PROFILE_TYPE"
        echo "Usage: $0 {cpu|memory|both|perf} [fetch_all_tables.py args...]"
        exit 1
        ;;
esac

echo ""
echo "Profiling complete!"
