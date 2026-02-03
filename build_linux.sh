#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "Building FRP Python for Linux..."
echo "Working directory: $(pwd)"

if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed. Please install Python 3 first."
    exit 1
fi

echo "Installing PyInstaller..."
python3 -m pip install pyinstaller

echo "Building frpc_multi..."
python3 -m PyInstaller --onefile --name frpc_multi --distpath dist/linux ./frpc_multi.py

echo "Building frps_multi..."
python3 -m PyInstaller --onefile --name frps_multi --distpath dist/linux ./frps_multi.py

echo "Build complete! Executables are in dist/linux/"
ls -lh dist/linux/
