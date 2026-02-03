@echo off
cd /d "%~dp0"
echo Building FRP Python for Windows...
echo Working directory: %CD%

python -m pip install pyinstaller

python -m PyInstaller --onefile --name frpc_multi --distpath dist\windows .\frpc_multi.py
python -m PyInstaller --onefile --name frps_multi --distpath dist\windows .\frps_multi.py

echo Build complete! Executables are in dist\windows\
dir dist\windows\
