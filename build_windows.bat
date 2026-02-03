@echo off
echo Building FRP Python for Windows...

python -m pip install pyinstaller

python -m PyInstaller --onefile --name frpc_multi --distpath dist\windows frp_python\frpc_multi.py
python -m PyInstaller --onefile --name frps_multi --distpath dist\windows frp_python\frps_multi.py

echo Build complete! Executables are in dist\windows\
dir dist\windows\
