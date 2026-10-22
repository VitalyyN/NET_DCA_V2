@echo off
cd /d "%~dp0"
"%~dp0env\Scripts\python.exe" -B -X utf8 gateway_control.py --stop
pause
