@echo off
cd /d "%~dp0"
set "BOT_SCRIPT=%~n0"
set "BOT_SCRIPT=%BOT_SCRIPT:start_=%"
"%~dp0env\Scripts\python.exe" -B -X utf8 "%~dp0%BOT_SCRIPT%.py" %*
pause
