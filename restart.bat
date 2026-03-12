@echo off
echo Restarting OpenText server...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8765 "') do (
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 1 /nobreak >nul
pushd "%~dp0"
start /b py -3 -m uvicorn main:app --host 0.0.0.0 --port 8765
timeout /t 3 /nobreak >nul
echo Server restarted on port 8765
