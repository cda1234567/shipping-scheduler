@echo off
setlocal
pushd "%~dp0"

echo Restarting Docker dispatch scheduler...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\docker_localserver.ps1" -Action restart
if errorlevel 1 goto :error

popd
endlocal
exit /b 0

:error
echo Docker restart failed. Please make sure Docker Desktop is running.
popd
endlocal
pause
exit /b 1
