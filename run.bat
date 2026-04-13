@echo off
setlocal
pushd "%~dp0"

echo Starting Docker dispatch scheduler...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\docker_localserver.ps1" -Action start
if errorlevel 1 goto :error

popd
endlocal
exit /b 0

:error
echo Docker startup failed. Please make sure Docker Desktop is running.
popd
endlocal
pause
exit /b 1
