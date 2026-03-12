@echo off
setlocal

pushd %~dp0
powershell -ExecutionPolicy Bypass -File "%~dp0verify.ps1"
set ERR=%ERRORLEVEL%
popd
exit /b %ERR%
