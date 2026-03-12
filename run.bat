@echo off
pushd "%~dp0"
py -3 -m uvicorn main:app --host 0.0.0.0 --port 8765 --reload
popd
pause
