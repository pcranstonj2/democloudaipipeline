@echo off
setlocal
pushd "%~dp0"

go build -o send-transactions.exe .
set EXITCODE=%ERRORLEVEL%

popd
exit /b %EXITCODE%
