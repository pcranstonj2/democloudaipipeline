@echo off
setlocal
pushd "%~dp0"

if not exist send-transactions.exe (
    echo send-transactions.exe not found. Building...
    go build -o send-transactions.exe .
    if errorlevel 1 (
        set EXITCODE=%ERRORLEVEL%
        popd
        exit /b %EXITCODE%
    )
)

send-transactions.exe -uri http://localhost:8080/transaction -input-file ..\data\test-transactions.txt -delay-ms 0 -quiet -progress-every 500 %*
set EXITCODE=%ERRORLEVEL%

popd
exit /b %EXITCODE%
