@echo off
REM ============================================================
REM AAE ERP — Install QB Polling Service as Scheduled Task
REM ============================================================
REM Creates a Windows Task Scheduler task that runs the poller
REM in daemon mode — polls every 15 min during business hours,
REM full sync at 2 AM, and checks for "Sync Now" button presses.
REM
REM Run this script AS ADMINISTRATOR on the QB server machine.
REM ============================================================

SET TASK_NAME=AAE_QB_Poller
SET PYTHON_PATH=python
SET SCRIPT_DIR=%~dp0
SET SCRIPT_PATH=%SCRIPT_DIR%qb_poller.py
SET LOG_FILE=%SCRIPT_DIR%qb_sync.log

echo.
echo ============================================================
echo AAE ERP — QuickBooks Polling Service Installer
echo ============================================================
echo.
echo This will create a scheduled task named: %TASK_NAME%
echo Script: %SCRIPT_PATH% --daemon
echo Schedule: Runs at system startup, polls every 15 min
echo.

REM Check if running as admin
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: This script must be run as Administrator.
    echo Right-click and select "Run as administrator".
    pause
    exit /b 1
)

REM Remove existing task if it exists
schtasks /query /tn "%TASK_NAME%" >nul 2>&1
if %errorLevel% equ 0 (
    echo Removing existing task...
    schtasks /delete /tn "%TASK_NAME%" /f
)

REM Create the scheduled task — runs at startup in daemon mode
echo Creating scheduled task (daemon mode)...
schtasks /create ^
    /tn "%TASK_NAME%" ^
    /tr "\"%PYTHON_PATH%\" \"%SCRIPT_PATH%\" --daemon" ^
    /sc onstart ^
    /ru SYSTEM ^
    /rl HIGHEST ^
    /f

if %errorLevel% equ 0 (
    echo.
    echo SUCCESS! Task "%TASK_NAME%" created.
    echo.
    echo The poller will start automatically at boot and run continuously.
    echo   - Polls every 15 min during business hours (7 AM - 6 PM)
    echo   - Full sync at 2:00 AM nightly
    echo   - Checks for "Sync Now" button presses from the ERP dashboard
    echo.
    echo To run manually:  python "%SCRIPT_PATH%"
    echo To run full sync: python "%SCRIPT_PATH%" --full
    echo To run daemon:    python "%SCRIPT_PATH%" --daemon
    echo To test QB conn:  python "%SCRIPT_PATH%" --test
    echo.
    echo To view task: schtasks /query /tn "%TASK_NAME%" /v
    echo To remove:    schtasks /delete /tn "%TASK_NAME%" /f
    echo.
    echo Starting the service now...
    schtasks /run /tn "%TASK_NAME%"
) else (
    echo.
    echo FAILED to create scheduled task. Check permissions.
)

echo.
pause
