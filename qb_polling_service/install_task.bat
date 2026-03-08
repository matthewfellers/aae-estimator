@echo off
REM ============================================================
REM AAE ERP — Install QB Polling Service as Scheduled Task
REM ============================================================
REM Creates a Windows Task Scheduler task that runs the poller
REM at 2:00 AM daily. Edit the time below if needed.
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
echo Script: %SCRIPT_PATH%
echo Schedule: Daily at 2:00 AM
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

REM Create the scheduled task
echo Creating scheduled task...
schtasks /create ^
    /tn "%TASK_NAME%" ^
    /tr "\"%PYTHON_PATH%\" \"%SCRIPT_PATH%\"" ^
    /sc daily ^
    /st 02:00 ^
    /ru SYSTEM ^
    /rl HIGHEST ^
    /f

if %errorLevel% equ 0 (
    echo.
    echo SUCCESS! Task "%TASK_NAME%" created.
    echo.
    echo The poller will run daily at 2:00 AM.
    echo.
    echo To run manually:  python "%SCRIPT_PATH%"
    echo To run full sync: python "%SCRIPT_PATH%" --full
    echo To test QB conn:  python "%SCRIPT_PATH%" --test
    echo.
    echo To view task: schtasks /query /tn "%TASK_NAME%" /v
    echo To remove:    schtasks /delete /tn "%TASK_NAME%" /f
) else (
    echo.
    echo FAILED to create scheduled task. Check permissions.
)

echo.
pause
