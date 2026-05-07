@echo off
title Medicare KPI Dashboard
echo ============================================
echo   Medicare KPI Dashboard - RevWorks
echo ============================================
echo.

set PYTHON="C:\Program Files\Python311\python.exe"
set MEDICARE_LIB_PATH=%LOCALAPPDATA%\medicare_dash_lib

echo Checking Python...
%PYTHON% --version 2>nul
if %errorlevel% neq 0 (
    echo ERROR: Python not found at expected path.
    echo Please install Python 3.x or update PYTHON variable in this script.
    pause & exit /b 1
)

echo.
echo Setting up Flask library at %MEDICARE_LIB_PATH%...
if not exist "%MEDICARE_LIB_PATH%" mkdir "%MEDICARE_LIB_PATH%"
%PYTHON% -m pip install flask python-dotenv --target "%MEDICARE_LIB_PATH%" --upgrade --quiet --disable-pip-version-check 2>nul
echo Flask OK.

echo.
echo ============================================
echo   Opening browser: http://localhost:5000
echo   Press Ctrl+C to stop
echo ============================================
echo.
echo NOTE: First launch reads Excel files (~5 min).
echo       After that, cached data loads instantly.
echo.

cd /d "%~dp0"
start "" "http://localhost:5000"
%PYTHON% dashboard\app.py

pause
