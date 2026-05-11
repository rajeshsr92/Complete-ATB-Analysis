@echo off
title ATB Dashboard Server
cd /d "%~dp0dashboard"
echo Starting ATB Dashboard...
echo.
echo Open your browser at: http://localhost:5000
echo.
echo Press Ctrl+C to stop the server.
echo.
python app.py
pause
