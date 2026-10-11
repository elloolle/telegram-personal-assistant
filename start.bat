@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
py -3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 'Python 3.10+ required')"
if errorlevel 1 goto fail
if not exist ".venv\Scripts\python.exe" py -3 -m venv .venv
if errorlevel 1 goto fail
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 goto fail
".venv\Scripts\python.exe" -m assistant setup %*
if errorlevel 1 goto fail
".venv\Scripts\python.exe" -m assistant run
if errorlevel 1 goto fail
exit /b 0
:fail
echo.
echo Не удалось запустить помощника. Причина указана выше.
pause
exit /b 1
