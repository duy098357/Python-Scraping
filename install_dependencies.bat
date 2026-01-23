@echo off
REM Install Python dependencies

echo Installing required packages...
pip install -r requirements.txt

if %errorlevel% neq 0 (
    echo Installation failed. Please check your internet connection.
    pause
    exit /b 1
)

echo.
echo Installation complete!
echo You can now run: python product_updater.py
pause
