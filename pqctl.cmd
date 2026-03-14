@echo off
cd /d "%USERPROFILE%\policyqueue"
powershell -NoProfile -ExecutionPolicy Bypass -File "%USERPROFILE%\policyqueue\pqctl.ps1" menu
echo.
echo Tip: run e.g.  pqctl.ps1 restart all
pause
