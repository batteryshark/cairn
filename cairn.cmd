@echo off
setlocal
where py >nul 2>nul
if errorlevel 1 (
  echo cairn requires Python 3.11 or newer from python.org. 1>&2
  exit /b 1
)
if exist "%~dp0cairn.pyz" (
  py -3 "%~dp0cairn.pyz" %*
) else (
  set "PYTHONPATH=%~dp0src;%PYTHONPATH%"
  py -3 -m cairn %*
)
exit /b %errorlevel%
