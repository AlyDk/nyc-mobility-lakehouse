@echo off
if exist "%USERPROFILE%\Documents\Programs\PortableGit\cmd" set "PATH=%USERPROFILE%\Documents\Programs\PortableGit\cmd;%PATH%"
call "%USERPROFILE%\.venvs\nyc-mobility-lakehouse\Scripts\activate.bat"
if exist "%USERPROFILE%\Documents\Programs\databricks-cli" set "PATH=%USERPROFILE%\Documents\Programs\databricks-cli;%PATH%"
if exist "%USERPROFILE%\.secrets\nyc-mobility.cmd" call "%USERPROFILE%\.secrets\nyc-mobility.cmd"