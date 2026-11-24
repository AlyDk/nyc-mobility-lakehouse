@echo off
if exist "%USERPROFILE%\Documents\Programs\PortableGit\cmd" set "PATH=%USERPROFILE%\Documents\Programs\PortableGit\cmd;%PATH%"
call "%USERPROFILE%\.venvs\nyc-mobility-lakehouse\Scripts\activate.bat"