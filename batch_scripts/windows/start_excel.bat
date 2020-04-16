REM This file will start Python in the right environment and then start Excel
set SCRIPT_FOLDER=%~dp0
call %SCRIPT_FOLDER%\installation\set_tcapy_env_vars

REM Set Python environment
call %SCRIPT_FOLDER%\installation\activate_python_environment

call %EXCEL_PATH% /x
