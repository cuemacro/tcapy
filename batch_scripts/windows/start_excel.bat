REM This file will start Python in the right environment and then start Excel
set SCRIPT_FOLDER=%~dp0
call %SCRIPT_FOLDER%\installation\set_tcapy_env_vars

REM Set Python environment
call %SCRIPT_FOLDER%\installation\activate_python_environment

cd %SCRIPT_FOLDER%

REM Open Excel with the tcapy_xl.xlsm spreadsheet
call %EXCEL_PATH% ..\..\..\tcapy\excel\tcapy_xl.xlsm /x
