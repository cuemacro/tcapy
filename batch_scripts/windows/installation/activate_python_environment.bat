REM This will activate our Python environment which has been created for tcapy

set SCRIPT_FOLDER=%~dp0
call %SCRIPT_FOLDER%\set_tcapy_env_vars

echo Activating Python environment %TCAPY_PYTHON_ENV% ... and adding tcapy to PYTHONPATH %TCAPY_CUEMACRO%
set PYTHONPATH=%TCAPY_CUEMACRO%:%PYTHONPATH%

REM if it's a conda environment start that
if %TCAPY_PYTHON_ENV_TYPE%==conda (
    call %CONDA_ACTIVATE%
    call activate %TCAPY_PYTHON_ENV%
)

REM otherwise start the virtualenv
if %TCAPY_PYTHON_ENV_TYPE%==virtualenv (
    call %TCAPY_PYTHON_ENV%\Scripts\activate
)