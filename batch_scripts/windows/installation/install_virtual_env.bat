REM This file with create a virtual environment for Python3 which can be specifically used with tcapy, so it does not
REM impact other Python applications on the server

set SCRIPT_FOLDER=%~dp0
call %SCRIPT_FOLDER%\set_tcapy_env_vars

if %TCAPY_PYTHON_ENV_TYPE%==virtualenv (
    echo 'Creating Python3 virtualenv...'
    call pip install virtualenv
    call virtualenv -p python %TCAPY_PYTHON_ENV%

    call %TCAPY_PYTHON_ENV%\Scripts\activate
)

if %TCAPY_PYTHON_ENV_TYPE%==conda (
    echo 'Creating Python3 conda...'
    call %CONDA_ACTIVATE%

    REM can be quite slow to update conda (also latest versions can have issues!)
    REM call conda update -n base conda
    call conda remove --name %TCAPY_PYTHON_ENV% --all --yes

    REM try an older version of conda - https://github.com/conda/conda/issues/9004
    call conda create -n %TCAPY_PYTHON_ENV% python=3.6 --yes

    call activate %TCAPY_PYTHON_ENV%
)