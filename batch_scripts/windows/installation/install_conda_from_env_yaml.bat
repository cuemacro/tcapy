REM Makes a conda environment from a premade YAML file
set SCRIPT_FOLDER=%~dp0
call %SCRIPT_FOLDER%\set_tcapy_env_vars

REM Set Python environment
call %SCRIPT_FOLDER%\activate_python_environment

echo 'Create a conda environment from YAML file...'

call conda activate
call conda remove --name %TCAPY_PYTHON_ENV% --all --yes

call conda env create -f %TCAPY_CUEMACRO%\batch_scripts\windows\installation\environment_windows_py37tca.yml
call conda activate %TCAPY_PYTHON_ENV%