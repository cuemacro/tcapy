REM Creates the environment YAML file for the conda environment py36tca
set SCRIPT_FOLDER=%~dp0
call %SCRIPT_FOLDER%\set_tcapy_env_vars

REM Set Python environment
call %SCRIPT_FOLDER%\activate_python_environment

echo 'Export conda environment'

call conda update -n base conda --yes
call conda env export > %TCAPY_CUEMACRO%\batch_scripts\windows\installation\environment_windows_py37tca.yml