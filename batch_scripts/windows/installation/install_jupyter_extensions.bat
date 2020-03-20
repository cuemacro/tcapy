REM Installs all the Python packages needed for tcapy
set SCRIPT_FOLDER=%~dp0
call %SCRIPT_FOLDER%\set_tcapy_env_vars

REM Set Python environment
call %SCRIPT_FOLDER%\activate_python_environment

echo 'Installing Jupyter extensions...'

REM Jupyter and Jupyterlab extensions
jupyter contrib nbextension install --user # to activate js on Jupyter
jupyter nbextension enable execute_time/ExecuteTime
jupyter-nbextension install rise --py --sys-prefix
jupyter nbextension enable rise --py --sys-prefix
jupyter nbextension enable toc2/main --sys-prefix

REM jupyter labextension install @jupyter-widgets/jupyterlab-manager --no-build
REM jupyter labextension install plotlywidget --no-build
REM jupyter labextension install jupyterlab-plotly --no-build
REM jupyter lab build