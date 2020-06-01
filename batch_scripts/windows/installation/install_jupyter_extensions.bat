REM Installs the Jupyter extensions which might be useful if you want to use Jupyter as a front end for tcapy
set SCRIPT_FOLDER=%~dp0
call %SCRIPT_FOLDER%\set_tcapy_env_vars

REM Set Python environment
call %SCRIPT_FOLDER%\activate_python_environment

echo 'Installing Jupyter extensions...'

REM Jupyter and Jupyterlab extensions
call jupyter contrib nbextension install --user # to activate js on Jupyter
call jupyter nbextension enable execute_time/ExecuteTime
call jupyter-nbextension install rise --py --sys-prefix
call jupyter nbextension enable rise --py --sys-prefix
call jupyter nbextension enable toc2/main --sys-prefix
call jupyter nbextension install --sys-prefix --symlink --py jupyter_dash
call jupyter nbextension enable --py jupyter_dash

call jupyter labextension install @jupyter-widgets/jupyterlab-manager --no-build
call jupyter labextension install plotlywidget --no-build
call jupyter labextension install jupyterlab-plotly --no-build
call jupyter labextension install bqplot
call jupyter lab build