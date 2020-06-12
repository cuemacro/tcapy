#!/bin/bash

# Installs all the Python packages needed for tcapy
export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# Set Python environment
source $SCRIPT_FOLDER/activate_python_environment.sh

echo 'Installing Jupyter extensions...'

# Jupyter extensions
jupyter contrib nbextension install --user # to activate js on Jupyter
jupyter nbextension enable execute_time/ExecuteTime
jupyter-nbextension install rise --py --sys-prefix
jupyter nbextension enable rise --py --sys-prefix
jupyter nbextension enable toc2/main --sys-prefix
jupyter nbextension install --sys-prefix --symlink --py jupyter_dash
jupyter nbextension enable --py jupyter_dash

# JupyterLab extensions optional
# jupyter labextension install @jupyter-widgets/jupyterlab-manager@2.0.0 --no-build
# jupyter labextension install plotlywidget@1.5.4 --no-build
# jupyter labextension install jupyterlab-plotly@1.5.4 --no-build
# jupyter labextension install bqplot
# jupyter lab build