#!/bin/bash

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# set Python environment
source $SCRIPT_FOLDER/activate_python_environment.sh

pip install chartpy IPython==7.7.0 dash-auth==1.3.2 cufflinks==0.17 plotly_express==0.4.1 dash==1.8.0 dash-html-components==1.0.2 dash-core-components==1.7.0 plotly==4.5.0 dash-table==4.6.0
