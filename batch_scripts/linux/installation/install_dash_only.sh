#!/bin/bash

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# Set Python environment
source $SCRIPT_FOLDER/activate_python_environment.sh

pip install chartpy==0.1.5 dash-auth==1.3.2 cufflinks==0.17.3 plotly_express==0.4.1 \
        dash==1.11.0 dash-html-components==1.0.3 dash-core-components==1.9.1 plotly==4.6.0 dash-table==4.6.2
