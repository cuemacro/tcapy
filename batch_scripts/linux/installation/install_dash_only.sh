#!/bin/bash

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# Set Python environment
source $SCRIPT_FOLDER/activate_python_environment.sh

pip install chartpy==0.1.8 dash-auth==1.3.2 cufflinks==0.17.3 plotly==4.9.0 \
        chart-studio==1.1.0 kaleido dash-bootstrap-components==0.10.3 \
        dash==1.12.0 dash-html-components==1.0.3 dash-core-components==1.10.0 dash-table==4.7.0 jupyter-dash==0.2.1


