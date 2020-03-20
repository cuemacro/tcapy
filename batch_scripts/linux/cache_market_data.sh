#!/bin/bash

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

# set Python environment
source $SCRIPT_FOLDER/installation/activate_python_environment.sh

# run python scripts in tcapy
python $TCAPY_CUEMACRO/tcapy_scripts/gen/volatile_market_trade_data_gen.py