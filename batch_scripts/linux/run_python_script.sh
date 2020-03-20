#!/bin/bash

# Demonstrates how to run a Python script in the Python environment setup for tcapy (usually Anaconda)

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

# set Python environment
source $SCRIPT_FOLDER/installation/activate_python_environment.sh

# Python command (add you python command here)
python $TCAPY_CUEMACRO/tcapy/tcapy_scripts/gen/dump_ncfx_to_csv.py