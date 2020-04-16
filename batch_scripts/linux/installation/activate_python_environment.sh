#!/bin/bash

# This will activate our Python environment which has been created for tcapy
export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"

if [ -d "$SCRIPT_FOLDER/installation/" ]; then
    source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh
elif [ -d "$SCRIPT_FOLDER/" ]; then
    source $SCRIPT_FOLDER/set_tcapy_env_vars.sh
fi

echo 'Activating Python environment' $TCAPY_PYTHON_ENV '... and adding tcapy to PYTHONPATH' $TCAPY_CUEMACRO
export PYTHONPATH=$TCAPY_CUEMACRO/:$PYTHONPATH

if [ $TCAPY_PYTHON_ENV_TYPE == "conda" ]; then
    echo 'Python env type' $TCAPY_PYTHON_ENV_TYPE 'and' $CONDA_ACTIVATE
    source $CONDA_ACTIVATE
    source activate $TCAPY_PYTHON_ENV
elif [ $TCAPY_PYTHON_ENV_TYPE == "virtualenv" ]; then
    echo 'Python env type ' $TCAPY_PYTHON_ENV_TYPE
    source $TCAPY_PYTHON_ENV/bin/activate
fi