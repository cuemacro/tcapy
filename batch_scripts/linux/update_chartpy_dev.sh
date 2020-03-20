#!/bin/bash

# updates chartpy from dev instance of chartpy on disk

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

cd $TCAPY_PYTHON_ENV/lib/python2.7/site-packages/chartpy
rm -rf *

cd $TCAPY_CUEMACRO/chartpy/
python setup.py install