#!/bin/bash

# Starts a Jupyter notebook, so tcapy can be accessed through that

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

source $SCRIPT_FOLDER/installation/activate_python_environment.sh

# Only allow local access
jupyter notebook \
  --notebook-dir=$TCAPY_CUEMACRO/tcapy_notebooks --ip=* --port=9999

# --ip=$(hostname -i)

# Alternatively have a key to access
# Create your own pem and key by following https://support.microfocus.com/kb/doc.php?id=7013103
# jupyter notebook \
# --certfile='mycert.pem' \
# --keyfile='mykey.key' --ip=* --server_port=9999