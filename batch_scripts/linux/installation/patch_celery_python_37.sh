#!/bin/bash

# Celery 4.2 uses "async" for some script names, but in Python 3.7, "async" is a reserved keyword
# This script manually patches the Celery code changing all instances of "async" to "asynchronous"

# This should be fixed in Celery 5.0 (https://github.com/celery/celery/issues/4500) - in general it is recommended
# to stick to Python 3.6 for Celery 4.2, till Celery 5.0 is released
# Celery 4.3 supports 3.7

# For older version of Python, we do not need to run this script

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

TARGET=$TCAPY_PYTHON_ENV/site-packages/celery/backends

cd $TARGET

if [ -e async.py ]
then
    mv async.py asynchronous.py
    sed -i 's/async/asynchronous/g' redis.py
    sed -i 's/async/asynchronous/g' rpc.py
fi