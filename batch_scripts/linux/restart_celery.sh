#!/bin/bash

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

# kill all Celery processes
sudo killall celery

# make sure permissions set properly
sudo setenforce 0

echo 'Set Python paths...'

export PYTHONPATH=$TCAPY_CUEMACRO/:$PYTHONPATH

# Make sure permissions set properly
sudo setenforce 0

# Set Python environment
source $SCRIPT_FOLDER/installation/activate_python_environment.sh

echo 'Flush Celery cache...'

# Purge every message from the "celery" queue everything on celery (not strcitly necessary if using Redis, as flushing in next line)
# celery purge -f

# Flush redis of everything in cache (saved dataframes and message queue)
echo 'Flushing Redis cache...'

redis-cli flushall

echo 'Current working folder'
echo $PWD

echo 'About to start celery...'
celery -A tcapy.conf.celery_calls worker --purge --discard --loglevel=debug -Q celery --concurrency=$TCAPY_CELERY_WORKERS -f $TCAPY_CUEMACRO/log/celery.log &