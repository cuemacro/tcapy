#!/bin/bash

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

# Celery log
tail -f $TCAPY_CUEMACRO/log/mongo.log ---disable-inotify