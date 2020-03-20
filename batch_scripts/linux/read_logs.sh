#!/bin/bash

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

# This file will start Gnome Terminal windows in Redhat to read all the various logs for tcapy

gnome-terminal -x sh -c 'top'

# Apache log (only for when using Apache/WSGI which is deprecated)
# gnome-terminal -x sh -c 'sudo tail -f /var/log/httpd/error_log'

# Gunicorn log
gnome-terminal -x sh -c 'sudo tail -f $TCAPY_CUEMACRO/log/linux*.log ---disable-inotify'

# Celery log
gnome-terminal -x sh -c 'tail -f $TCAPY_CUEMACRO/log/celery.log ---disable-inotify'

# MongoDB log
gnome-terminal -x sh -c 'sudo tail -f $TCAPY_CUEMACRO/log/mongo.log ---disable-inotify'
