#!/bin/bash

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

sudo killall influxd
sudo influxd -config $TCAPY_CUEMACRO/tcapy/conf/influxdb.conf