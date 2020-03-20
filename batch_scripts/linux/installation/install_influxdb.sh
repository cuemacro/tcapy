#!/bin/bash

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# Installation of influxdb time series database (can be used instead of MongoDB/Arctic)
# further details at https://docs.influxdata.com/influxdb/v1.7/introduction/installation/
if [ $DISTRO == "ubuntu" ]; then
    wget -qO- https://repos.influxdata.com/influxdb.key | sudo apt-key add -
    source /etc/os-release
    echo "deb https://repos.influxdata.com/debian $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/influxdb.list
    sudo apt-get -y install influxdb

elif [ $DISTRO == "redhat"  ]; then
    # Update repo file
    sudo cp $SCRIPT_FOLDER/influxdb.repo /etc/yum.repos.d/influxdb.repo

    # Now install influxdb
    sudo yum install -y influxdb
fi

