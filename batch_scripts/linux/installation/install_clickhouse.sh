#!/bin/bash

# This script installs ClickHouse on your machine. This will require editing, depending on where you want to install.

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

if [ $DISTRO == "ubuntu" ]; then
    # Ubuntu installation
    sudo apt-get install apt-transport-https ca-certificates dirmngr
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4

    echo "deb https://repo.clickhouse.tech/deb/stable/ main/" | sudo tee \
        /etc/apt/sources.list.d/clickhouse.list
    sudo apt-get update

    sudo apt-get install -y clickhouse-server clickhouse-client

elif [ $DISTRO == "redhat"  ]; then
    # Red Hat installation
    sudo yum install yum-utils
    sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
    sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/clickhouse.repo
    sudo yum install clickhouse-server clickhouse-client
fi

# By default data folder is at
# /var/lib/clickhouse/