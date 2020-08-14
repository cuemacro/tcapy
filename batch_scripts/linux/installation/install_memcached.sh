#!/bin/bash

# Installs Memcached (in memory cache, like Redis) which is used by Celery as a results backend

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

if [ $DISTRO == "ubuntu" ]; then
    sudo apt-get install --yes memcached libmemcached-tools

elif [ $DISTRO == "redhat"  ]; then
    # Based on https://access.redhat.com/solutions/1160613
    # You need to enable the RHEL7 repo to download Memcached from
    # sudo subscription-manager repos --enable=rhel-7-server-rpms
    sudo yum install --yes memcached
fi

# By default it is started at 127.0.0.1:11211
# If you want to change the server_port and other settings edit /etc/sysconfig/memcached
# Note: it should NOT be accessible from outside