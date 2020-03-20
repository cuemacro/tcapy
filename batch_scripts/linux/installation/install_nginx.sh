#!/bin/bash

# Install nginx web server (for tcapy it is assumed to be default web server, instead of Apache) on Red Hat
export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

if [ $DISTRO == "ubuntu" ]; then
    sudo apt-get install --yes nginx

elif [ $DISTRO == "redhat" ]; then
    # Update repo file
    sudo cp $SCRIPT_FOLDER/installation/nginx.repo /etc/yum.repos.d/nginx.repo

    # Now install nginx
    sudo yum install --yes install nginx-1.12.1
fi


