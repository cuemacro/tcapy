#!/bin/bash

# Install MySQL 8 on Red Hat

# Based on instructions from
# https://dev.mysql.com/doc/refman/8.0/en/linux-installation-yum-repo.html

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

if [ $DISTRO == "ubuntu" ]; then
    # mkdir -p /tmp
    # cd /tmp
    # sudo curl -OL https://dev.mysql.com/get/mysql-apt-config_0.8.13-1_all.deb
    # yes | sudo dpkg -i mysql-apt-config*
    # sudo rm -f mysql-apt-config*
    # sudo apt update
    # sudo apt install --yes mysql-server
    sudo apt-get install --yes mysql-server
elif [ $DISTRO == "redhat"  ]; then
    # update repo file (assumes MySQL8)
    sudo cp $SCRIPT_FOLDER/mysql80.repo /etc/yum.repos.d/mysql80.repo

    # sudo yum-config-manager --disable mysql57-community
    # sudo yum-config-manager --enable mysql80-community

    # sudo yum module disable mysql

    sudo yum install --yes mysql-community-server
fi

sudo service mysql start

# Will set various parameters like passwords
sudo mysql_secure_installation

