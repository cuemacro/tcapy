#!/bin/bash

# This script installs MongoDB on your machine. This will require editing, depending on where you want to install
# MongoDB, or if have already installed MongoDB.

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

if [ $DISTRO == "ubuntu" ]; then
    # Ubuntu installation
    sudo rm /etc/apt/sources.list.d/mongodb*.list

    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 4B7C549A058F8B6B

    # For Mongo 3.6
    # echo "deb [ arch=amd64,arm64 ] http://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/3.6 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.6.list

    # For Mongo 4.2
    echo "deb [arch=amd64] http://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.2.list

    sudo apt-get update
    sudo apt-get install --yes mongodb-org mongodb-org-tools

elif [ $DISTRO == "redhat"  ]; then
    # Red Hat installation
    # Update repo file (assumes MongoDB 3.6) and then install

    # For Mongo 3.6
    sudo cp $TCAPY_CUEMACRO/batch_scripts/installation/mongodb-org-3.6.repo /etc/yum.repos.d/mongodb-org-3.6.repo

    # For Mongo 4.2
    # sudo cp $TCAPY_CUEMACRO/batch_scripts/installation/mongodb-org-4.2.repo /etc/yum.repos.d/mongodb-org-4.2.repo

    sudo yum install --yes mongodb-org
fi

# Create data folder and make MongoDB the owner
sudo mkdir -p /data/db
sudo chown -R mongodb:mongodb /data/db
sudo chmod -R a+rw /data/db

# Make sure to edit mongo.conf to your tcapy log folder location!