#!/bin/bash

# Install Google Chrome
export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# Linux installation
if [ $DISTRO == "ubuntu" ]; then
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
    sudo apt -y install ./google-chrome-stable_current_amd64.deb

elif [ $DISTRO == "redhat"  ]; then
    # Copy repo file (for Google Chrome - by default not in Red Hat distribution)
    sudo cp $TCAPY_CUEMACRO/batch_scripts/installation/google-chrome.repo /etc/yum.repos.d/google-chrome.repo

    # Now install Chrome
    sudo yum -y install google-chrome-stable
fi



