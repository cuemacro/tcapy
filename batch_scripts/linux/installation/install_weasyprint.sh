#!/bin/bash

# This installs required dependencies for WeasyPrint

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

if [ $DISTRO == "ubuntu" ]; then
    sudo apt-get update
    sudo apt-get install --yes \
      build-essential libcairo2 libpango-1.0-0 libpangocairo-1.0-0 libgdk-pixbuf2.0-0 libffi-dev shared-mime-info

elif [ $DISTRO == "redhat"  ]; then
    # For Red Hat force it to enable subscription (once it has been paid) - otherwise can't access mod_ssl
    sudo subscription-manager attach --auto
    sudo subscription-manager repos --enable=rhel-7-server-rpms

    sudo yum update
    sudo yum install --yes \
      redhat-rpm-config libffi-devel cairo pango gdk-pixbuf2
fi
