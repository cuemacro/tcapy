#!/bin/bash

# This script will install a number of different applications
# - Python development tools
# - gcc compiler
# - tofrodos useful utility for converting Windows to Linux text files
# - openssl
# - Apache web server

# Update repo
# Install EPEL repo access (required for downloading certain packages like python-pip)
# Install python-pip (to install Python packages), python-devel (needed for Python libraries) and git (source control)
# Install GCC (can be required to compile some Python packages & dependencies)
# Install converter between Windows and Linux files (converts line endings)
# Install openssl-lib (for accessing https proxy for some market data providers, like NCFX)
# Install Apache web server
# For Red Hat/Amazon Linux for a Apache module
# Install snappy compression for PyStore

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

if [ $DISTRO == "ubuntu" ]; then
    sudo apt-get update
    sudo apt-get install --yes \
      python-setuptools python-dev python-pip git \
      gcc g++ wget make \
      tofrodos \
      apache2 apache2-utils ssl-cert openssl liblasso3 libapache2-mod-wsgi \
      libsnappy-dev

elif [ $DISTRO == "redhat"  ]; then
    # For Red Hat force it to enable subscription (once it has been paid) - otherwise can't access mod_ssl
    sudo subscription-manager attach --auto
    sudo subscription-manager repos --enable=rhel-7-server-rpms

    sudo yum update
    sudo yum -y install \
      https://dl.fedoraproject.org/pub/epel/epel-release-latest-$(rpm -E '%{rhel}').noarch.rpm \
      python-setuptools python-devel python-pip git \
      gcc gcc-c++ wget make \
      tofrodos \
      openssl-libs \
      httpd httpd-tools openssl mod_auth_mellon mod_wsgi \
      mod_ssl \
      snappy-devel
fi


# for pycurl (NOTE: you might need to change "libcrypto.so.1.0.1e" to whatever version is installed on your machine)
# pycurl won't start if libcrypto.so.1.0.0 not present, so create a symbolic link
# sudo ln -sf /usr/lib64/libcrypto.so.1.0.1e /usr/lib64/libcrypto.so.1.0.0
# sudo ln -sf /usr/lib64/libssl.so.1.0.1e /usr/lib64/libssl.so.1.0.0

# enable proxy modules to be able to run gunicorn through apache
# sudo a2enmod proxy proxy_ajp proxy_http rewrite deflate headers proxy_balancer proxy_connect proxy_html