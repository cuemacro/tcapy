#!/bin/bash

# Installs Redis, which is used in many places in tcapy
# - to cache market and trade data
# - manage communication between tcapy app and Celery as a message broker
# - it is recommended to install latest versions of Redis, which support features like UNLINK
#
# Can either install from Chris Lea's repo or compile from source (downloading from Redis website), which are latest versions
# of Redis
#
# Generally, the standard Linux repo's tend to have older versions of Redis, which will work, but have fewer features
#
# Need GCC to compile Redis - ie. wget make gcc, these will be installed by install_python_tools_apache.sh

# Reference from http://www.nixtechnix.com/how-to-install-redis-4-0-on-centos-6-and-amazon-linux/
# also see https://redis.io/topics/quickstart

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

sudo adduser --system --group --no-create-home redis

if [ $DISTRO == "ubuntu" ] && [ $COMPILE_REDIS_FROM_SOURCE == 0 ]; then
    # Use Chris Lea's repo which has newer versions of Redis
    sudo add-apt-repository -y ppa:chris-lea/redis-server
    sudo apt-get update
    sudo apt -y install redis-server
else
    # Create temporary folder for redis
    cd /home/$USER/

    # Remove any previous redis temporary file installations
    rm -rf /home/$USER/redis-stable
    rm -rf /home/$USER/redis-stable.tar.gz

    # Download latest stable version and unzip
    wget -c http://download.redis.io/redis-stable.tar.gz
    tar -xvzf redis-stable.tar.gz

    # Now build redis from the source code
    cd redis-stable
    make
    make test
    sudo make install

    sudo cp -f src/redis-server /usr/bin/
    sudo cp -f src/redis-cli /usr/bin/

    cd utils

    # Finally
    # sudo ./install_server.sh
    echo -n | sudo ./install_server.sh
fi