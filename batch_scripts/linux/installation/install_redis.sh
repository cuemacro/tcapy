#!/bin/bash

# Installs Redis, which is used in many places in tcapy
# - to cache market and trade data
# - manage communication between tcapy app and Celery as a message broker
# - it is recommended to install latest versions of Redis, which support features like UNLINK
# Need GCC to compile Redis - ie. wget make gcc, these will be installed by install_python_tools_apache.sh

# Reference from http://www.nixtechnix.com/how-to-install-redis-4-0-on-centos-6-and-amazon-linux/
# also see https://redis.io/topics/quickstart

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