#!/bin/bash

# This will kill all the databases used by tcapy and all the tcapy processes
# 1) Celery - for distributed computation
# 2) Gunicorn - to serve Python web app for tcapy
# 3) Apache - web server
# 4) Nginx - web server
# 5) MongoDB - for market tick data
# 6) MySQL - for trade/order data
# 7) Redis - key/value store for short term caching of market/trade/order data and as a message broker for Celery
# 8) Memcached - results backend for Celery

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

# Sometimes security settings can prevent MongoDB from running
sudo setenforce 0

# Kill web app
sudo killall celery
sudo killall gunicorn
sudo killall httpd
sudo killall apache2
sudo service nginx stop
sudo service httpd stop

# Note you will have to edit this if you choose to use a different database other than Arctic/MongoDB
# sudo service mongod stop
sudo rm -f /data/db/mongod.lock
sudo rm -f /tmp/mongod-27017.sock
sudo killall mongod

# Stop MySQL
sudo service mysql stop

# Kill Redis
sudo service redis-server stop
sudo killall redis-server
sudo redis-cli -p 6379 shutdown

# Kill Memcached
sudo service memcached stop
