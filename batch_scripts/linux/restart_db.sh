#!/bin/bash

# Will restart the databases/key-value stores used by tcapy
# 1) MongoDB - for market tick data
# 2) MySQL - for trade/order data
# 3) Redis - key/value store for short term caching of market/trade/order data and as a message broker for Celery
# 4) Memcached - results backend for Celery
#
# If you use different databases for market tick data and/or trade/order data, you will need to edit this script to
# restart those servers. Also if you want to use RabbitMQ as a message broker/results backend you'll also need to edit.

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

# Sometimes security settings can prevent MongoDB from running
sudo setenforce 0

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

# Wait for mongod to shutdown first
sleep 10s

# Make sure file opened limit is very large (otherwise Mongo will rollover!)
# This should have been adjusted in limits.conf
# sudo sh -c "ulimit -c 100000 && exec su $LOGNAME"

# source $SCRIPT_FOLDER/activate_file_limits.sh

# Now start up redis, mysql and mongod in the background
# sudo service mongod start
if [ $START_CLICKHOUSE == 1 ]; then
  # Starting Clickhouse columnar database
  sudo service clickhouse-server start
  echo "Started Clickhouse"
fi

if [ $START_MONGODB == 1 ]; then
  # Starting MongoDB as root
  sudo mongod --config $TCAPY_CUEMACRO/tcapy/conf/mongo.conf
  echo "Started MongoDB"
fi

# Start MySQL
if [ $START_MYSQL == 1 ]; then
  sudo service mysql restart
  echo "Started MySQL"
fi

# Start Redis and flush cache
sudo redis-server $TCAPY_CUEMACRO/tcapy/conf/redis.conf --daemonize yes
sudo redis-cli flushall

# Start Memcached and flush cache
sudo service memcached start
echo flush_all > /dev/tcp/localhost/11211
