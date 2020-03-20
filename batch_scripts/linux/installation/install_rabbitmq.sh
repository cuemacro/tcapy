#!/bin/bash

# Installs RabbitMQ messaging broker (needs Erlang as a prerequisite first) - note this is optional
# By default tcapy uses memcached as Celery results backend and Redis as a message broker, which are easier to manage

sudo yum install --yes erlang
sudo yum install --yes socat
sudo rpm --import https://www.rabbitmq.com/rabbitmq-signing-key-public.asc
sudo rpm -Uvh https://www.rabbitmq.com/releases/rabbitmq-server/v3.6.9/rabbitmq-server-3.6.9-1.el6.noarch.rpm
sudo rabbitmq-plugins enable rabbitmq_management