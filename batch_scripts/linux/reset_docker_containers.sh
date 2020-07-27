#!/bin/bash

# This will kill all Docker containers and then remove volumes for some of the containers used by tcapy
# This will often be necessary to do when we are switching between test and product docker-compose

docker-compose rm -f mysql
docker-compose rm -f mongo
docker-compose rm -f redis
docker-compose rm -f memcached
docker-compose rm -f mysql
docker-compose rm -f celery
docker-compose rm -f gunicorn_tcapy
docker-compose rm -f gunicorn_tcapyboard nginx

docker-compose rm -v -f mysql
docker-compose rm -v -f mongo
docker-compose rm -v -f redis
docker-compose rm -v -f memcached
docker-compose rm -v -f mysql
docker-compose rm -v -f celery
docker-compose rm -v -f gunicorn_tcapy
docker-compose rm -v -f gunicorn_tcapyboard
docker-compose rm -v -f nginx

docker kill $(docker ps -q)