#!/bin/bash

# This will kill all Docker containers and then remove volumes for some of the containers used by tcapy
# This will often be necessary to do when we are switching between test and product docker-compose

docker kill $(docker ps -q)
docker-compose rm -v -f mysql
docker-compose rm -v -f mongo
docker-compose rm -v -f celery
docker-compose rm -v -f redis