#!/bin/bash

# If we want to use a proxy server for https (eg. for NCFX) like pproxy need SSL certificate
# this script creates a self signed SSL certificate

openssl genrsa -des3 -out server.key 1024
openssl req -new -key server.key -out server.csr
cp server.key server.key.org
openssl rsa -in server.key.org -out server.key
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt