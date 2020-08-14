#!/bin/bash

# starts a proxy (mainly useful for testing)
# pproxy needs Python3
# SSL is usually 443 - but we choose a higher server_port so can run without sudo

pproxy -l http+ssl://127.0.0.1:7000 -l http://127.0.0.1:8080 --ssl server.crt,server.key --pac /autopac