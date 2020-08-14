#!/bin/bash

# In this case we assume our user is running on 192.168.82.85 (you will need to change IPs as appropriate) and
# are opening up ports so they can access
# tcapy webapp and also so MongoDB can be accessed

# We can remove the rules by editing file
# /etc/firewalld/zones/public.xml

# careful ONLY add the IPs of trader/compliance machines you want to have access
# do not expose to the whole network!

# only these browsers will able to access the applications which are on these IPs (server_port 80)
sudo firewall-cmd --permanent --zone=public --add-rich-rule='rule family="ipv4" source address="192.168.82.85/32" port protocol="tcp" port="80" accept'

# for https
sudo firewall-cmd --permanent --zone=public --add-rich-rule='rule family="ipv4" source address="192.168.82.85/32" port protocol="tcp" port="443" accept'

# for MongoDB access (only add IP for systems where we are running tcapy) - server_port 27017
sudo firewall-cmd --permanent --zone=public --add-rich-rule='rule family="ipv4" destination address="192.168.1.192/32" port protocol="tcp" port="27017" accept'
sudo firewall-cmd --reload

# we are assuming that Redis is running on the tcapy machine