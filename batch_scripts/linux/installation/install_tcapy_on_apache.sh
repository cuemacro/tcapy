#!/bin/bash

# Installs tcapy app on Apache (using WSGI)

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

site_name="tcapy"
site_name_conf="tcapy.conf"
# site_name_apache_conf="tcapy_apache.conf"
site_name_apache_conf="tcapy_apache.conf"
site_folder="$TCAPY_CUEMACRO/tcapy/conf"

sudo mkdir -p /etc/httpd/sites-available
sudo mkdir -p /etc/httpd/sites-enabled
sudo chmod a+x $site_folder/$site_name_apache_conf
sudo cp $site_folder/$site_name_apache_conf  /etc/httpd/sites-available/$site_name_conf
sudo cp $site_folder/$site_name_apache_conf  /etc/httpd/sites-enabled/$site_name_conf
sudo cp $site_folder/$site_name_apache_conf  /etc/httpd/conf.d/$site_name_conf

# on Red Hat this file doesn't usually exist
sudo rm /etc/httpd/sites-enabled/000-default.conf

# need to link Python script to web server
sudo mkdir /var/www/$site_name
sudo chown $TCAPY_USER /var/www/$site_name
sudo cp $site_folder/"$site_name.wsgi" /var/www/$site_name
sudo cd /var/www/$site_name

# allows reading of files outside of Apache's folder
sudo setenforce 0

sudo chmod -R o+rx $site_folder
sudo chmod a+xr /var/www/$site_name/"$site_name.wsgi"
sudo chmod -R a+r /var/log/httpd
# sudo ln /var/log/httpd/error_log $site_folder/log/error_log