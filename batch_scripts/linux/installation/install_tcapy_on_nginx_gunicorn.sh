#!/bin/bash

# Installs tcapy app on nginx web server, using gunicorn to run the Python app, generally much easier to use
# than any of the Apache combinations

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# for running Flask/Dash app in nginx but redirected via Gunicorn (rather than using mod_wsgi, which tends to be slower)
site_name="tcapy"
site_name_conf="tcapy.conf"
site_name_nginx_conf="tcapy_nginx_gunicorn.conf"
site_folder="$TCAPY_CUEMACRO/tcapy/conf"

sudo mkdir -p /etc/nginx/sites-available
sudo mkdir -p /etc/nginx/sites-enabled
sudo chmod a+x $site_folder/$site_name_nginx_conf
sudo cp $site_folder/$site_name_nginx_conf  /etc/nginx/sites-available/$site_name_conf
sudo cp $site_folder/$site_name_nginx_conf  /etc/nginx/sites-enabled/$site_name_conf
sudo cp $site_folder/$site_name_nginx_conf  /etc/nginx/conf.d/$site_name_conf

# on Red Hat these file doesn't usually exist
sudo rm -f /etc/nginx/sites-enabled/000-default.conf
sudo rm -f /etc/nginx/conf.d/000-default.conf
sudo rm -f /etc/nginx/conf.d/default.conf

# Allows reading of files outside of nginx folder (just for Red Hat/Centos
if [ $DISTRO == "redhat"  ]; then
  sudo setenforce 0
fi

sudo chmod -R o+rx $site_folder
# sudo chmod -R a+r /var/log/httpd
# sudo ln /var/log/httpd/error_log $site_folder/log/error_log