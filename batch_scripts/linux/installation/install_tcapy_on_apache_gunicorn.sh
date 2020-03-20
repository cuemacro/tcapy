#!/bin/bash

# Installs tcapy app on Apache (using gunicorn to run the app)

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# for running Flask/Dash app in Apache but redirected via Gunicorn (rather than using mod_wsgi, which tends to be slower)
site_name="tcapy"
site_name_conf="tcapy.conf"
site_name_apache_conf="tcapy_apache_gunicorn.conf"
site_folder="$TCAPY_CUEMACRO/tcapy/conf"

sudo mkdir -p /etc/httpd/sites-available
sudo mkdir -p /etc/httpd/sites-enabled
sudo chmod a+x $site_folder/$site_name_apache_conf
sudo cp $site_folder/$site_name_apache_conf  /etc/httpd/sites-available/$site_name_conf
sudo cp $site_folder/$site_name_apache_conf  /etc/httpd/sites-enabled/$site_name_conf
sudo cp $site_folder/$site_name_apache_conf  /etc/httpd/conf.d/$site_name_conf

# on Red Hat this file doesn't usually exist
sudo rm /etc/httpd/sites-enabled/000-default.conf

# allows reading of files outside of Apache's folder
sudo setenforce 0

sudo chmod -R o+rx $site_folder
sudo chmod -R a+r /var/log/httpd
# sudo ln /var/log/httpd/error_log $site_folder/log/error_log