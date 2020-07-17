#!/bin/bash

# This will install necessary components for tcapy. In practice, you might end up having different configuations, because
# you may to choose different databases for your trade/order data (eg. MySQL or PostgreSQL) and also a different database
# for your market tick data, instead of MongoDB/Arctic (eg. KDB or InfluxDB, which are also supported), or you may
# choose to use CSVs for all market/trade/order data.
#
# If you have already installed some of these dependecies like MongoDB, you will need to remove them from this list.
#
# - Python tools/gcc/Apache (optional)
# - create a virtual environment (either "conda" which is preferred default, or "virtualenv" style)
# - install Linux driver to access Microsoft SQL Server (if you use it to store trade/order data)
# - install various Python packages necessary for tcapy into the Python virtual environment
# - install nginx web server (which is the recommended server for tcapy's web app)
# - install tcapy on nginx, so the tcapy web app is accessable
# - install MongoDB to store market tick data (optional KDB/InfluxDB/CSV/DataFrame)
# - increase file open limits (without this MongoDB tends to be very unstable)
# - install MySQL to store trade/order data (optional Microsoft SQL Server/PostgreSQL/MySQL/CSV)
# - install Memcached as a results backend for Celery
# - install tcapy on Apache, which is deprecated
# - install wkhtmltopdf for converting HTML to PDF
# - install dependencies for Weasyprint another converter for HTML to PDF
# - install Jupyter extensions
# - install Redis to cache market/trade/order data and as a message broker with Celery
#
# The various scripts will work either on Red Hat or Ubuntu. You might need to make minor changes if you want to run
# on similar Linux variants to Red Hat (eg. CentOS) or Ubuntu (eg. Debian)
#
# You will like need to modify it, as you are already likely to store your trade data (ie. Microsoft SQL Server,
# Postgres etc.) in an existing database, or you might use CSVs for your trade/order data
#
# It is assumed that you have already installed Anaconda Python on your machine using install_anaconda.sh

# Install Python setup tools, gcc (compiler) and Apache web server etc.
source install_python_tools_apache.sh

# Setup the virtual Python environmnent (py36tca) - by default conda environment from environment_linux_py36.yml
source install_virtual_env.sh

# Install the Microsoft SQL Server driver on Linux (only necessary if we want to use SQL Server for trade data)
# assumes that Microsoft SQL Server has already been installed (or you are accessing it over a network)
sudo ./install_sql_driver.sh

# Install all the Python packages in the py36tca environment
# If the conda environment has not already been created from the environment_linux_py36tca.yml file (default)
# It is generally quicker to create from YML file rather than running conda/pip for each library
source install_pip_python_packages.sh

# Install nginx web server (primary web server supported by tcapy)
source install_nginx.sh

# Install database for tick data (MongoDB)
# note that we can run MongoDB, MySQL and Redis on different computers
source install_mongo.sh

# Increases the number of open files for root user (for MongoDB)
source increase_file_limits.sh

# Install database for trade/order data (MySQL) - PostgreSQL also supported
source install_mysql.sh

# Install Memcached as a results backend for Celery (recommend on the same server)
source install_memcached.sh

# Install RabbitMQ as a results backend for Celery (AMPQ as a message broker is deprecated in Celery)
# source install_rabbitmq.sh

# Setup the tcapy application so that it can be picked up nginx/gunicorn
source install_tcapy_on_nginx_gunicorn.sh
# source install_tcapy_on_apache_gunicorn.sh
# source install_tcapy_on_apache.sh # uses WSGI, but this tends to be slower

# Install wkhtmltopdf for converting HTML to PDF
source install_pdf.sh

# Install weasyprint dependencies
source install_weasyprint.sh

# Install Jupyter extensions
source install_jupyter_extensions.sh

# Install Redis key-value store for general caching and as Celery message broker (recommend on same server)
source install_redis.sh

# We need to open ports to allow access to MongoDB and to give web access to specific clients
# source add_ip_to_firewall.sh