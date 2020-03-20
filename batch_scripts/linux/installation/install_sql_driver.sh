#!/bin/bash

# Install Microsoft SQL Server driver on Linux (only necessary if your trade/order data is stored on a Microsoft
# SQL Server

# See https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15#ubuntu17

# Run this script with sudo.. eg sudo ./install_sql_driver.sh
# adapted from https://blogs.msdn.microsoft.com/sqlnativeclient/2017/06/30/servicing-update-for-odbc-driver-13-1-for-linux-and-macos-released/

# Also see https://serverfault.com/questions/838166/installing-odbc-driver-13-for-mssql-server-in-amazon-linux-on-ec2-instance
# for alternative way of installing on Amazon Linux

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

if [ $DISTRO == "ubuntu" ]; then
    # sudo su
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

    # Download appropriate package for the OS version
    # Choose only ONE of the following, corresponding to your OS version

    # Ubuntu 16.04
    # curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

    # Ubuntu 18.04
    curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

    # Ubuntu 19.10
    # curl https://packages.microsoft.com/config/ubuntu/19.10/prod.list > /etc/apt/sources.list.d/mssql-release.list

    # exit
    sudo apt-get update
    sudo apt-get remove msodbcsql mssql-tools
    sudo ACCEPT_EULA=Y apt-get --yes install msodbcsql17 #=13.0.1.0-1

    # Optional: for bcp and sqlcmd
    sudo ACCEPT_EULA=Y apt-get --yes install mssql-tools #=14.0.2.0-1
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
    source ~/.bashrc

    # Optional: for unixODBC development headers
    sudo apt-get --yes install unixodbc-dev

elif [ $DISTRO == "redhat"  ]; then
    # sudo su

    # Download appropriate package for the OS version
    # Choose only ONE of the following, corresponding to your OS version

    # RedHat Enterprise Server 6
    # curl https://packages.microsoft.com/config/rhel/6/prod.repo > /etc/yum.repos.d/mssql-release.repo

    # RedHat Enterprise Server 7
    curl https://packages.microsoft.com/config/rhel/7/prod.repo > /etc/yum.repos.d/mssql-release.repo

    # RedHat Enterprise Server 8 and Oracle Linux 8
    # curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo

    sudo yum remove unixODBC-utf16 unixODBC-utf16-devel msodbcsql # To avoid conflicts
    sudo ACCEPT_EULA=Y yum install --yes msodbcsql=13.0.1.0-1

    # Optional: for bcp and sqlcmd
    # sudo ACCEPT_EULA=Y yum install --yes mssql-tools
    # echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
    # echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
    source ~/.bashrc

    # Optional: for unixODBC development headers
    sudo yum install unixODBC-devel

elif [ $DISTRO == "redhat-old"  ]; then
    # Install MSSQL driver
    sudo curl https://packages.microsoft.com/config/rhel/7/prod.repo > /etc/yum.repos.d/mssql-release.repo # (for Redhat)
    # sudo curl https://packages.microsoft.com/config/rhel/7/prod.repo | sudo tee /etc/yum.repos.d/msprod.repo # (for Amazon Linux)

    sudo yum remove unixODBC-utf16 unixODBC-utf16-devel # To avoid conflicts
    # sudo ACCEPT_EULA=Y yum install msodbcsql-13.1.9.0-1 mssql-tools-14.0.6.0-1 unixODBC-devel
    sudo ACCEPT_EULA=Y yum install msodbcsql mssql-tools unixODBC-devel
fi