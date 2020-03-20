#!/bin/bash

# orca is an additional plotly application for converting Plotly charts to binary PNG images (so can be inlined on webpages)
# it is only necessary to install if you wish to generate PDF reports (HTML reports do not need this) or want
# to allow Plotly to create PNG
#
# If you install with conda you don't need to do this, you can just do something like...
#   conda install -c plotly plotly-orca=1.2.1 --yes

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# Need to install xvfb server and also wkhtmltopdf (orca will be run inside this too)
if [ $DISTRO == "ubuntu" ]; then
    sudo apt-get update
    sudo apt-get install --yes xvfb libfontconfig wkhtmltopdf

elif [ $DISTRO == "redhat"  ]; then
    # isn't usually in Red Hat repo, but can download manually from centos (compatible Linux distribution with Red hat)
    wget http://vault.centos.org/6.2/os/x86_64/Packages/xorg-x11-server-Xvfb-1.10.4-6.el6.x86_64.rpm
    sudo yum --yes localinstall xorg-x11-server-Xvfb-1.10.4-6.el6.x86_64.rpm

    # Remove the downloaded file
    rm xorg-x11-server-Xvfb-1.10.4-6.el6.x86_64.rpm.*

    # wkhtmltopdf is an a command line application for converting HTML to PDF, which is required by orca (and pdfkit)
    cd /tmp

    rm wkhtmltox-0.12.4_linux-generic-amd64.tar.xz

    wget https://github.com/wkhtmltopdf/wkhtmltopdf/releases/download/0.12.4/wkhtmltox-0.12.4_linux-generic-amd64.tar.xz
    sudo tar -xvf wkhtmltox-0.12.4_linux-generic-amd64.tar.xz
    sudo cp wkhtmltox/bin/wkhtmltopdf /usr/bin/
fi