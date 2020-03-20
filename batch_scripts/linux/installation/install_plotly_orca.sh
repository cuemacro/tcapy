#!/bin/bash

# orca is an additional plotly application for converting Plotly charts to binary PNG images (so can be inlined on webpages)
# it is only necessary to install if you wish to generate PDF reports (HTML reports do not need this) or want
# to allow Plotly to create PNG
#
# If you install with conda you don't need to do this, you can just do something like...
#   conda install -c plotly plotly-orca=1.2.1 --yes

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

cd $TCAPY_CUEMACRO

# Get orca from Plotly
wget https://github.com/plotly/orca/releases/download/v1.2.1/orca-1.2.1-x86_64.AppImage

chmod +x orca-1.2.1-x86_64.AppImage

ln -s $TCAPY_CUEMACRO/orca-1.2.1-x86_64.AppImage /usr/bin/orca