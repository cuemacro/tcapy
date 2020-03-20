#!/bin/bash

# This will install Anaconda into /home/$USER/anaconda3 - note you might need to change the URL where you
# are downloading from
export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# Get correct installer script and config file for OS (you may need to change this)
URL="https://repo.continuum.io/archive/Anaconda3-2020.02-Linux-x86_64.sh"
CONFIG=".bashrc"

# Download, run, and clean up installer script
wget -O ~/anaconda-installer.sh $URL
bash ~/anaconda-installer.sh -b -p ~/anaconda3
rm ~/anaconda-installer.sh

# Enable Anaconda and add to path from within config file
chmod -R 777 ~/anaconda3
echo 'export PATH=~/anaconda3/bin:$PATH' >> ~/${CONFIG}
source ~/${CONFIG}

echo "Anaconda3 has been successfully installed to ~/anaconda3."
echo "This version will override all other versions of Python on your system."
echo "Your ~/${CONFIG} file has been modified to add Anaconda 3 to your PATH variable."
echo "For more info about Anaconda, see http://docs.continuum.io/anaconda/index.html"

# also install a Python 3.6 conda environment
# ~/anaconda3/bin/conda create -n $TCAPY_PYTHON_ENV python=3.6

# So conda can be used in bash shell
~/anaconda3/bin/conda init bash
