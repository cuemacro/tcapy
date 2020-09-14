#!/bin/bash

# Create a conda environment (py36tca) from a pre-made YAML file, which is much faster than
# running conda/pip for individual packages, as done in install_pip_python_packages.sh
export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

conda env create -f $TCAPY_CUEMACRO/batch_scripts/linux/installation/environment_linux_py37tca.yml