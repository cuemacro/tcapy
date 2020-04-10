#!/bin/bash

# Increase file open limits for MongoDB (tested with Ubunut)
# Increasing the file open limits MongoDB tends to be more more stable
# We assume that the root user will run MongoDB

# See https://docs.mongodb.com/manual/reference/ulimit/
# See https://askubuntu.com/questions/162229/how-do-i-increase-the-open-files-limit-for-a-non-root-user

# Note, will overwrite with tcapy prepared versions
# * /etc/security/limits.conf (increases the nofile limit for root)
# * /etc/pam.d/common-session (enforces that limits.conf is read, by adding pam_limits.so)
# * /etc/pam.d/common-session-noninteractive /etc/pam.d
export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

sudo cp $TCAPY_CUEMACRO/tcapy/conf/limits.conf /etc/security/
sudo cp $TCAPY_CUEMACRO/tcapy/conf/common-session /etc/pam.d/
sudo cp $TCAPY_CUEMACRO/tcapy/conf/common-session-noninteractive /etc/pam.d/