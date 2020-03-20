#!/bin/bash

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"

echo $SCRIPT_FOLDER