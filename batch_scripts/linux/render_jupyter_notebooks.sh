# Note that on some instances of Linux this may not fetch the required directory (it has been tested with RedHat)
export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

echo 'Batch folder' $SCRIPT_FOLDER
echo 'Cuemacro TCAPY' $TCAPY_CUEMACRO

# Set Python environment
source $SCRIPT_FOLDER/installation/activate_python_environment.sh

cd $TCAPY_CUEMACRO/tcapy_notebooks
jupyter nbconvert --to html *.ipynb