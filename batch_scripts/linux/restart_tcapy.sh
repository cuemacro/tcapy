#!/bin/bash

# Starts the tcapy web application
# 1) gets tcapy enviroment variables
# 2) kills existing tcapy processes (celery, gunicorn etc.)
# 3) activates tcapy Python environment
# 4) starts gunicorn (if necessary)
# 5) restarts web server (Apache or Nginx)
# 6) clears Redis cache
# 7) starts celery for batch processing of TCA requests

# Note that on some instances of Linux this may not fetch the required directory (it has been tested with RedHat)
export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/installation/set_tcapy_env_vars.sh

# Set Python environment
source $SCRIPT_FOLDER/installation/activate_python_environment.sh

echo 'Batch folder' $SCRIPT_FOLDER
echo 'Cuemacro TCAPY' $TCAPY_CUEMACRO

# Kill all Python and Celery process and stop webserver
echo 'Killing any existing Python and celery processes...'
# sudo killall python
sudo killall celery
sudo killall gunicorn
sudo killall httpd
sudo killall apache2

# Make sure permissions set properly
sudo setenforce 0

# Create log folder (if it doesn't exist already)
mkdir -p $TCAPY_CUEMACRO/log/

# To run tests
mkdir -p /tmp/tcapy
mkdir -p /tmp/csv

if [ $TCAPY_PYTHON_STARTER == "gunicorn" ]; then
    echo 'Start Gunicorn to interact with Flask/Dash app'

    # Always start default 'tcapy' web interface
    gunicorn --bind 127.0.0.1:8090 --workers 4 --threads 6 --preload --chdir $TCAPY_CUEMACRO/tcapy/conf/ \
    --access-logfile $TCAPY_CUEMACRO/log/gunicorn_tcapy_access.log \
    --error-logfile $TCAPY_CUEMACRO/log/gunicorn_tcapy_error.log \
    --log-level DEBUG \
    tcapy_wsgi:application &

    # Start 'tcapy' RESTful API
    if [ $START_TCAPY_API == 1 ]; then
        echo 'Start Gunicorn for RESTful API...'
        gunicorn --bind 127.0.0.1:8091 --workers 4 --threads 6 --preload --chdir $TCAPY_CUEMACRO/tcapy/conf/ \
        --access-logfile $TCAPY_CUEMACRO/log/gunicorn_tcapy_access.log \
        --error-logfile $TCAPY_CUEMACRO/log/gunicorn_tcapy_error.log \
        --log-level DEBUG \
        tcapyapi_wsgi:application &

    fi

    # Start 'tcapyboard' web interface
    if [ $START_TCAPY_BOARD == 1 ]; then
        echo 'Start Gunicorn for tcapyboard...'
        gunicorn --bind 127.0.0.1:8092 --workers 4 --threads 6 --preload --chdir $TCAPY_CUEMACRO/tcapy/conf/ \
        --access-logfile $TCAPY_CUEMACRO/log/gunicorn_tcapy_access.log \
        --error-logfile $TCAPY_CUEMACRO/log/gunicorn_tcapy_error.log \
        --log-level DEBUG \
        tcapyboard_wsgi:application &

    fi

    # (We can add many different additional gunicorn instances here here too)

    # --log-file $TCAPY_CUEMACRO/log/gunicorn_tcapy.log \
    # --log-level DEBUG \&
elif [ $TCAPY_PYTHON_STARTER == "mod_wsgi" ]; then
    echo 'Using mod_wsgi to interact with Flask/Dash app (make sure paths are set in tcapy.wsgi are set correctly before installation)'
fi

echo 'Python is ' $TCAPY_PYTHON_ENV

echo 'Restarting webserver...' $TCAPY_WEB_SERVER

# Restart webserver
if [ $DISTRO == "ubuntu" ]; then
    if [ "$TCAPY_WEB_SERVER" == "apache" ]; then
        sudo service nginx stop
        sudo service apache2 restart
    elif [ "$TCAPY_WEB_SERVER" == "nginx" ]; then
        sudo service apache2 stop
        sudo service nginx restart
    fi

elif [ $DISTRO == "redhat"  ]; then
    if [ "$TCAPY_WEB_SERVER" == "apache" ]; then
        sudo service nginx stop
        sudo service httpd restart
    elif [ "$TCAPY_WEB_SERVER" == "nginx" ]; then
        sudo service httpd stop
        sudo service nginx restart
    fi
fi

echo 'Flush Celery cache...'

# Purge every message from the "celery" queue everything on celery (not strcitly necessary if using Redis, as flushing in next line)
# celery purge -f

# Flush redis of everything in cache (saved dataframes and message queue)
echo 'Flushing Redis cache...'

redis-cli flushall

echo 'Current working folder (set to notebook folder by default - so works with test trade files)'
cd $TCAPY_CUEMACRO/tcapy_notebooks
echo $PWD

echo 'About to start celery...'
celery -A tcapy.conf.celery_calls worker --purge --discard --loglevel=debug -Q celery --concurrency=$TCAPY_CELERY_WORKERS -f $TCAPY_CUEMACRO/log/celery.log &