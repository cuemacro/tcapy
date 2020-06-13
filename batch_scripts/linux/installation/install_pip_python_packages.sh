#!/bin/bash

# Installs all the Python packages needed for tcapy, it also installs some additional packages for Jupyter, so you
# can interact with tcapy with Jupyter (strictly speaking if we could ignore these, if you don't wish to run Jupyter on
# top of tcapy

export SCRIPT_FOLDER="$( dirname "$(readlink -f -- "$0")" )"
source $SCRIPT_FOLDER/set_tcapy_env_vars.sh

# Set Python environment
source $SCRIPT_FOLDER/activate_python_environment.sh

echo 'Installing Python packages...'

if [ $TCAPY_PYTHON_ENV_TYPE == "virtualenv" ]; then

    # Install everything by pip
    pip install
        setuptools-git==1.2 cython==0.29.13 arctic==1.79.2 sqlalchemy==1.3.7 redis==3.3.7 pymssql==2.1.4 \
        pandas==0.24.2 numpy==1.16.4 scipy==1.3.1 statsmodels==0.10.1 tables==3.5.2 blosc==1.8.3 pyarrow==0.16.0 \
        pathos==0.2.1 multiprocess==0.70.8 fastparquet==0.3.3 \
        flask-restplus==0.13.0 gunicorn==19.9.0 \
        beautifulsoup4==4.8.0 pdfkit==0.6.1 psutil==5.6.3 \
        matplotlib==3.1.1 \
        boto3==1.5.11 \
        pyodbc==4.0.23 \
        pytest==5.1.0 pytest-cov==2.5.1 \
        mysql-connector-python==8.0.19 \
        IPython==7.13.0 chartpy==0.1.8 findatapy==0.1.11 dash-auth==1.3.2 cufflinks==0.17.3 plotly==4.8.0 chart-studio==1.1.0 \
        dash==1.12.0 dash-html-components==1.0.3 dash-core-components==1.10.0 dash-table==4.7.0 jupyter-dash==0.2.1 dtale==1.8.1 \
        dtale==1.8.1 \
        qpython==2.0.0 influxdb==5.2.3 \
        Flask-Session==0.3.1 \
        celery==4.4.0 pytest-tap kombu==4.6.7 python-memcached==1.59 numba==0.48.0 vispy==0.6.4 jinja2==2.11.1 \
        jupyterlab jupyter_contrib_nbextensions jupyter_nbextensions_configurator RISE bqplot WeasyPrint==51 \
        dask==2.14.0 distributed==2.14.0 cloudpickle==1.3.0 python-snappy==0.5.4 bokeh==2.0.1 msgpack==1.0.0 pystore==0.1.15 fsspec==0.3.3

    # Can't install orca with pip (has to be done manually or via conda)
    sudo apt-get install nodejs npm

elif [ $TCAPY_PYTHON_ENV_TYPE == "conda" ] && [ $CONDA_FROM_YAML == 0 ]; then

    # Install conda forge packages
    conda install -c conda-forge \
        setuptools-git=1.2 cython=0.29.13 arctic=1.79.2 sqlalchemy=1.3.7 redis-py=3.3.7 pymssql=2.1.4 \
        pandas=0.24.2 numpy=1.16.4 scipy=1.3.1 statsmodels=0.10.1 pytables=3.5.2 python-blosc=1.8.3 \
        pathos=0.2.1 multiprocess=0.70.8 fastparquet=0.3.3 \
        flask-restplus=0.13.0 gunicorn=19.9.0 \
        beautifulsoup4=4.8.0 python-pdfkit=0.6.1 psutil=5.6.3 \
        matplotlib=3.1.1 \
        boto3=1.5.11 \
        pyodbc=4.0.23 \
        pytest=5.1.0 pytest-cov=2.5.1 \
        numba=0.48.0 pyarrow=0.16.0 vispy=0.6.4 jinja2=2.11.1 \
        jupyterlab jupyter_contrib_nbextensions jupyter_nbextensions_configurator nodejs rise bqplot \
        dask=2.14.0 distributed=2.14.0 cloudpickle=1.3.0 python-snappy=0.5.4 bokeh=2.0.1 msgpack-python=1.0.0 --yes

    # Install charting libraries
    # for flash recording of session variables
    # to allow celery to use Redis
    pip install mysql-connector-python==8.0.19 chartpy==0.1.8 findatapy==0.1.11 dash-auth==1.3.2 cufflinks==0.17.3 plotly==4.8.0 \
        chart-studio==1.1.0 \
        dash==1.12.0 dash-html-components==1.0.3 dash-core-components==1.10.0 dash-table==4.7.0 jupyter-dash==0.2.1 dtale==1.8.1 \
        qpython==2.0.0 influxdb==5.2.3 \
        Flask-Session==0.3.1 \
        celery==4.4.0 pytest-tap kombu==4.6.7 python-memcached==1.59 WeasyPrint==51 pystore==0.1.15 fsspec==0.3.3

    # celery[redis]==4.1.1 celery[msgpack]==4.1.1
    # To allow printing of Plotly to PDF/creation of PNG files
    conda install -c plotly plotly-orca=1.3.1 --yes
fi