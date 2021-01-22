REM Installs all the Python packages needed for tcapy
set SCRIPT_FOLDER=%~dp0
call %SCRIPT_FOLDER%\set_tcapy_env_vars

REM Set Python environment
call %SCRIPT_FOLDER%\activate_python_environment

echo 'Installing Python packages...'

if %TCAPY_PYTHON_ENV_TYPE%==virtualenv (

    REM Install everything by pip
    call pip install ^
        setuptools-git==1.2 cython arctic==1.79.4 sqlalchemy==1.3.17 redis==3.3.7 ^
        pandas==1.0.5 numpy scipy statsmodels==0.11.1 blosc==1.8.3 pyarrow==2.0.0 ^
        pathos==0.2.1 multiprocess==0.70.9 fastparquet==0.5.0 ^
        flask-restplus==0.13.0 ^
        beautifulsoup4==4.8.0 psutil==5.6.6 ^
        matplotlib==3.3.1 ^
        boto3==1.5.11 ^
        pyodbc==4.0.23 ^
        pytest==5.4.3 pytest-cov==2.5.1 ^
        mysql-connector-python==8.0.19 ^
        chartpy==0.1.8 findatapy==0.1.19 IPython==7.14.0 dash-auth==1.4.0 ^
        cufflinks==0.17.3 plotly==4.9.0 chart-studio==1.1.0 kaleido==0.0.2 dash-bootstrap-components==0.10.3 ^
        dash==1.12.0 dash-html-components==1.0.3 dash-core-components==1.10.0 dash-table==4.7.0 jupyter-dash==0.2.1 dtale==1.8.1 ^
        qpython==2.0.0 influxdb==5.2.3 ^
        Flask-Session==0.3.1 ^
        celery==celery==5.0.5 msgpack-python pytest-tap kombu==4.6.7 python-memcached==1.59 ^
        numba==0.48.0 vispy==0.6.4 pdfkit==0.6.1 jinja2==2.11.2 xlwings==0.20.2 ^
        jupyterlab jupyter_contrib_nbextensions jupyter_nbextensions_configurator RISE bqplot WeasyPrint==51 ^
        dask==2.14.0 distributed==2.14.0 cloudpickle==1.3.0 python-snappy==0.5.4 bokeh==2.0.1 msgpack==1.0.0 pystore==0.1.22 ^
        fsspec==0.3.3 eikon=1.1.2
)

if %TCAPY_PYTHON_ENV_TYPE%==conda (

    if %CONDA_FROM_YAML%==0 (

        REM Install conda forge packages (removed gunicorn and python-pdfkit)
        call conda install -c conda-forge ^
            setuptools-git cython sqlalchemy redis-py ^
            pandas=1.0.5 numpy scipy statsmodels python-blosc ^
            pathos multiprocess fastparquet ^
            beautifulsoup4 psutil ^
            matplotlib ^
            pyodbc ^
            pytest pytest-cov ^
            numba pyarrow vispy jinja2 xlwings=0.20.2 ^
            jupyterlab jupyter_contrib_nbextensions jupyter_nbextensions_configurator nodejs rise bqplot ^
            dask distributed cloudpickle python-snappy bokeh msgpack-python --yes

        REM Install charting libraries for flash recording of session variables to allow celery to use Redis
        call pip install arctic==1.79.4 mysql-connector-python==8.0.19 chartpy==0.1.8 findatapy==0.1.19 dash-auth==1.3.2 cufflinks==0.17.3 ^
          plotly==4.9.0 chart-studio==1.1.0 kaleido==0.0.2 dash-bootstrap-components==0.10.3 ^
          dash==1.12.0 dash-html-components==1.0.3 dash-core-components==1.10.0 dash-table==4.7.0 jupyter-dash==0.2.1 dtale==1.8.1 ^
          qpython==2.0.0 influxdb==5.2.3 ^
          Flask-Session==0.3.1 ^
          celery==5.0.5 pytest-tap kombu python-memcached==1.59 boto3==1.5.11 pdfkit==0.6.1 WeasyPrint==51 pystore==0.1.22 ^
          fsspec==0.3.3 eikon==1.1.2
    )
)