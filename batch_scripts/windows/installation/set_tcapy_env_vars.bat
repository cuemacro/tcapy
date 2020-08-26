REM This script has various configuration flags for tcapy which need to be set
REM It is unlikely you will need to change many of these, except possibly the folder where tcapy resides

echo "Setting environment variables for tcapy for current script"

REM Python environment settings ########################################################################################

REM Folder where tcapy is, note, if you will need to change this in tcapy/conf/mongo.conf too
set TCAPY_CUEMACRO=e:\cuemacro\tcapy

REM Is the Python environment either "conda" or "virtualenv"?
set TCAPY_PYTHON_ENV_TYPE=conda

REM virtualenv folder or conda name
REM set TCAPY_PYTHON_ENV=/home/$USER/py37tca/

REM virtualenv folder or conda name
set CONDA_ACTIVATE=C:\Anaconda3\Scripts\activate.bat
set TCAPY_PYTHON_ENV=py37tca
set TCAPY_PYTHON_ENV_BIN=%TCAPY_PYTHON_ENV%\bin\

REM Installation parameters - create conda environment from YAML (1 or 0)
set CONDA_FROM_YAML=1

REM Only Python 3 is now supported
set TCAPY_PYTHON_VERSION=3

REM for using Excel/xlwings front end for tcapy
set EXCEL_PATH="C:\Program Files\Microsoft Office\root\Office16\EXCEL.EXE"