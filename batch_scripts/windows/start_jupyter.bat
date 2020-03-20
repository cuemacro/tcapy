REM Starts a Jupyter notebook, so tcapy can be accessed through that
set SCRIPT_FOLDER=%~dp0
call %SCRIPT_FOLDER%\installation\set_tcapy_env_vars

REM Set Python environment
call %SCRIPT_FOLDER%\installation\activate_python_environment

REM Only allow local access
jupyter notebook ^
  --notebook-dir=%TCAPY_CUEMACRO%\tcapy_notebooks --ip=* --port=9999

REM Alternatively have a key to access
REM Create your own pem and key by following https://support.microfocus.com/kb/doc.php?id=7013103
REM jupyter notebook ^
REM --certfile='mycert.pem' ^
REM --keyfile='mykey.key' --ip=* --port=9999