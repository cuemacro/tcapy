REM Restarts Windows Subsystem for Linux (needs to be run as administrator)
REM can be necessary to restart sometimes to kill processes

call net stop LxssManager
call net start LxssManager