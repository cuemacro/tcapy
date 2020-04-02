# Preferred to use apache/gunicorn (easier to support Python 3) instead of apache/wsgi (more for Python 2)

# if we need to run apache/wsgi for Python 3, then needs some patching of mod_wsgi
# as suggested in http://devmartin.com/blog/2015/02/how-to-deploy-a-python3-wsgi-application-with-apache2-and-debian/

import os
import sys

try:
    tcapy_cuemacro = os.environ['TCAPY_CUEMACRO']
    python_home = os.environ['TCAPY_PYTHON_ENV']
except:
    user_home = os.environ['USER']

    # if TCAPY_CUEMACRO not set globally (or the Python environment for TCAPY), we need to specify it here
    tcapy_cuemacro = '/home/' + user_home + '/cuemacro/'
    python_home = '/home/' + user_home + '/py36tca/'

activate_this = python_home + '/bin/activate_this.py'

execfile(activate_this, dict(__file__=activate_this))

sys.path.insert(0, tcapy_cuemacro + '/tcapy/')
# os.chdir(tcapy_cuemacro+ '/tcapy/tcapy/vis/')

from tcapy.vis.app_board import server as application
application.root_path = tcapy_cuemacro + '/tcapy/tcapy/vis/'