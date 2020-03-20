import os
import sys

tcapy_cuemacro_home = os.environ['TCAPY_CUEMACRO']

sys.path.insert(0, tcapy_cuemacro_home)
# os.chdir(user_home + '/cuemacro/tcapy/tcapypro/vis/')

from tcapy.api.app_api import application
application.root_path = tcapy_cuemacro_home  + '/tcapy/api/'