"""This provides the entry point for the GUI web application, which uses the Dash library on top lightweight server
web application (eg. Flask). It queries TCAEngine, which returns appropriate TCA output (via TCACaller). Uses Layout
class to render the layout and SessionManager to keep track of each user session.

"""
from __future__ import division, print_function

__author__ = 'saeedamen' # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2019 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#
# This may not be distributed without the permission of Cuemacro.
#

import os
import sys

# for LINUX machines
try:
    tcapy_cuemacro = os.environ['TCAPY_CUEMACRO']
    sys.path.insert(0, tcapy_cuemacro + '/tcapy/')
except:
    print('Did not set path! Check if TCAPY_CUEMACRO environment variable is set?')

# it is recommended to set your Python environment before running app.py!

# try:
#     user_home = os.environ['HOME']
#
#     python_home = user_home + '/py27tca'
#     activate_this = python_home + '/bin/activate_this.py'
#
#     execfile(activate_this, dict(__file__=activate_this))
# except:
#     pass

# set plotly to work privately/offline
try:
    import plotly  # JavaScript based plotting library with Python connector

    plotly.tools.set_config_file(plotly_domain='https://type-here.com',
                                 world_readable=False,
                                 sharing='private')
except:
    pass

## web server components (used later)
import flask
from flask import Flask

import dash
import dash_bootstrap_components as dbc
from dash.dependencies import Output, Input

## for getting paths and general file operations
import os
import sys

# for caching data (in Redis)
from tcapy.util.mediator import Mediator

# utility stuff
from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.utilfunc import UtilFunc

# for caching data (in Redis)

# creates the HTML layout of the web pages
from tcapy.vis.sessionmanager import SessionManager
from tcapy.vis.sessionmanager import CallbackManager

constants = Constants()
util_func = UtilFunc()

# manage session information for every client
session_manager = SessionManager()

# manage creation of callback for Dash
callback_manager = CallbackManager()

logger = LoggerManager.getLogger(__name__)

# print constants for user information
logger.info("Platform = " + constants.plat)
logger.info("Env = " + constants.env)
logger.info("Python = " + sys.executable)
logger.info("Debug environment = " + str(constants.debug_start_flask_server_directly))

logger.info("Database volatile cache/Redis server_host = " + str(constants.volatile_cache_host_redis))
logger.info("Database arctic server_host = " + str(constants.arctic_host))
logger.info("Database ms sql server server_host = " + str(constants.ms_sql_server_host))

logger.info("Database trade/order data source = " + str(constants.default_trade_data_store))
logger.info("Database market data source = " + str(constants.default_market_data_store))