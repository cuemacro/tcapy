from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#


import logging
import logging.config

import threading

# import cloghandler
# from cloghandler import ConcurrentRotatingFileHandler

from tcapy.conf.constants import Constants
from tcapy.util.singleton import Singleton

constants = Constants()

try:
    from celery.utils.log import get_task_logger
    from celery import current_task
except:
    current_task = False

class LoggerManager(object):
    """LoggerManager acts as a wrapper for logging the runtime operation of tcapy

    """
    __metaclass__ = Singleton

    _loggers = {}
    _loggers_lock = threading.Lock()

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def getLogger(name=None):

        if not current_task:
            # Directly called (ie. not by celery)

            if not name:
                try:
                    logging.config.dictConfig(constants.logging_parameters)
                except:
                    pass

                log = logging.getLogger();
            elif name not in LoggerManager._loggers.keys():
                try:
                    # logging.config.fileConfig(Constants().logging_conf)
                    logging.config.dictConfig(constants.logging_parameters)
                except:
                    pass

                with LoggerManager._loggers_lock:
                    LoggerManager._loggers[name] = logging.getLogger(str(name))

            log = LoggerManager._loggers[name]

            # When recalling appears to make other loggers disabled
            # hence apply this hack!
            for name in LoggerManager._loggers.keys():
                with LoggerManager._loggers_lock:
                    LoggerManager._loggers[name].disabled = False

            # log.debug("Called directly")

        elif current_task.request.id is None:
            log = get_task_logger(name)
            # log.debug("Called synchronously")
        else:
            log = get_task_logger(name)
            # log.debug("Dispatched now!")

        return log
