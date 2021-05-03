from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc

## web server components
import dash_core_components as dcc
import dash_html_components as html

import dash_table as dt

## time/date components
import datetime
from datetime import timedelta

from collections import OrderedDict

from tcapy.util.utilfunc import UtilFunc

from tcapy.conf.constants import Constants

from chartpy.dashboard.layoutcanvas import LayoutCanvas

class LayoutDash(LayoutCanvas):
    """Abstract class for creating HTML pages via Dash/HTML components. Has generic methods for creating HTML/Dash
    components, including, header bars, link bars, buttons and plots
    """

    def __init__(self, app=None, constants=None, url_prefix=''):
        super(LayoutDash, self).__init__(app=app, constants=constants, url_prefix=url_prefix)

        self.id_flags = {}
        self.pages = {}

        self._util_func = UtilFunc()
        self._url_prefix = url_prefix

    def id_flag_parameters(self):
        return self.id_flags

    def calculate_button(self):
        pass

    def page_name(self):
        pass

    def attach_callbacks(self):
        pass

    def construct_layout(self):
        pass
