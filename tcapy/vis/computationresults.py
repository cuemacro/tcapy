from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc

from tcapy.util.utilfunc import UtilFunc
from tcapy.vis.displaylisteners import PlotRender

ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

class ComputationResults(ABC):
    """Abstract class holds the results of a computation in a friendly format, splitting out the various dataset which can be used
    for charts. Also converts these datasets to Plotly Figure objects, ready to be plotted in HTML documents.

    """

    def __init__(self, dict_of_df, computation_request, text_preamble=''):
        self._plot_render = PlotRender()
        self._util_func = UtilFunc()
        self.text_preamble = text_preamble

        self.computation_request = computation_request

        self._rendered = False

    @abc.abstractmethod
    def render_computation_charts(self):
        """Takes the various dataframes computation results output, and then renders these as Plotly JSON charts (data and
        all their graphical properties), which are easy to plot later.

        Returns
        -------

        """
        pass

    ##### Other data (eg. text)
    @property
    def text_preamble(self):
        return self.__text_preamble

    @text_preamble.setter
    def text_preamble(self, text_preamble):
        self.__text_preamble = text_preamble

    @property
    def computation_request(self):
        return self.__computation_request

    @computation_request.setter
    def computation_request(self, computation_request):
        self.__computation_request = computation_request