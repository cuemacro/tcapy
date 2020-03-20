from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#


from tcapy.vis.report.computationreport import ComputationReport

class TCAReport(ComputationReport):
    """Creates an HTML or PDF report from TCA results, which can be written to disk or returned as a string/binary
    for display elsewhere (eg. to be output by a webserver)

    """

    def __init__(self, tca_results, title='Cuemacro TCA'):
        super(TCAReport, self).__init__(tca_results, title=title)

    def _layout_computation_results_to_html(self, embed_chart='offline_embed_js_div'):

        # make sure to convert the dataframes into Plotly Figures first
        self._computation_results.render_computation_charts()

        # add some text at the beginning
        text_preamble = self._create_text_html(self._computation_results.text_preamble)

        # generate the HTML for all the sparse market charts
        sparse_market_charts = self._create_chart_html(self._computation_results.sparse_market_charts,
                                                      embed_chart=embed_chart)

        # generate the HTML for all the timeline charts
        timeline_charts = self._create_chart_html(self._computation_results.timeline_charts, embed_chart=embed_chart)

        # generate the HTML for all the bar charts
        bar_charts = self._create_chart_html(self._computation_results.bar_charts, embed_chart=embed_chart)

        # generate the HTML for all the distribution charts
        dist_charts = self._create_chart_html(self._computation_results.dist_charts, embed_chart=embed_chart)

        # include the HTML for tables
        styled_tables = self._create_table_html(self._computation_results.styled_tables)

        return text_preamble + sparse_market_charts + timeline_charts + bar_charts + dist_charts + styled_tables
