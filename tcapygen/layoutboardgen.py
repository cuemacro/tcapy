from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

## Web server components
import dash_core_components as dcc
import dash_html_components as html

import base64

import os

## Date/Time components
import pandas as pd
import datetime
from datetime import timedelta

from pandas.tseries.offsets import *

from tcapy.vis.layoutdash import LayoutDash

########################################################################################################################

class LayoutDashImplBoardGen(LayoutDash):
    """This implements the LayoutDash abstract class, to create the web based GUI for the tcapy application. It creates two
    web pages

    - detailed_page - for doing detailed tcapy analysis for a specific currency pair
    - aggregated_page - for more aggregated style analysis across multiple currency pairs and over multiple time periods

    """

    def __init__(self, app=None, onstants=None, url_prefix=''):
        super(LayoutDashImplBoardGen, self).__init__(app=app, constants=constants, url_prefix=url_prefix)

        available_dates = pd.date_range(
            datetime.datetime.today().date() - timedelta(days=self._constants.gui_lookback_window),
            datetime.datetime.today().date(), freq=BDay())

        times = pd.date_range("0:00", "23:59", freq="15min")

        ### Create the possible values for drop down boxes on both pages

        # Reverse date list (for both detailed and aggregated pages)
        self.available_dates = [x.date() for x in available_dates[::-1]]

        # For detailed page only
        self.available_times = [t.strftime("%H:%M") for t in times]

        self.available_tickers = self._constants.available_tickers_dictionary['All']
        self.available_venues = self._constants.available_venues_dictionary['All']

        self.available_brokers = self._constants.available_brokers_dictionary['All']
        self.available_algos = self._constants.available_algos_dictionary['All']
        self.available_market_data = self._constants.available_market_data

        self.available_order_plot_lines = ['candlestick', 'mid', 'bid', 'ask', 'arrival', 'twap', 'vwap',
                                           'buy trade', 'sell trade']
        self.available_execution_plot_lines = ['candlestick', 'mid', 'bid', 'ask', 'buy trade', 'sell trade']

        # For aggregated page only
        self.available_grouped_tickers = self._flatten_dictionary(self._constants.available_tickers_dictionary)
        self.available_grouped_venues = self._flatten_dictionary(self._constants.available_venues_dictionary)
        self.available_grouped_brokers = self._flatten_dictionary(self._constants.available_brokers_dictionary)
        self.available_grouped_algos = self._flatten_dictionary(self._constants.available_algos_dictionary)

        self.available_event_types = self._constants.available_event_types
        self.available_metrics = self._constants.available_metrics

        self.available_reload = ['no', 'yes']

        self.construct_layout()

    def _flatten_dictionary(self, dictionary):
        available = dictionary['All']
        available_groups = self._util_func.dict_key_list(dictionary.keys())

        return self.flatten_list_of_strings([available_groups, available])

    def construct_layout(self):
        self.page_content = html.Div([
            dcc.Location(id='url', refresh=False),
            html.Div(id='page-content')
        ])

        link_bar_dict = {'Aggregated' : '/aggregated'}

        markout_cols = self._util_func.flatten_list_of_lists(
            ['Date', 'exec not', 'not cur', 'side',
             [str(x) + self._constants.markout_unit_of_measure for x in self._constants.markout_windows], 'markout'])

        # Main page for detailed analysing of (eg. over the course of a few days)

        ########################################################################################################################

        # Secondary page for analysing aggregated statistics over long periods of time, eg. who is the best broker?

        self.pages['aggregated'] = html.Div([

            self._sc.header_bar('FX: Aggregated - Trader Analysis', img='logo.png'),
            self._sc.link_bar(link_bar_dict),
            self._sc.width_row_cell(html.B("Status: ok", id='aggregated-status'), margin_left=5),

            self._sc.horizontal_bar(),

            # dropdown selection boxes
            html.Div([
                self._sc.drop_down(caption='Market Data', id='market-data-val', prefix_id='aggregated',
                               drop_down_values=self.available_market_data),
            ]),

            self._sc.horizontal_bar(),
            self._sc.uploadbox(caption='Aggregated trade CSV', id='csv-uploadbox', prefix_id='aggregated'),

            self._sc.horizontal_bar(),
            self._sc.button(caption='Calculate', id='calculation-button', prefix_id='aggregated'),

            # , msg_id='aggregated-status'),
            self._sc.horizontal_bar(),

            # self.date_picker_range(caption='Start/Finish Dates', id='aggregated-date-val', offset=[-7,-1]),
            self._sc.plot(caption='Aggregated Trader: Summary',
                      id=['execution-by-ticker-bar-plot', 'execution-by-venue-bar-plot', 'execution-by-broker_id-bar-plot'], prefix_id='aggregated', height=500),
            self._sc.horizontal_bar(),
            self._sc.plot(caption='Aggregated Trader: Timeline', id='execution-by-ticker-timeline-plot',
                      prefix_id='aggregated', height=500),
            self._sc.horizontal_bar(),
            self._sc.plot(caption='Aggregated Trader: PDF fit (' + self._constants.reporting_currency + ' notional)',
                      id=['execution-by-ticker-dist-plot', 'execution-by-broker_id-dist-plot', 'execution-by-venue-dist-plot'],
                      prefix_id='aggregated', height=500),
            self._sc.horizontal_bar(),
            self._sc.table(caption='Executions: Markout Table', id='execution-table', prefix_id='aggregated', columns=markout_cols,
                       downloadplot_caption=['Markout CSV', 'Full execution CSV'],
                       downloadplot_tag=['execution-markout-download-link', 'execution-full-download-link'],
                       download_file=['download_execution_markout.csv', 'download_execution_full.csv']
                       ),
        ],
            style={'width': '1000px', 'marginRight': 'auto', 'marginLeft': 'auto'})


        # ID flags
        self.id_flags = {

            # aggregated trader page
            'aggregated_bar_trade_order': {'execution-by-ticker': 'bar_trade_df_by/mean/ticker',
                                           'execution-by-broker_id': 'bar_trade_df_by/mean/broker_id',
                                           'execution-by-venue': 'bar_trade_df_by/mean/venue'},

            'aggregated_timeline_trade_order': {'execution-by-ticker': 'timeline_trade_df_by/mean_date/ticker'},

            'aggregated_dist_trade_order': {'execution-by-ticker': 'dist_trade_df_by/pdf/ticker',
                                            'execution-by-broker_id': 'dist_trade_df_by/pdf/broker_id',
                                            'execution-by-venue': 'dist_trade_df_by/pdf/venue'},

            'aggregated_table_trade_order': {'execution': 'table_trade_df_markout_by_all'},

            'aggregated_download_link_trade_order': {'execution-full': 'trade_df',
                                                     'execution-markout': 'table_trade_df_markout_by_all'},
        }
