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

## Date/time components
import pandas as pd
import datetime
from datetime import timedelta

from collections import OrderedDict

from pandas.tseries.offsets import *

## For program constants
from tcapy.conf.constants import Constants

from tcapy.vis.layout import Layout

# Just instantiate once!
constants = Constants()

########################################################################################################################

class LayoutImplGen(Layout):
    """This implements the Layout abstract class, to create the web based GUI for the tcapy application. It creates two
    web pages

    - detailed_page - for doing detailed tcapy analysis for a specific currency pair
    - aggregated_page - for more aggregated style analysis across multiple currency pairs and over multiple time periods

    """

    def __init__(self, url_prefix=''):
        super(LayoutImplGen, self).__init__(url_prefix=url_prefix)

        available_dates = pd.date_range(
            datetime.datetime.today().date() - timedelta(days=constants.gui_lookback_window),
            datetime.datetime.today().date(), freq=BDay())

        times = pd.date_range("0:00", "23:59", freq="15min")

        ### create the possible values for drop down boxes on both pages

        # Reverse date list (for both detailed and aggregated pages)
        self.available_dates = [x.date() for x in available_dates[::-1]]

        # For detailed page only
        self.available_times = [t.strftime("%H:%M") for t in times]

        self.available_tickers = constants.available_tickers_dictionary['All']
        self.available_venues = constants.available_venues_dictionary['All']

        self.available_brokers = constants.available_brokers_dictionary['All']
        self.available_algos = constants.available_algos_dictionary['All']
        self.available_market_data = constants.available_market_data

        self.available_order_plot_lines = ['candlestick', 'mid', 'bid', 'ask', 'arrival', 'twap', 'vwap',
                                           'buy trade', 'sell trade']
        self.available_execution_plot_lines = ['candlestick', 'mid', 'bid', 'ask', 'buy trade', 'sell trade']

        self.available_slippage_bounds = ['0.25', '0.5', '1.0', '1.25', '1.5', '2.0', 'bid/ask']

        # For aggregated page only
        self.available_grouped_tickers = self._flatten_dictionary(constants.available_tickers_dictionary)
        self.available_grouped_venues = self._flatten_dictionary(constants.available_venues_dictionary)
        self.available_grouped_brokers = self._flatten_dictionary(constants.available_brokers_dictionary)
        self.available_grouped_algos = self._flatten_dictionary(constants.available_algos_dictionary)

        self.available_event_types = constants.available_event_types
        self.available_metrics = constants.available_metrics

        self.available_reload = ['no', 'yes']
        self.available_visualization = ['yes', 'no']

        # For local images we need to encode as binary first for Dash output
        self.image_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logo.png')
        self.encoded_image = None

        try:
            self.encoded_image = base64.b64encode(open(self.image_filename, 'rb').read())
        except:
            pass

        self.create_layouts()

    def _flatten_dictionary(self, dictionary):
        available = dictionary['All']
        available_groups = self._util_func.dict_key_list(dictionary.keys())

        return self.flatten_list_of_strings([available_groups, available])

    def create_layouts(self):
        self.page_content = html.Div([
            dcc.Location(id='url', refresh=False),
            html.Div(id='page-content')
        ])

        link_bar_dict = OrderedDict([('Detailed', 'detailed'), ('Aggregated', 'aggregated'),
                                     ('Compliance', 'compliance')])

        trade_outliers_cols = ['Date', 'ticker', 'side', 'notional cur', 'benchmark', 'exec not',
                               'exec not in rep cur', 'slippage']

        broker_cols = ['Date', 'by broker notional (rep cur)']

        # Main page for detailed analysing of (eg. over the course of a few days)
        self.pages['detailed'] = html.Div([

            self.header_bar('FX: Detailed - Trader Analysis'),
            self.link_bar(link_bar_dict),
            self.width_cel(html.B("Status: ok", id='detailed-status'), margin_left=5),
            self.horizontal_bar(),

            # Dropdown selection boxes
            html.Div([
                self.drop_down(caption='Start Date', id=OrderedDict([('start-date-val', self.available_dates),
                                                                     ('start-time-val', self.available_times)]),
                               prefix_id='detailed'),
                self.drop_down(caption='Finish Date', id=OrderedDict([('finish-date-val', self.available_dates),
                                                                      ('finish-time-val', self.available_times)]),
                               prefix_id='detailed'),
                self.drop_down(caption='Ticker', id='ticker-val', prefix_id='detailed',
                               drop_down_values=self.available_tickers),
                self.drop_down(caption='Broker', id='broker-val', prefix_id='detailed',
                               drop_down_values=self.available_grouped_brokers),
                self.drop_down(caption='Algo', id='algo-val', prefix_id='detailed',
                               drop_down_values=self.available_grouped_algos),
                self.drop_down(caption='Venue', id='venue-val', prefix_id='detailed',
                               drop_down_values=self.available_grouped_venues),
                self.drop_down(caption='Market Data', id='market-data-val', prefix_id='detailed',
                               drop_down_values=self.available_market_data),
                self.drop_down(caption='Metric', id='metric-val', prefix_id='detailed',
                               drop_down_values=self.available_metrics)
            ]),

            self.horizontal_bar(),
            self.button(caption='Calculate', id='calculation-button', prefix_id='detailed'),
            # self.button(caption = 'Print PDF', id = 'detailed-print-pdf-button', className = 'no-print'),

            # Orders
            self.horizontal_bar(),
            self.plot(caption='Orders: Timeline', id='order-candle-timeline-plot', prefix_id='detailed',
                      element_add=self.timeline_dropdown('detailed-order-candle-timeline-plot',
                                                         self.available_order_plot_lines),
                      downloadplot_caption='Download CSV',
                      downloadplot_tag='order-candle-timeline-download-link',
                      download_file='download_order_candle_timeline'),
            self.plot(caption='Orders: Markout', id='order-markout-plot', prefix_id='detailed'),
            self.plot(caption='Orders: Histogram vs PDF fit', id='order-dist-plot', prefix_id='detailed'),

            # Execution trades
            self.horizontal_bar(),
            self.plot(caption='Executions: Timeline', id='execution-candle-timeline-plot', prefix_id='detailed',
                      element_add=self.timeline_dropdown('detailed-execution-candle-timeline-plot',
                                                         self.available_execution_plot_lines),
                      downloadplot_caption='Download CSV',
                      downloadplot_tag='execution-candle-timeline-download-link',
                      download_file='download_execution_candle_timeline.csv'),
            self.plot(caption='Executions: Markout', id='execution-markout-plot', prefix_id='detailed'),
            self.plot(caption='Executions: Histogram vs PDF fit', id='execution-dist-plot', prefix_id='detailed'),

            # Detailed tcapy markout table for executions
            html.Div([
                html.H3('Executions: Markout Table'),
                html.Div(id='detailed-execution-table')
            ],
                style={'width': '1000px', 'display': 'inline-block', 'marginBottom': 5, 'marginTop': 5, 'marginLeft': 5,
                       'marginRight': 5}),
        ],
            style={'width': '1000px', 'marginRight': 'auto', 'marginLeft': 'auto'})

        ################################################################################################################

        # Secondary page for analysing aggregated statistics over long periods of time, eg. who is the best broker?
        self.pages['aggregated'] = html.Div([

            self.header_bar('FX: Aggregated - Trader Analysis'),
            self.link_bar(link_bar_dict),
            self.width_cel(html.B("Status: ok", id='aggregated-status'), margin_left=5),
            self.horizontal_bar(),

            # dropdown selection boxes
            html.Div([
                self.drop_down(caption='Start Date', id='start-date-val', prefix_id='aggregated',
                               drop_down_values=self.available_dates),
                self.drop_down(caption='Finish Date', id='finish-date-val', prefix_id='aggregated',
                               drop_down_values=self.available_dates),
                self.drop_down(caption='Ticker', id='ticker-val', prefix_id='aggregated',
                               drop_down_values=self.available_grouped_tickers, multiselect=True),
                self.drop_down(caption='Broker', id='broker-val', prefix_id='aggregated',
                               drop_down_values=self.available_grouped_brokers, multiselect=True),
                self.drop_down(caption='Algo', id='algo-val', prefix_id='aggregated',
                               drop_down_values=self.available_grouped_algos, multiselect=True),
                self.drop_down(caption='Venue', id='venue-val', prefix_id='aggregated',
                               drop_down_values=self.available_grouped_venues, multiselect=True),
                self.drop_down(caption='Reload', id='reload-val', prefix_id='aggregated',
                               drop_down_values=self.available_reload),
                self.drop_down(caption='Market Data', id='market-data-val', prefix_id='aggregated',
                               drop_down_values=self.available_market_data),
                self.drop_down(caption='Event Type', id='event-type-val', prefix_id='aggregated',
                               drop_down_values=self.available_event_types),
                self.drop_down(caption='Metric', id='metric-val', prefix_id='aggregated',
                               drop_down_values=self.available_metrics),
            ]),

            self.horizontal_bar(),
            self.button(caption='Calculate', id='calculation-button', prefix_id='aggregated'),
            # , msg_id='aggregated-status'),
            self.horizontal_bar(),

            # self.date_picker_range(caption='Start/Finish Dates', id='aggregated-date-val', offset=[-7,-1]),
            self.plot(caption='Aggregated Trader: Summary',
                      id=['execution-by-ticker-bar-plot', 'execution-by-venue-bar-plot'], prefix_id='aggregated'),
            self.horizontal_bar(),
            self.plot(caption='Aggregated Trader: Timeline', id='execution-by-ticker-timeline-plot',
                      prefix_id='aggregated'),
            self.horizontal_bar(),
            self.plot(caption='Aggregated Trader: PDF fit (' + constants.reporting_currency + ' notional)', id=['execution-by-ticker-dist-plot',
                                                                'execution-by-venue-dist-plot'],
                      prefix_id='aggregated'),
            self.horizontal_bar()
        ],
            style={'width': '1000px', 'marginRight': 'auto', 'marginLeft': 'auto'})

        ################################################################################################################

        self.pages['compliance'] = html.Div([

            self.header_bar('FX: Compliance Analysis'),
            self.link_bar(link_bar_dict),
            self.width_cel(html.B("Status: ok", id='compliance-status'), margin_left=5),
            self.horizontal_bar(),

            # Dropdown selection boxes
            html.Div([
                self.drop_down(caption='Start Date', id='start-date-val', prefix_id='compliance',
                              drop_down_values=self.available_dates),
                self.drop_down(caption='Finish Date', id='finish-date-val', prefix_id='compliance',
                              drop_down_values=self.available_dates),
                self.drop_down(caption='Ticker', id='ticker-val', prefix_id='compliance',
                               drop_down_values=self.available_grouped_tickers, multiselect=True),
                self.drop_down(caption='Broker', id='broker-val', prefix_id='compliance',
                               drop_down_values=self.available_grouped_brokers, multiselect=True),
                self.drop_down(caption='Algo', id='algo-val', prefix_id='compliance',
                               drop_down_values=self.available_grouped_algos, multiselect=True),
                self.drop_down(caption='Venue', id='venue-val', prefix_id='compliance',
                               drop_down_values=self.available_grouped_venues, multiselect=True),
                self.drop_down(caption='Reload', id='reload-val', prefix_id='compliance',
                               drop_down_values=self.available_reload),
                self.drop_down(caption='Market Data', id='market-data-val', prefix_id='compliance',
                               drop_down_values=self.available_market_data),
                self.drop_down(caption='Filter by Time', id='filter-time-of-day-val', prefix_id='compliance',
                               drop_down_values=self.available_reload),
                self.drop_down(caption='Start Time of Day', id='start-time-of-day-val', prefix_id='compliance',
                               drop_down_values=self.available_times),
                self.drop_down(caption='Finish Time of Day', id='finish-time-of-day-val', prefix_id='compliance',
                               drop_down_values=self.available_times),
                self.drop_down(caption='Slippage to Mid (bp)', id='slippage-bounds-val', prefix_id='compliance',
                               drop_down_values=self.available_slippage_bounds),
                self.drop_down(caption='Visualization', id='visualization-val', prefix_id='compliance',
                               drop_down_values=self.available_visualization)
            ]),

            self.horizontal_bar(),

            html.Div([
                self.button(caption='Calculate', id='calculation-button', prefix_id='compliance'),
                # self.date_picker(caption='Start Date', id='start-date-dtpicker', prefix_id='compliance'),
                # self.date_picker(caption='Finish Date', id='finish-date-dtpicker', prefix_id='compliance'),
            ]),

            self.horizontal_bar(),
            self.table(caption='Compliance: Trade Outliers', id='execution-by-anomalous-table', prefix_id='compliance',
                       columns=trade_outliers_cols,
                       downloadplot_caption='Trade outliers CSV',
                       downloadplot_tag='execution-by-anomalous-download-link',
                       download_file='download_execution_by_anomalous.csv'),

            self.table(caption='Compliance: Totals by Broker', id='summary-by-broker-table', prefix_id='compliance',
                       columns=broker_cols,
                       downloadplot_caption='Download broker CSV',
                       downloadplot_tag='summary-by-broker-download-link',
                       download_file='download_broker.csv'
                       ),

            self.horizontal_bar()
        ],
            style={'width': '1000px', 'marginRight': 'auto', 'marginLeft': 'auto'})

        # ID flags
        self.id_flags = {
            # Detailed trader page
            # 'timeline_trade_orders' : {'client-orders': 'order', 'executions': 'trade'},
            # 'markout_trade_orders' : {'client-orders': 'order_df', 'executions': 'trade_df'},
            'detailed_candle_timeline_trade_order': {'execution': 'sparse_market_trade_df',
                                                     'order': 'sparse_market_order_df'},
            'detailed_markout_trade_order': {'execution': 'trade_df', 'order': 'order_df'},
            'detailed_table_trade_order': {'execution': 'table_trade_df_markout_by_all'},
            'detailed_dist_trade_order': {'execution': 'dist_trade_df_by/pdf/side', 'order': 'dist_order_df_by/pdf/side'},
            'detailed_download_link_trade_order': {'execution-candle-timeline': 'sparse_market_trade_df',
                                                   'order-candle-timeline': 'sparse_market_order_df'},

            # Aggregated trader page
            'aggregated_bar_trade_order': {'execution-by-ticker': 'bar_trade_df_by/mean/ticker',
                                           'execution-by-venue': 'bar_trade_df_by/mean/venue'},
            'aggregated_timeline_trade_order': {'execution-by-ticker': 'timeline_trade_df_by/mean_date/ticker',
                                                'execution-by-venue': 'timeline_trade_df_by/mean_date/venue'},
            'aggregated_dist_trade_order': {'execution-by-ticker': 'dist_trade_df_by/pdf/ticker',
                                            'execution-by-venue': 'dist_trade_df_by/pdf/venue'},

            # Compliance page
            'compliance_metric_table_trade_order':
                {'execution-by-anomalous': 'table_trade_df_slippage_by_worst_all',
                 'summary-by-broker': 'bar_trade_df_executed_notional_in_reporting_currency_by_broker_id'},

            'compliance_download_link_trade_order':
                {'execution-by-anomalous': 'table_trade_df_slippage_by_worst_all',
                 'summary-by-broker': 'bar_trade_df_executed_notional_in_reporting_currency_by_broker_id'},
        }
