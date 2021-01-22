from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import time

import abc
import dash

from collections import OrderedDict

from tcapy.analysis.tcarequest import TCARequest
from tcapy.analysis.algos.metric import *

from tcapy.analysis.algos.resultsform import *

from tcapy.data.databasesource import DatabaseSourceCSVBinary

from tcapy.util.fxconv import FXConv

from tcapy.util.utilfunc import UtilFunc
from tcapy.util.loggermanager import LoggerManager

from tcapy.vis.tcacaller import TCACaller

ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

constants = Constants()

class TCACallerImplBoardGen(TCACaller):

    def __init__(self, app, session_manager, callback_manager, glob_volatile_cache, layout, callback_dict=None):
        super(TCACallerImplBoardGen, self).__init__(app, session_manager, callback_manager, glob_volatile_cache, layout,
                                                    callback_dict=callback_dict)

        self._util_func = UtilFunc()

    def calculate_computation_summary(self, tca_type, external_params=None):

        def callback(*args):
            """Calculates the aggregated TCA computation when the "Calculate" button is clicked. Cached the results and
            then updates the status label when done.

            Parameters
            ----------
            ticker_val : str(list)
                _tickers (eg. EURUSD, GBPUSD etc)

            venue_val : str(list)
                Trading venues

            start_date_val : str(list)
                Start date of TCA _calculations

            finish_date_val : str(list)
                Finish date of TCA _calculations

            reload_val : str
                Whether underlying market and trade data should be reloaded from dataframe or fetched from cache

            n_clicks : int
                Number of time button has been clicked

            Returns
            -------
            str
            """
            start = time.time()

            tag = tca_type + '-calculation-button'

            logger = LoggerManager.getLogger(__name__)
            logger.debug('Triggered click ' + tca_type)

            old_clicks = self._session_manager.get_session_clicks(tag)

            # Make sure none of the other charts are plotted till we have completed this!
            self._session_manager.set_session_flag(
                [self._plot_flags['aggregated']], False)

            if tca_type == 'aggregated':

                uploadbox, market_data_val, n_clicks = args

                # Catch cases where users repeatedly click, which can cause misalignment in clicks
                self._session_manager.set_session_clicks(tag, n_clicks, old_clicks=old_clicks)

                logger.debug(self.create_generate_button_msg(old_clicks, n_clicks))

                if uploadbox is not None and market_data_val != '' and n_clicks > old_clicks:
                    # Assume that the user uploaded a binary CSV file
                    trade_df = DatabaseSourceCSVBinary(trade_data_database_csv=uploadbox).fetch_trade_order_data()

                    data_frame_trade_order_mapping = {'trade_df' : trade_df}

                    start_date = trade_df.index[0];
                    finish_date = trade_df.index[-1]

                    ticker_val = FXConv().correct_unique_notation_list(trade_df['ticker'].unique().tolist())

                    metric_val = 'slippage'

                    self._session_manager.set_session_flag('metric', value=metric_val)
                    self._session_manager.set_session_flag('aggregated-visualization', True)

                    try:
                    #if True:

                        # clear the cache for the current user
                        self._glob_volatile_cache.clear_key_match(self._session_manager.get_session_id())

                        results_form = [
                            # show the distribution of the selected metric for trades weighted by notional
                            # aggregated by ticker and then by venue
                            DistResultsForm(market_trade_order_list=['trade_df'], metric_name=metric_val,
                                            aggregate_by_field=['ticker', 'broker_id', 'venue'],
                                            weighting_field='executed_notional_in_reporting_currency'),

                            # display the timeline of metrics average by day (and weighted by notional)
                            TimelineResultsForm(market_trade_order_list=['trade_df'], by_date='date',
                                                metric_name=metric_val,
                                                aggregation_metric='mean',
                                                aggregate_by_field=['ticker'], scalar=10000.0,
                                                weighting_field='executed_notional_in_reporting_currency'),

                            # display a bar chart showing the average metric weighted by notional and aggregated by ticker
                            # venue
                            BarResultsForm(market_trade_order_list=['trade_df'],
                                           metric_name=metric_val,
                                           aggregation_metric='mean',
                                           aggregate_by_field=['ticker', 'venue', 'broker_id'], scalar=10000.0,
                                           weighting_field='executed_notional_in_reporting_currency'),

                            # create a table the markout of every trade
                            TableResultsForm(market_trade_order_list=['trade_df'], metric_name='markout', filter_by='all',
                                             replace_text={'markout_': '', 'executed_notional': 'exec not',
                                                           'notional_currency': 'exec not cur'},
                                             keep_fields=['executed_notional', 'side', 'notional_currency'],
                                             scalar={'all': 10000.0, 'exclude': ['executed_notional', 'side']},
                                             round_figures_by={'all': 2, 'executed_notional': 0, 'side': 0},
                                             weighting_field='executed_notional')
                        ]

                        try:
                        #if True:
                            timeline_trade_df_metric_by_ticker = self.get_cached_computation_analysis(
                                key='timeline_trade_df_' + metric_val + '_by/mean_date/ticker',
                                tca_engine=self._tca_engine,
                                force_calculate=True,

                                tca_request=
                                    TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker_val,
                                               tca_type='aggregated',
                                               market_data_store=market_data_val, trade_data_store='dataframe',
                                               trade_order_mapping=data_frame_trade_order_mapping,
                                               metric_calcs=[MetricSlippage(), MetricMarkout(trade_order_list=['trade_df'])],
                                               results_form=results_form, dummy_market=True, use_multithreading=True)
                            )

                            calc_start = timeline_trade_df_metric_by_ticker.index[0]
                            calc_end = timeline_trade_df_metric_by_ticker.index[-1]

                            aggregated_title = self.create_status_msg_flags('aggregated', ticker_val, calc_start, calc_end)

                            logger.debug('Plotted aggregated summary plot!')

                            finish = time.time()

                        except Exception as e:
                            logger.exception(e)

                            return "Status: error - " + str(e) + ". Check data exists for these dates?" + self.get_username_string()

                    except Exception as e:
                        logger.exception(e)

                        return 'Status: error - ' + str(e) + ". Check data exists for these dates?" + self.get_username_string()

                    return 'Status: calculated ' + str(
                        round(finish - start, 3)) + "s for " + aggregated_title + self.get_username_string()

            raise dash.exceptions.PreventUpdate("No data changed")  # not very elegant but only way to prevent plots disappearing

            # return 'Status: ok'

        if external_params is not None:
            return callback(**external_params)

        return callback
