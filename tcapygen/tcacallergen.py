from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#
# This may not be distributed without the permission of Cuemacro.
#

import time
import abc

import pandas as pd
import dash

from tcapy.analysis.algos.metric import *
from tcapy.analysis.algos.benchmark import *
from tcapy.analysis.algos.resultsform import *

from tcapy.util.utilfunc import UtilFunc
from tcapy.util.loggermanager import LoggerManager

from tcapy.vis.tcacaller import TCACaller

ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

constants = Constants()

class TCACallerImplGen(TCACaller):

    def __init__(self, app, session_manager, callback_manager, glob_volatile_cache, layout):
        super(TCACallerImplGen, self).__init__(app, session_manager, callback_manager, glob_volatile_cache, layout)

        self._util_func = UtilFunc()

    def calculate_computation_summary(self, tca_type, external_params=None):

        # callback triggered by Dash application
        def callback(*args):
            """Kicks off fetching of data of market data and TCA _calculations for a specific currency pair. Caches the data
            in a VolatileCache instance, ready to be read in by the other charts.

            Parameters
            ----------
            ticker_val : str
                ticker to be used in TCA _calculations

            start_date_val : str
                Start date of TCA analysis

            start_time_val : str
                Start time of TCA analysis

            finish_date_val : str
                Finish date of TCA analysis

            finish_time_val : str
                Finish time of TCA analysis

            venue_val : str
                Venue data to be used

            n_clicks : int
                Number of clicks

            Returns
            -------
            str
            """
            start = time.time()

            tag = tca_type + '-calculation-button'

            old_clicks = self._session_manager.get_session_clicks(tag)

            # Make sure none of the other charts/links are plotted till we have completed this!
            self._session_manager.set_session_flag(
                [self._plot_flags['aggregated'], self._plot_flags['detailed'], self._plot_flags['compliance']], False)
            
            logger = LoggerManager.getLogger(__name__)

            if tca_type == 'detailed':
                ticker_val, start_date_val, start_time_val, finish_date_val, finish_time_val, \
                broker_val, algo_val, venue_val, market_data_val, metric_val, n_clicks = args

                # Catch cases where users repeatedly click, which can cause misalignment in clicks
                self._session_manager.set_session_clicks(tag, n_clicks, old_clicks=old_clicks)

                logger.debug(self.create_generate_button_msg(old_clicks, n_clicks))

                # Make sure all the parameters have been selected
                if ticker_val != '' and venue_val != '' and start_date_val != '' and start_time_val != '' and \
                        finish_date_val != '' and finish_time_val != '' and market_data_val != '' and broker_val != '' and \
                        algo_val != '' and n_clicks > old_clicks:

                    # Expand _tickers/broker fields etc, in case for example 'All' has been specified or any other groups
                    broker_val = self._util_func.populate_field(broker_val, constants.available_brokers_dictionary, exception_fields='All')
                    algo_val = self._util_func.populate_field(algo_val, constants.available_algos_dictionary, exception_fields='All')
                    venue_val = self._util_func.populate_field(venue_val, constants.available_venues_dictionary, exception_fields='All')

                    # Combine the start date/time and finish date/time
                    start_date_val = start_date_val + ' ' + start_time_val
                    finish_date_val = finish_date_val + ' ' + finish_time_val

                    metric_val = metric_val.replace(' ', '_')

                    logger.debug('Calculation click old: ' + str(old_clicks) + " clicks vs new " + str(n_clicks))

                    self._session_manager.set_session_clicks(tag, n_clicks)
                    self._session_manager.set_session_flag('metric', value=metric_val)

                    self._session_manager.set_session_flag('detailed-visualization', value=True)

                    logger.info('Selected ' + ticker_val + " " + start_date_val + " - " + finish_date_val)

                    # Check that dates are less than 1 month apart
                    if pd.Timestamp(finish_date_val) - pd.Timestamp(start_date_val) > pd.Timedelta(
                            days=constants.max_plot_days):
                        return "Status: Cannot plot more than " + str(constants.max_plot_days) + " days!"
                    elif pd.Timestamp(start_date_val) >= pd.Timestamp(finish_date_val):
                        return "Status: Start date must be before the end date"

                    try:
                    #if True:

                        # Clear the cache for the current user
                        self._glob_volatile_cache.clear_key_match(self._session_manager.get_session_id())

                        results_form = [
                            # Calculate the distribute of the metric for trades/orders, broken down by trade side (buy/sell)
                            DistResultsForm(market_trade_order_list=['trade_df', 'order_df'],
                                            metric_name=metric_val, aggregate_by_field='side', scalar=10000.0,
                                            weighting_field='executed_notional_in_reporting_currency'),

                            # Create a table the markout of every trade
                            TableResultsForm(market_trade_order_list=['trade_df'], metric_name='markout', filter_by='all',
                                             replace_text={'markout_': '', 'executed_notional': 'exec not',
                                                           'notional_currency': 'exec not cur'},
                                             keep_fields=['executed_notional', 'side', 'notional_currency'],
                                             scalar={'all': 10000.0, 'exclude': ['executed_notional', 'side']},
                                             round_figures_by={'all': 2, 'executed_notional': 0, 'side': 0},
                                             weighting_field='executed_notional')]

                        benchmark_calcs = [
                            # Calculate the arrival prices for every trade/order
                            BenchmarkArrival(trade_order_list=['trade_df', 'order_df']),

                            # Calculate the VWAP for each order
                            BenchmarkVWAP(trade_order_list=['order_df']),

                            # Calculate the TWAP for each order
                            BenchmarkTWAP(trade_order_list=['order_df'])]

                        metric_calcs = [metric_val, MetricMarkout(trade_order_list=['trade_df'])]

                        # Get from cache, note given that we are in the first part of the chain we should force it to calculate!
                        sparse_market_trade_df = self.get_cached_computation_analysis(key='sparse_market_trade_df',
                                                                                      start_date=start_date_val,
                                                                                      finish_date=finish_date_val,
                                                                                      ticker=ticker_val,
                                                                                      venue=venue_val, market_data=market_data_val,
                                                                                      event_type='trade',
                                                                                      dummy_market=False,
                                                                                      broker=broker_val, algo=algo_val,
                                                                                      metric_calcs=metric_calcs,
                                                                                      metric_trade_order_list=['trade_df',
                                                                                                       'order_df'],
                                                                                      benchmark_calcs=benchmark_calcs,
                                                                                      tca_type='detailed',
                                                                                      tca_engine=self._tca_engine,
                                                                                      results_form=results_form,
                                                                                      force_calculate=True)

                        calc_start = sparse_market_trade_df.index[0]
                        calc_end = sparse_market_trade_df.index[-1]

                        detailed_title = self.create_status_msg_flags('detailed', ticker_val, calc_start, calc_end)

                    except Exception as e:
                         LoggerManager().getLogger(__name__).exception(e)

                         return "Status: error " + str(e) + ". Check dates?"

                    finish = time.time()

                    return 'Status: calculated ' + str(round(finish - start, 3)) + "s for " + detailed_title

            elif tca_type == 'aggregated':
                ticker_val, start_date_val, finish_date_val, broker_val, algo_val, venue_val, reload_val, market_data_val, \
                event_type_val, metric_val, n_clicks = args

                # Catch cases where users repeatedly click, which can cause misalignment in clicks
                self._session_manager.set_session_clicks(tag, n_clicks, old_clicks=old_clicks)

                logger.debug(self.create_generate_button_msg(old_clicks, n_clicks))

                if ticker_val != '' and start_date_val != '' and venue_val != '' \
                        and finish_date_val != '' and reload_val != '' and event_type_val != '' and metric_val != '' and \
                        n_clicks > old_clicks:

                    # Expand _tickers/broker fields etc, in case for example 'All' has been specified or any other groups
                    ticker_val_list = self._util_func.populate_field(ticker_val, constants.available_tickers_dictionary)
                    broker_val_list = self._util_func.populate_field(broker_val, constants.available_brokers_dictionary)
                    algo_val_list = self._util_func.populate_field(algo_val, constants.available_algos_dictionary)
                    venue_val_list = self._util_func.populate_field(venue_val, constants.available_venues_dictionary)

                    metric_val = metric_val.replace(' ', '_')

                    logger.debug('Calculation click old: ' + str(old_clicks) + " clicks vs new " + str(n_clicks))

                    self._session_manager.set_session_clicks(tag, n_clicks)
                    self._session_manager.set_session_flag('metric', value=metric_val)

                    self._session_manager.set_session_flag('aggregated-visualization', True)

                    try:
                        # if True:

                        # Clear the cache for the current user
                        self._glob_volatile_cache.clear_key_match(self._session_manager.get_session_id())

                        results_form = [
                            # Show the distribution of the selected metric for trades weighted by notional
                            # aggregated by ticker and then by venue
                            DistResultsForm(market_trade_order_list=['trade_df'], metric_name=metric_val,
                                            aggregate_by_field=['ticker', 'venue'],
                                            weighting_field='executed_notional_in_reporting_currency'),

                            # Display the timeline of metrics average by day (and weighted by notional)
                            TimelineResultsForm(market_trade_order_list=['trade_df'], by_date='date',
                                                metric_name=metric_val,
                                                aggregation_metric='mean',
                                                aggregate_by_field='ticker', scalar=10000.0,
                                                weighting_field='executed_notional_in_reporting_currency'),

                            # Display a bar chart showing the average metric weighted by notional and aggregated by ticker
                            # venue
                            BarResultsForm(market_trade_order_list=['trade_df'],
                                           metric_name=metric_val,
                                           aggregation_metric='mean',
                                           aggregate_by_field=['ticker', 'venue'], scalar=10000.0,
                                           weighting_field='executed_notional_in_reporting_currency')
                        ]

                        try:
                            # if True:
                            timeline_trade_df_metric_by_ticker = self.get_cached_computation_analysis(
                                key='timeline_trade_df_' + metric_val + '_by/mean_date/ticker',
                                start_date=start_date_val,
                                finish_date=finish_date_val,
                                event_type=event_type_val,
                                ticker=ticker_val_list,
                                broker=broker_val_list, algo=algo_val_list,
                                venue=venue_val_list, market_data=market_data_val,
                                dummy_market=True,
                                tca_engine=self._tca_engine,
                                tca_type='aggregated',
                                metric_calcs=metric_val,
                                metric_trade_order_list=['trade_df'],
                                results_form=results_form,
                                force_calculate=True, reload_val=reload_val, trade_order_mapping=['trade_df'])

                            calc_start = timeline_trade_df_metric_by_ticker.index[0]
                            calc_end = timeline_trade_df_metric_by_ticker.index[-1]

                            aggregated_title = self.create_status_msg_flags('aggregated', ticker_val, calc_start,
                                                                            calc_end)

                            logger.debug('Plotted aggregated summary plot!')

                            finish = time.time()

                        except Exception as e:
                            LoggerManager().getLogger(__name__).exception(e)

                            return "Status: error - " + str(e) + ". Check data exists for these dates?"

                    except Exception as e:
                        LoggerManager().getLogger(__name__).exception(e)

                        return 'Status: error - ' + str(e) + ". Check data exists for these dates?"

                    return 'Status: calculated ' + str(
                        round(finish - start, 3)) + "s for " + aggregated_title

            elif tca_type == 'compliance':
                ticker_val, start_date_val, finish_date_val, broker_val, algo_val, venue_val, reload_val, market_data_val, \
                filter_time_of_day_val, start_time_of_day_val, finish_time_of_day_val, slippage_bounds_val, visualization_val, n_clicks = args

                # Catch cases where users repeatedly click, which can cause misalignment in clicks
                self._session_manager.set_session_clicks(tag, n_clicks, old_clicks=old_clicks)

                logger.debug(self.create_generate_button_msg(old_clicks, n_clicks))

                if ticker_val != '' and start_date_val != '' and broker_val != '' and algo_val != '' and venue_val != '' \
                        and finish_date_val != '' and reload_val != '' and filter_time_of_day_val != '' \
                        and start_time_of_day_val != '' and finish_time_of_day_val != '' and slippage_bounds_val != '' \
                        and n_clicks > old_clicks:

                    ticker_val_list = self._util_func.populate_field(ticker_val, constants.available_tickers_dictionary)
                    broker_val_list = self._util_func.populate_field(broker_val, constants.available_brokers_dictionary, exception_fields='All')
                    algo_val_list = self._util_func.populate_field(algo_val, constants.available_algos_dictionary, exception_fields='All')
                    venue_val_list = self._util_func.populate_field(venue_val, constants.available_venues_dictionary, exception_fields='All')

                    logger.debug('Calculation click old: ' + str(old_clicks) + " clicks vs new " + str(n_clicks))

                    self._session_manager.set_session_clicks(tag, n_clicks)

                    if visualization_val == 'yes':
                        self._session_manager.set_session_flag('compliance-visualization', True)
                    else:
                        self._session_manager.set_session_flag('compliance-visualization', False)

                    try:
                        # if True:

                        # Clear the cache for the current user
                        self._glob_volatile_cache.clear_key_match(self._session_manager.get_session_id())

                        slippage_bounds = 0.0
                        overwrite_bid_ask = True

                        if slippage_bounds_val == 'bid/ask':
                            overwrite_bid_ask = False
                        else:
                            slippage_bounds = float(slippage_bounds_val)

                        metric_calcs = [
                            # Calculate slippage for trades
                            MetricSlippage(trade_order_list='trade_df'),
                        ]

                        benchmark_calcs = [
                            # Generate the spread to mid for market data (in certain case artificially create a spread)
                            BenchmarkMarketSpreadToMid(bid_mid_bp=slippage_bounds, ask_mid_bp=slippage_bounds,
                                                       overwrite_bid_ask=overwrite_bid_ask)]

                        results_form = [
                            # Display a table of all the anomalous trades by slippage (ie. outside bid/ask)
                            TableResultsForm(
                                # Only display for trades
                                market_trade_order_list=['trade_df'],

                                # Display slippage
                                metric_name='slippage',

                                # Order by the worst slippage
                                filter_by='worst_all',

                                # Replace text on table to make it look nicer
                                replace_text={'markout_': '', 'executed_notional': 'exec not', '_currency': ' cur',
                                              '_in_reporting': ' in rep',
                                              'slippage_benchmark': 'benchmark', 'slippage_anomalous': 'anomalous',
                                              'broker_id': 'broker ID', 'algo_id': 'algo ID',
                                              'executed_price': 'price'},

                                exclude_fields_from_avg=['slippage_anomalous', 'slippage_benchmark', 'side'],

                                # Only select trades outside bid/ask (ie. where slippage anomalous = 1)
                                tag_value_combinations={'slippage_anomalous': 1.0},

                                # Display several columns
                                keep_fields=['ticker', 'broker_id', 'algo_id', 'notional_currency', 'executed_notional',
                                             'executed_notional_in_reporting_currency', 'side', 'executed_price'],

                                # Multiply slippage field by 10000 (to convert into basis points)
                                scalar={'slippage': 10000.0},

                                # Round figures to make them easier to read
                                round_figures_by={'executed_notional': 0, 'executed_notional_in_reporting_currency': 0,
                                                  'side': 0, 'slippage': 2, 'slippage_benchmark': 4}),

                            # Get the total notional executed by broker (in reporting currency)
                            BarResultsForm(
                                # Select child orders
                                market_trade_order_list=['trade_df'],

                                # Aggregate by broker name
                                aggregate_by_field='broker_id',

                                # Select the notional for analysis
                                metric_name='executed_notional_in_reporting_currency',  # analyse notional

                                # Sum all the notionals
                                aggregation_metric='sum',

                                # Round figures
                                round_figures_by=0)
                        ]

                        # Reformat tables for notional by broker
                        join_tables = [
                            # JoinTables(
                            # tables_dict={'table_name': 'jointables_broker_id_df',
                            #
                            #              # fetch the following calculated tables
                            #              'table_list': [
                            #                  'bar_order_df_executed_notional_in_reporting_currency_by_broker_id'],
                            #
                            #              # append to the columns of each table
                            #              'column_list': ['notional (rep cur)'],
                            #              'replace_text': {'broker_id': 'broker ID'}
                            #              })
                        ]

                        try:
                            # if True:
                            trade_df = self.get_cached_computation_analysis(
                                key='trade_df',
                                start_date=start_date_val,
                                finish_date=finish_date_val,
                                start_time_of_day=start_time_of_day_val, finish_time_of_day=finish_time_of_day_val,
                                filter_time_of_day=filter_time_of_day_val,
                                event_type='trade',
                                ticker=ticker_val_list, broker=broker_val_list, algo=algo_val_list,
                                venue=venue_val_list,
                                dummy_market=True,
                                market_data=market_data_val,
                                tca_engine=self._tca_engine,
                                tca_type='compliance',
                                metric_calcs=metric_calcs,
                                benchmark_calcs=benchmark_calcs,
                                metric_trade_order_list=['trade_df'],
                                results_form=results_form,
                                join_tables=join_tables,
                                force_calculate=True, reload_val=reload_val,
                                trade_order_mapping=['trade_df'])

                            calc_start = trade_df.index[0]
                            calc_end = trade_df.index[-1]

                            compliance_title = self.create_status_msg_flags('compliance', ticker_val, calc_start,
                                                                            calc_end)

                            logger.debug('Generated compliance summary.. awaiting plot callbacks!')

                            finish = time.time()

                        except Exception as e:
                            logger.exception(e)

                            return "Status: error " + str(e) + ". Check data exists for these dates?"

                    except Exception as e:
                        logger.exception(e)

                        return 'Status: error ' + str(e) + ". Check data exists for these dates?"

                    return 'Status: calculated ' + str(
                        round(finish - start, 3)) + "s for " + compliance_title

            raise dash.exceptions.PreventUpdate("No data changed - " + tca_type)  # Not very elegant but only way to prevent plots disappearing
                # return "Status: ok"

        if external_params is not None:
            return callback(**external_params)

        return callback