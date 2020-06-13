from __future__ import division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import pandas as pd

from datetime import timedelta

from tcapy.analysis.tcamarkettradeloader import TCAMarketTradeLoader

from tcapy.analysis.tcarequest import TCARequest
from tcapy.analysis.dataframeholder import DataFrameHolder

from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.customexceptions import *
from tcapy.util.mediator import Mediator

import traceback

constants = Constants()

from tcapy.conf.celery_calls import calculate_metrics_single_ticker_via_celery, \
    get_market_trade_holder_via_celery, get_market_trade_holder_and_calculate_metrics_single_ticker_via_celery
from celery import chord, group

class TCAMarketTradeLoaderImpl(TCAMarketTradeLoader):
    """This allows the parallel fetching of market/trade data across different tickers
    """

    def __init__(self, version=constants.tcapy_version):
        super(TCAMarketTradeLoaderImpl, self).__init__(version=version)

    def _get_market_trade_metrics(self, tca_request_list, dummy_market):
        # volatile_cache = Mediator.get_volatile_cache(version=self._version)

        if tca_request_list is None:
            return {}, DataFrameHolder()

        if len(tca_request_list) == 0:
            return {}, DataFrameHolder()

        # Only attempt to execute in parallel if flag has been enabled
        if tca_request_list[0].use_multithreading:
            market_df_dict, trade_order_results_df_dict = self._parallel_get_market_trade_metrics(
                tca_request_list, dummy_market)
        else:
            # Otherwise run without any use_multithreading
            return super(TCAMarketTradeLoaderImpl, self)._get_market_trade_metrics(tca_request_list, dummy_market)

        return market_df_dict, trade_order_results_df_dict

    def _apply_summary_metrics(self, tca_request_list, trade_order_results_df_dict, market_df_dict):

        trade_order_list = self._util_func.dict_key_list(trade_order_results_df_dict.keys())
        market_list = self._util_func.dict_key_list(market_df_dict.keys())

        if not (isinstance(trade_order_list, list)):
            trade_order_list = [trade_order_list]

        if not (isinstance(market_list, list)):
            market_list = [market_list]

        # First get the market data (for doing bid/ask on distributions) - only does the first ticker!
        market_df = market_df_dict[tca_request_list[0].ticker]

        logger = LoggerManager.getLogger(__name__)
        logger.debug("Constructing results form to summarize analysis...")

        # Calculate user specified aggregate result forms (eg. timelines, distribution etc.) for each trade/order
        # which has been selected
        results_form = tca_request_list[0].results_form
        join_tables = tca_request_list[0].join_tables

        # If dummy market (ie. don't return market data to the user) has been specified then market data cannot
        # be included in ResultsForm calculations
        if results_form is not None:

            # Go through all the trade/orders doing statistical aggregations
            for i in range(0, len(trade_order_results_df_dict)):

                # Ignore 'fig' objects which are Plotly JSON Figures, and only process DataFrames
                if 'df' in trade_order_list[i]:
                    for r in results_form:

                        # Filter the trades for the event type which has been requested (eg. 'trade' or 'placement')
                        trade_order_df = self._trade_order_tag.filter_trade_order(
                            trade_order_df=trade_order_results_df_dict[trade_order_list[i]],
                            tag_value_combinations={'event_type': tca_request_list[0].event_type})

                        # Calculate aggregate ResultForm
                        results = r.aggregate_results(
                            market_trade_order_df=trade_order_df, market_df=market_df, market_trade_order_name=trade_order_list[i])

                        if results[0] is not None:
                            for results_form_df, results_form_name in results:
                                trade_order_results_df_dict[results_form_name] = results_form_df

            # Go through all the market data doing statistical aggregations
            for i in range(0, len(market_df_dict)):

                # Ignore 'fig' objects which are Plotly JSON Figures, and only process DataFrames which are not empty
                if 'fig' not in market_list[i] and market_df_dict[market_list[i]] is not None:
                    if not(market_df_dict[market_list[i]].empty):
                        for r in results_form:

                            # Calculate aggregate ResultForm
                            results = r.aggregate_results(
                                market_trade_order_df=market_df_dict[market_list[i]],
                                market_df=market_df_dict[market_list[i]], market_trade_order_name=market_list[i])

                            if results[0] is not None:
                                for results_form_df, results_form_name in results:
                                    trade_order_results_df_dict[results_form_name] = results_form_df

        logger.debug("Now join table results...")

        # As a final stage, join together any tables which have been specified by the user
        # for example: does the user want to combine certain metrics or trades together?
        if join_tables is not None:
            for j in join_tables:
                results = j.aggregate_tables(df_dict=trade_order_results_df_dict)

                if results != []:
                    if results[0] is not None:
                        for results_form_df, results_form_name in results:
                            trade_order_results_df_dict[results_form_name] = results_form_df

        logger.debug("Finished calculating results form and join table results!")

        return trade_order_results_df_dict

    def _parallel_get_market_trade_metrics(self, tca_request_list, dummy_market):
        logger = LoggerManager.getLogger(__name__)

        market_holder_list = DataFrameHolder()
        trade_order_holder_list = DataFrameHolder()

        # For each currency pair select collect the trades and market data, then calculate benchmarks and slippage
        result = []

        keep_looping = True

        # If we have also asked for trades/order
        if tca_request_list[0].trade_order_mapping is not None:
            point_in_time_executions_only = \
                self._util_func.dict_key_list(tca_request_list[0].trade_order_mapping) == ['trade_df']
        else:
            point_in_time_executions_only = True

        parallel_library = tca_request_list[0].multithreading_params['parallel_library']

        if parallel_library == 'single':
            # from tcapy.analysis.tcatickerloaderimpl import TCATickerLoaderImpl
            tca_ticker_loader = Mediator.get_tca_ticker_loader(version=self._version)

        start_date = tca_request_list[0].start_date
        finish_date = tca_request_list[0].finish_date

        # Parameters for the loop
        i = 0; no_of_tries = 5

        # Error trapping for Celery, if have failed event retry it
        while i < no_of_tries and keep_looping:

            try:
                # For each TCA request kick off a thread
                for tca_request_single_ticker in tca_request_list:

                    # Split up the request by date (monthly/weekly chunks)
                    tca_request_date_split = self._split_tca_request_by_date(
                        tca_request_single_ticker, tca_request_single_ticker.ticker,
                        period=tca_request_single_ticker.multithreading_params['cache_period'])

                    if not(constants.multithreading_params['splice_request_by_dates']) \
                                or tca_request_list[0].tca_type == 'detailed' \
                                or tca_request_list[0].tca_type == 'compliance' \
                                or tca_request_list[0].summary_display == 'candlestick'\
                                or not(point_in_time_executions_only):

                        if 'celery' in parallel_library:
                            # Load all the data for this ticker and THEN calculate the metrics on it
                            result.append(chord((get_market_trade_holder_via_celery.s(tca_request_data)
                                                 for tca_request_data in tca_request_date_split),
                                                calculate_metrics_single_ticker_via_celery.s(tca_request_single_ticker,
                                                                                             dummy_market)).apply_async())
                        elif parallel_library == 'single':
                            # This is not actually parallel, but is mainly for debugging purposes
                            for tca_request_s in tca_request_date_split:

                                # print(tca_request_s.start_date)
                                market_df, trade_order_df_dict = tca_ticker_loader.get_market_trade_order_holder(
                                    tca_request_s, return_cache_handles=False)

                                market_df, trade_order_df_list, ticker, trade_order_keys = \
                                    tca_ticker_loader.calculate_metrics_single_ticker((market_df, trade_order_df_dict),
                                                                                        tca_request_s, dummy_market)

                                market_holder_list.add_dataframe(market_df, ticker)

                                trade_order_holder_list.add_dataframe_dict(
                                    dict(zip(trade_order_keys, trade_order_df_list)))


                    else:
                        # Otherwise work on parallel chunks by date
                        # doesn't currently work with orders which straddle day/week/month boundaries
                        # but should work with points in time
                        #
                        # In practice, it's not really much faster than the above code
                        if 'celery' == parallel_library:

                            # For each ticker/date combination load data and process chunk (so can do fully in parallel)
                            result.append(group(get_market_trade_holder_and_calculate_metrics_single_ticker_via_celery.s(
                                         tca_request_data,
                                         dummy_market) for tca_request_data in tca_request_date_split).apply_async())

                # Now combine the results from the parallel operations, if using celery
                if 'celery' in parallel_library:

                    # Careful, when the output is empty!
                    output = [p.get(timeout=constants.celery_timeout_seconds) for p in result if p is not None]

                    # If pipelined/splice_request_by_dates will have two lists so flatten it into one
                    output = self._util_func.flatten_list_of_lists(output)

                    for market_df, trade_order_df_list, ticker, trade_order_keys in output:
                        market_holder_list.add_dataframe(market_df, ticker)
                        # market_df_dict[ticker] = market_df

                        trade_order_holder_list.add_dataframe_dict(dict(zip(trade_order_keys, trade_order_df_list)))

                    del result
                    del output

                keep_looping = False

            except DateException as e:
                raise e

                keep_looping = False

            except TradeMarketNonOverlapException as e:
                raise e

                keep_looping = False

            except DataMissingException as e:
                raise e

                keep_looping = False

            except ErrorWritingOverlapDataException as e:
                raise e

                keep_looping = False

            # Exception likely related to Celery and possibly lack of communication with Redis message broker
            # or Memcached results backend
            # except Exception as e:
            except Exception as e:
                if i == no_of_tries - 1:
                    err_msg = "Failed with " + parallel_library + " after multiple attempts: " + str(e) + ", " + str(traceback.format_exc())

                    raise Exception(err_msg)

                i = i + 1

                logger.warn("Failed with " + parallel_library + ", trying again for " + str(i) + " time: " + str(e) + ", " + str(traceback.format_exc()))

        logger.debug("Finished parallel computation")

        # Expand out the DataFrame holders into dictionaries of DataFrames
        market_df_dict = market_holder_list.get_combined_dataframe_dict()
        trade_order_results_df_dict = trade_order_holder_list.get_combined_dataframe_dict(start_date=start_date, finish_date=finish_date)

        # TODO add candlestick drawing here for cases when using split threading by date
        trade_order_results_df_dict = self._util_func.remove_keymatch_dict(trade_order_results_df_dict, 'market_df_downsampled')

        return market_df_dict, trade_order_results_df_dict

    def _split_tca_request_by_date(self, tca_request, tick, split_dates=True, period='month'):

        tca_request_list = []

        dates = []

        # Break up dates into day/week/month chunks - our cache works on day/week/month chunks (can specify in constants)
        # Typically day chunks seem optimal
        # Careful to floor dates for midnight for caching purposes
        if split_dates:
            if period == 'month':
                split_dates_freq = 'MS'
            elif period == 'week':
                split_dates_freq = 'W-MON'
            elif period == 'day':
                split_dates_freq = 'D'

            start_date_floored = self._util_func.floor_tick_of_date(tca_request.start_date)
            finish_date_floored = self._util_func.floor_tick_of_date(tca_request.finish_date, add_day=True)

            dates = pd.date_range(start=start_date_floored, end=finish_date_floored,
                                     freq=split_dates_freq).tolist()

        # Add start date and finish date if necessary
        # if len(dates) > 0:
        #     if start_date_floored < dates[0]:
        #         dates.insert(0, start_date_floored)
        #
        #     if finish_date_floored > dates[-1]:
        #         dates.append(finish_date_floored)
        # else:
        #     dates = [start_date_floored, finish_date_floored]

        logger = LoggerManager().getLogger(__name__)

        # If our start/finish date ends up being more than a month
        # eg. Jan 8th - Mar 7th - split into
        # Jan 8th - Jan 31st 23:59:59.999, Feb 1st 00:00:00.000 - Feb 28th 23:59:59.999 etc
        if len(dates) > 0:

            # For the very first chunk in our series
            if tca_request.start_date < dates[0]:
                tca_request_temp = TCARequest(tca_request=tca_request)
                tca_request_temp.ticker = tick
                tca_request_temp.start_date = tca_request.start_date
                tca_request_temp.finish_date = dates[0] - timedelta(microseconds=1)

                tca_request_list.append(tca_request_temp)

            # For full months in between during our request
            for i in range(0, len(dates) - 1):
                tca_request_temp = TCARequest(tca_request=tca_request)
                tca_request_temp.ticker = tick
                tca_request_temp.start_date = dates[i]
                tca_request_temp.finish_date = dates[i + 1] - timedelta(microseconds=1)

                tca_request_list.append(tca_request_temp)

            # For the very last chunk of our series
            if dates[-1] < tca_request.finish_date:
                tca_request_temp = TCARequest(tca_request=tca_request)
                tca_request_temp.ticker = tick
                tca_request_temp.start_date = dates[-1]
                tca_request_temp.finish_date = tca_request.finish_date

                tca_request_list.append(tca_request_temp)
        else:
            tca_request_temp = TCARequest(tca_request=tca_request)
            tca_request_temp.ticker = tick

            tca_request_list.append(tca_request_temp)

        date_str = ''

        for t in tca_request_list:
            date_str = date_str + ' / ' + str(t.start_date) + ' to ' + str(t.finish_date)

        logger.debug("Split TCA request for " + str(tca_request.ticker) + " dates " + date_str +
                     " from original request " + str(tca_request.start_date) + ' to ' + str(tca_request.finish_date))

        return tca_request_list

    def get_tca_version(self):
        return 'pro'
