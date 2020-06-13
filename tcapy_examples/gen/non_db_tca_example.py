from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os
from collections import OrderedDict

import time

from tcapy.util.mediator import Mediator
from tcapy.conf.constants import Constants

constants = Constants()

folder = constants.test_data_harness_folder

volatile_cache = Mediator.get_volatile_cache()

def tca_example_csv_trade_data_dukascopy():
    """Loads up trade/order data from CSV files and market data externally from Dukascopy. Does not use any databases, if
    you rarely use TCA, this is fine. However, for heavy use of TCA, we strongly recommend maintaining an internal tick
    database, as external downloading of data can be very slow.

    In this case we are simply calculating the slippage of every trade and orders above them.
    """

    from tcapy.analysis.tcaengine import TCAEngineImpl
    from tcapy.analysis.tcarequest import TCARequest

    from tcapy.analysis.algos.benchmark import BenchmarkArrival, BenchmarkMarketSpreadToMid
    from tcapy.analysis.algos.metric import MetricSlippage

    from tcapy.analysis.algos.resultsform import TimelineResultsForm

    tca_version  = constants.tcapy_version
    tca_engine = TCAEngineImpl(version=tca_version)

    # The test trade/order data is populated between 25 Apr 2017-05 Jun 2017
    # with trades/orders for 'EURUSD', 'USDJPY' and 'EURJPY'
    csv_trade_order_mapping = OrderedDict([('trade_df', os.path.join(folder, 'small_test_trade_df.csv')),
                                           ('order_df', os.path.join(folder, 'small_test_order_df.csv'))])

    # Specify the TCA request (note: by specifiying use_multithreading is False, we avoid dependencies like Celery

    # Depending on how the caching is setup, tcapy may try to download market data in monthly/weekly chunks and cache them,
    # To force deletion of the cache you can run the below

    # volatile_cache.clear_cache()

    # However if you run TCA for the same period, it will load the market data from Redis/in-memory, rather than
    # downloading it externally from Dukascopy
    tca_request = TCARequest(start_date='05 May 2017', finish_date='10 May 2017', ticker=['EURUSD'],
                             tca_type='detailed',
                             trade_data_store='csv', market_data_store='dukascopy',
                             trade_order_mapping=csv_trade_order_mapping,
                             metric_calcs=[MetricSlippage()],
                             results_form=[TimelineResultsForm(metric_name='slippage', by_date='datehour', scalar=10000.0)],
                             benchmark_calcs=[BenchmarkArrival(), BenchmarkMarketSpreadToMid()],
                             use_multithreading=False)

    # Dictionary of dataframes as output from TCA calculation
    dict_of_df = tca_engine.calculate_tca(tca_request)

    print(dict_of_df.keys())

def tca_example_csv_trade_data_dukascopy_no_redis():
    """Running TCA calculation but without any Redis caching at all. In practice, this should be avoided, since it will
    likely be much slower, given we'll end up accessing market data/trade data a lot more often from a slow source.

    This is particularly an issue when we're downloading large samples of market data from an external source. For very small
    time periods this might be fine.
    """
    from tcapy.analysis.tcaengine import TCAEngineImpl
    from tcapy.analysis.tcarequest import TCARequest

    from tcapy.analysis.algos.benchmark import BenchmarkArrival, BenchmarkMarketSpreadToMid
    from tcapy.analysis.algos.metric import MetricSlippage

    from tcapy.analysis.algos.resultsform import TimelineResultsForm

    tca_version = constants.tcapy_version
    tca_engine = TCAEngineImpl(version=tca_version)

    # The test trade/order data is populated between 25 Apr 2017-05 Jun 2017
    # with trades/orders for 'EURUSD', 'USDJPY' and 'EURJPY'
    csv_trade_order_mapping = OrderedDict([('trade_df', os.path.join(folder, 'small_test_trade_df.csv')),
                                           ('order_df', os.path.join(folder, 'small_test_order_df.csv'))])

    # Specify the TCA request (note: by specifiying use_multithreading is False, we avoid dependencies like Celery

    # Depending on how the caching is setup, tcapy may try to download market data in monthly/weekly chunks and cache them,
    # To force deletion of the cache you can run the below

    # volatile_cache.clear_cache()

    # However if you run TCA for the same period, it will load the market data from Redis/in-memory, rather than
    # downloading it externally from Dukascopy
    tca_request = TCARequest(start_date='05 May 2017', finish_date='06 May 2017', ticker=['EURUSD'],
                             tca_type='detailed',
                             trade_data_store='csv', market_data_store='dukascopy',
                             trade_order_mapping=csv_trade_order_mapping,
                             metric_calcs=[MetricSlippage()],
                             results_form=[
                                 TimelineResultsForm(metric_name='slippage', by_date='datehour', scalar=10000.0)],
                             benchmark_calcs=[BenchmarkArrival(), BenchmarkMarketSpreadToMid()],
                             use_multithreading=False)

    tca_request.multithreading_params = {'splice_request_by_dates': False,  # True or False
                                         'cache_period': 'month',  # month or week

                                         # Cache trade data in monthly/periodic chunks in Redis (reduces database calls a lot)
                                         'cache_period_trade_data': False,

                                         # Cache market data in monthly/periodic chunks in Redis (reduces database calls a lot)
                                         'cache_period_market_data': False,

                                         # Return trade data internally as handles (usually necessary for Celery)
                                         'return_cache_handles_trade_data': False,

                                         # Return market data internally as handles (usually necessary for Celery)
                                         'return_cache_handles_market_data': False,

                                         # Recommend using Celery, which allows us to reuse Python processes
                                         'parallel_library': 'single'
                                         }

    # Dictionary of dataframes as output from TCA calculation
    dict_of_df = tca_engine.calculate_tca(tca_request)

    print(dict_of_df.keys())

    market_df = dict_of_df['market_df']

    market_df_minute = market_df.resample('1min').last()
    print(market_df_minute)


if __name__ == '__main__':
    start = time.time()

    # tca_example_csv_trade_data_dukascopy()
    tca_example_csv_trade_data_dukascopy_no_redis()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
