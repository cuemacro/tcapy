from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os

from tcapy.analysis.tcaengine import TCAEngineImpl
from tcapy.analysis.tcarequest import TCARequest
from tcapy.util.loggermanager import LoggerManager

from tcapy.conf.constants import Constants

logger = LoggerManager.getLogger(__name__)

constants = Constants()

# 'dukascopy' or 'ncfx'
data_source = 'ncfx'

# Change the market and trade data store as necessary
market_data_store = 'arctic-' + data_source

trade_data_source = 'ms_sql_server'
ticker = 'EURUSD'
tca_type = 'aggregated'
start_date = '01 May 2017'
finish_date = '01 Aug 2017'
bid_mid_bp = 0.1;
ask_mid_bp = 0.1

use_test_csv = False

folder = constants.test_data_folder

def get_sample_data():
    """Load sample market/trade/order data
    """
    logger.info("About to load data for " + ticker)

    if use_test_csv:
        tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                 trade_data_store='csv',
                                 reporting_currency='EUR',
                                 market_data_store=os.path.join(folder, 'small_test_market_df.csv.gz'),
                                 trade_order_mapping={'trade_df': os.path.join(folder, 'small_test_trade_df.csv'),
                                                      'order_df': os.path.join(folder, 'small_test_order_df.csv')},
                                 tca_type=tca_type)
    else:
        tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                 trade_data_store=trade_data_source,
                                 reporting_currency='EUR',
                                 market_data_store=market_data_store,
                                 trade_order_mapping=['trade_df', 'order_df'], tca_type=tca_type)

    tca_engine = TCAEngineImpl()

    trade_order_results_df_dict = tca_engine.calculate_tca(tca_request)

    return trade_order_results_df_dict[ticker + '_df'], trade_order_results_df_dict['trade_df'], \
           trade_order_results_df_dict['order_df']


def example_calculate_vwap_benchmark():
    """Example showing how to calculate VWAP benchmarks on orders
    """

    from tcapy.analysis.algos.benchmark import BenchmarkVWAP

    market_df, trade_df, order_df = get_sample_data()

    benchmark_vwap = BenchmarkVWAP()
    order_df, _ = benchmark_vwap.calculate_benchmark(trade_order_df=order_df, market_df=market_df)

    print(order_df)


def example_calculate_twap_benchmark():
    """Example showing how to calculate TWAP (time weighted average price) benchmarks on orders
    """

    from tcapy.analysis.algos.benchmark import BenchmarkTWAP

    market_df, trade_df, order_df = get_sample_data()

    benchmark_twap = BenchmarkTWAP()
    order_df, _ = benchmark_twap.calculate_benchmark(trade_order_df=order_df, market_df=market_df)

    print(trade_df)


def example_calculate_arrival_benchmark():
    """Example on how to add arrival price to trades
    """
    from tcapy.analysis.algos.benchmark import BenchmarkArrival

    market_df, trade_df, order_df = get_sample_data()

    benchmark_arrival = BenchmarkArrival(bid_benchmark='mid', ask_benchmark='mid')
    trade_df, market_df = benchmark_arrival.calculate_benchmark(trade_order_df=trade_df, market_df=market_df)

    print(trade_df)


def example_calculate_spread_to_mid_benchmark():
    """Example on how to add spread to the benchmark
    """
    from tcapy.analysis.algos.benchmark import BenchmarkMarketSpreadToMid

    market_df, trade_df, order_df = get_sample_data()

    benchmark_spread_to_mid = BenchmarkMarketSpreadToMid()
    trade_df, market_df = benchmark_spread_to_mid.calculate_benchmark(trade_order_df=trade_df, market_df=market_df)

    print(trade_df)

if __name__ == '__main__':
    import time

    start = time.time()

    example_calculate_vwap_benchmark()
    example_calculate_twap_benchmark()
    example_calculate_arrival_benchmark()
    example_calculate_spread_to_mid_benchmark()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
