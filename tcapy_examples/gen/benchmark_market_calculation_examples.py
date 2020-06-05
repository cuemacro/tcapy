from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
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
data_source = 'dukascopy'

# Change the market and trade data store as necessary
market_data_store = 'arctic-' + data_source

trade_data_source = 'ms_sql_server'
ticker = ['EURUSD', 'USDJPY']

# We are purely doing market analysis using market data (and client trade/order data)
tca_type = 'market-analysis'
start_date = '01 Jan 2017'; finish_date = '01 Aug 2017'
bid_mid_bp = 0.1;
ask_mid_bp = 0.1

use_test_csv = False

folder = constants.test_data_folder

def get_tca_request():
    """This TCARequest is purely for market analysis
    """

    if use_test_csv:
        return TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                 reporting_currency='USD',
                                 market_data_store=os.path.join(folder, 'small_test_market_df.csv.gz'),
                                 tca_type=tca_type)
    else:
        return TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                 reporting_currency='USD',
                                 market_data_store=market_data_store,
                                 tca_type=tca_type)

def get_sample_data():
    """Load sample market/trade/order data
    """
    logger.info("About to load data for " + ticker)

    tca_engine = TCAEngineImpl()

    trade_order_results_df_dict = tca_engine.calculate_tca(get_tca_request())

    return trade_order_results_df_dict[ticker + '_df']

def example_calculate_spread_to_mid_benchmark():
    """Example on how to add spread to the benchmark
    """
    from tcapy.analysis.algos.benchmark import BenchmarkMarketSpreadToMid

    market_df, trade_df, order_df = get_sample_data()

    benchmark_spread_to_mid = BenchmarkMarketSpreadToMid()
    trade_df, market_df = benchmark_spread_to_mid.calculate_benchmark(trade_order_df=trade_df, market_df=market_df)

    print(trade_df)

def example_request_mid_benchmark():
    """Example of how to do a calculation to do market analysis to calculate mid, resample etc. without any trade data

    """
    from tcapy.analysis.algos.benchmark import BenchmarkMarketMid, BenchmarkMarketSpreadToMid, BenchmarkMarketResampleOffset

    tca_request = get_tca_request()

    # Allow analysis to be done in a parallel way day by day
    # (note: can't do analysis which requires data outside of the day to do this!)
    tca_request.multithreading_params['splice_request_by_dates'] = True

    # We'll calculate the market mid, then calculate the spread to the mid, then we shall resample the data into 1 minute
    # data, taking the mean of each minute (and TWAP)
    tca_request.benchmark_calcs = [BenchmarkMarketMid(), BenchmarkMarketSpreadToMid(),
                                   BenchmarkMarketResampleOffset(market_resample_freq='1', market_resample_unit='min', price_field='mid',
                                                                 resample_how=['mean', 'twap'])]
    tca_request.use_multithreading = True

    tca_engine = TCAEngineImpl()

    dict_of_df = tca_engine.calculate_tca(tca_request)

    print(dict_of_df)

if __name__ == '__main__':
    import time

    start = time.time()

    example_calculate_spread_to_mid_benchmark()
    example_request_mid_benchmark()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
