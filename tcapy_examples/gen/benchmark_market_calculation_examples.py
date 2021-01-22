from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os

from chartpy import Chart, Style

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

trade_data_source = 'mysql'
ticker = ['EURUSD', 'USDJPY']

use_multithreading = False

# We are purely doing market analysis using market data (and client trade/order data)
tca_type = 'market-analysis'
start_date = '01 Jan 2020'; finish_date = '01 Jun 2020'

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
                                 tca_type=tca_type, use_multithreading=use_multithreading)
    else:
        return TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                 reporting_currency='USD',
                                 market_data_store=market_data_store,
                                 tca_type=tca_type, use_multithreading=use_multithreading)

def get_sample_data():
    """Load sample market/trade/order data
    """
    logger.info("About to load data for " + ticker[0])

    tca_engine = TCAEngineImpl()

    trade_order_results_df_dict = tca_engine.calculate_tca(get_tca_request())

    return trade_order_results_df_dict[ticker[0] + '_df']

def example_calculate_spread_to_mid_benchmark():
    """Example on how to add spread to the benchmark market data
    """
    from tcapy.analysis.algos.benchmark import BenchmarkMarketSpreadToMid

    market_df = get_sample_data()

    benchmark_spread_to_mid = BenchmarkMarketSpreadToMid()
    market_df = benchmark_spread_to_mid.calculate_benchmark(market_df=market_df)

    print(market_df)

def example_request_mid_benchmark():
    """Example of how to do a calculation to do market analysis to calculate mid, resample etc. without any trade data

    """
    from tcapy.analysis.algos.benchmark import BenchmarkMarketMid, BenchmarkMarketSpreadToMid, BenchmarkMarketResampleOffset, \
        BenchmarkMarketFilter
    from tcapy.analysis.algos.resultsform import BarResultsForm, TimelineResultsForm

    tca_request = get_tca_request()

    # Allow analysis to be done in a parallel approach day by day
    # (note: can't do analysis which requires data outside of the daily chunks to do this!)
    tca_request.multithreading_params['splice_request_by_dates'] = use_multithreading

    # Filter market data by time of day between 15:00-17:00 LDN
    # Then calculate the market mid, then calculate the spread to the mid,
    # Then resample the data into 1 minute, taking the mean of each minute (and TWAP) and calculating the absolute range
    tca_request.benchmark_calcs = [BenchmarkMarketFilter(time_of_day={'start_time' : "15:00", 'finish_time' : "17:00"},
                                                         time_zone='Europe/London'),
                                   BenchmarkMarketMid(), BenchmarkMarketSpreadToMid(),
                                   BenchmarkMarketResampleOffset(market_resample_freq='1', market_resample_unit='min',
                                        price_field='mid', resample_how=['mean', 'twap', 'absrange'], dropna=True),
                                   ]

    # Calculate the mean spread to mid for EURUSD by time of day during our sample (do not weight by any other field)
    # Calculate the mean absrange for EURUSD by time of day (London timezone)/month of _year (ie. proxy for volatility)
    tca_request.results_form = \
        [TimelineResultsForm(market_trade_order_list='EURUSD', metric_name='ask_mid_spread',
                             weighting_field=None, by_date='time', scalar=10000.0),
         TimelineResultsForm(market_trade_order_list='EURUSD', metric_name='absrange',
                             weighting_field=None, by_date=['month', 'timeldn'], scalar=10000.0)
        ]

    # return
    tca_request.use_multithreading = True

    tca_engine = TCAEngineImpl()

    dict_of_df = tca_engine.calculate_tca(tca_request)

    # Print out all keys for all the DataFrames returned
    print(dict_of_df.keys())

    # Print market data snapshots
    print(dict_of_df['EURUSD_df'])
    print(dict_of_df['USDJPY_df'])
    print(dict_of_df['EURUSD_df'].columns)
    print(dict_of_df['USDJPY_df'].columns)

    # Print out mean spread by time of day
    print(dict_of_df['timeline_EURUSD_ask_mid_spread_by/mean_time/all'])

    # Plot mean spread by time of day and absrange by time of day (in London timezone)
    Chart(engine='plotly').plot(dict_of_df['timeline_EURUSD_ask_mid_spread_by/mean_time/all'])

    # Plot absolute range over each minute, averaged by time of day and month of the _year
    Chart(engine='plotly').plot(dict_of_df['timeline_EURUSD_absrange_by/mean_month_timeldn/all'],
                                style=Style(title='EURUSD absolute range by time of day (LDN)', color='Reds', scale_factor=-1))

if __name__ == '__main__':
    import time

    start = time.time()

    example_calculate_spread_to_mid_benchmark()
    example_request_mid_benchmark()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
