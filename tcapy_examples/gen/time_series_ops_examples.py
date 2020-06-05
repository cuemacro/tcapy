from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#
# This may not be distributed without the permission of Cuemacro.
#

import os

from tcapy.analysis.tcaengine import TCAEngineImpl
from tcapy.analysis.tcarequest import TCARequest
from tcapy.analysis.algos.benchmark import BenchmarkMarketMid
from tcapy.util.timeseries import TimeSeriesOps
from tcapy.util.loggermanager import LoggerManager

from tcapy.conf.constants import Constants

logger = LoggerManager.getLogger(__name__)
time_series_ops = TimeSeriesOps()
constants = Constants()

# 'dukascopy' or 'ncfx'
data_source = 'ncfx'

# Change the market and trade data store as necessary
market_data_store = 'arctic-' + data_source
trade_data_source = 'ms_sql_server'
ticker = 'EURUSD'
tca_type = 'aggregated'
start_date = '01 May 2017'
finish_date = '12 May 2017'
bid_mid_bp = 0.1;
ask_mid_bp = 0.1

use_test_csv = True

folder = constants.test_data_harness_folder


def get_sample_data():
    logger.info("About to load data for " + ticker)

    if use_test_csv:
        tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                 trade_data_store='csv',
                                 reporting_currency='EUR',
                                 market_data_store=os.path.join(folder, 'small_test_market_df.csv.gz'),
                                 trade_order_mapping={'trade_df': os.path.join(folder, 'small_test_trade_df.csv'),
                                                      'order_df': os.path.join(folder, 'small_test_order_df.csv')},
                                 tca_type=tca_type, benchmark_calcs=BenchmarkMarketMid())
    else:
        tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                 trade_data_store=trade_data_source,
                                 reporting_currency='EUR',
                                 market_data_store=data_source,
                                 trade_order_mapping=['trade_df'], tca_type=tca_type, benchmark_calcs=BenchmarkMarketMid())

    tca_engine = TCAEngineImpl()

    trade_order_results_df_dict = tca_engine.calculate_tca(tca_request)

    return trade_order_results_df_dict[ticker + '_df'], trade_order_results_df_dict['trade_df'], \
           trade_order_results_df_dict['order_df']


def example_calculate_weighted_average():
    """Example to create a weighted average of all (numerical) columns trades (with weighting by notional)
    """
    market_df, trade_df, order_df = get_sample_data()

    avg = time_series_ops.weighted_average_of_each_column(trade_df, weighting_col='notional')

    print(avg)


if __name__ == '__main__':
    import time

    start = time.time()

    example_calculate_weighted_average()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
