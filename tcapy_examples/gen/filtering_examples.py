from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os

from tcapy.util.loggermanager import LoggerManager

from tcapy.analysis.tcaengine import TCAEngineImpl
from tcapy.analysis.tcarequest import TCARequest

from tcapy.analysis.algos.benchmark import BenchmarkMarketMid

from tcapy.data.databasesource import *

logger = LoggerManager.getLogger(__name__)

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
                                 market_data_store=market_data_store,
                                 trade_order_mapping=['trade_df', 'order_df'],
                                 tca_type=tca_type, benchmark_calcs=BenchmarkMarketMid())

    tca_engine = TCAEngineImpl()

    trade_order_results_df_dict = tca_engine.calculate_tca(tca_request)

    return trade_order_results_df_dict[ticker + '_df'], trade_order_results_df_dict['trade_df'], \
           trade_order_results_df_dict['order_df']


def example_filter_by_time_of_day_day_of_week():
    """Example showing how to filter trade data by the time of data and day of the week
    """

    from tcapy.analysis.tradeorderfilter import TradeOrderFilterTimeOfDayWeekMonth

    # Get sample market/trade/order data
    market_df, trade_df, order_df = get_sample_data()

    # Create a filter for trades between 7-12pm
    trade_order_filter = TradeOrderFilterTimeOfDayWeekMonth(
        time_of_day={'start_time': '07:00', 'finish_time': '12:00'}, day_of_week='Mon', month_of_year='May')

    trade_df = trade_order_filter.filter_trade_order(trade_df)

    # Check the final time series has no values outside of 7am-12pm, on Mondays in May
    assert (trade_df.index.hour >= 7).all() and (trade_df.index.hour <= 12).all() \
           and (trade_df.index.dayofweek == 0).all() and (trade_df.index.month == 5).all()

if __name__ == '__main__':
    import time

    start = time.time()

    example_filter_by_time_of_day_day_of_week()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
