"""Dumps a small dataset of test market and trade data (with tick marekt data fetched from external data source Eikon)
which can be used to test tcapy in compressed CSV format and Parquet and will place it in the test harness folder.

Test trade/order data already exists from the
repo, however, you will need to download the market data from Dukascopy to run many of the CSV based tests.

eg. /home/tcapyuser/cuemacro/tcapy/tests_harness_data
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.data.datatestcreator import DataTestCreator
from tcapy.conf.constants import Constants
from tcapy.analysis.tcarequest import MarketRequest
from tcapy.analysis.tcamarkettradeloaderimpl import TCAMarketTradeLoaderImpl

from tcapy.util.utilfunc import UtilFunc

import os
import pandas as pd

constants = Constants()

postfix = 'eikon'

import datetime
from datetime import timedelta

# if already dumped in database (need to use this, if we want to calculate cross rates eg. EURJPY from EURUSD and USDJPY)
# _data_store = 'arctic-eikon'
data_store = 'eikon'            # if downloading from Dukascopy externally

ticker = ['EURUSD', 'USDJPY']
ticker_trades = ['EURUSD', 'USDJPY', 'EURJPY']
start_date = datetime.datetime.utcnow() - timedelta(hours=24)
finish_date = datetime.datetime.utcnow()

folder = constants.test_data_harness_folder

# By default don't generate test trade/order data, can use the files already dumped for test harness
# - small_test_trade_df_eikon.csv
# - small_test_order_df_eikon.csv
create_trade_order_data = True

def create_market_trade_data_eikon():
    """Creates a small dataset for testing purposes for market, trade and order data for EURUSD at the start of May 2017,
    which is dumped to the designated tcapy test harness folder.

    Returns
    -------

    """
    # Use database source as Arctic (or directly from Dukascopy) for market data (assume we are using market data as a source)
    tca_market = TCAMarketTradeLoaderImpl()

    util_func = UtilFunc()

    market_df = []

    for tick in ticker:
        market_request = MarketRequest(ticker=tick, data_store=data_store,
                                       start_date=start_date, finish_date=finish_date)

        market_df.append(tca_market.get_market_data(market_request=market_request))

    # Note: it can be very slow to write these CSV files
    market_df = pd.concat(market_df)
    market_df.to_csv(os.path.join(folder, 'small_test_market_df_eikon.csv.gz'), compression='gzip')

    # Also write to disk as HDF5 file (easier to load up later)
    util_func.write_dataframe_to_binary(market_df, os.path.join(folder, 'small_test_market_df_eikon.gzip'))

    # Create a spot file in reverse order
    market_df.sort_index(ascending=False)\
        .to_csv(os.path.join(folder, 'small_test_market_df_reverse_eikon.csv.gz'), compression='gzip')

    # Also write to disk as Parquet file (easier to load up later)
    util_func.write_dataframe_to_binary(market_df, os.path.join(folder, 'small_test_market_df_reverse_eikon.parquet'))

    if create_trade_order_data:
        # Use the market data we just downloaded to CSV, and perturb it to generate the trade data
        data_test_creator = DataTestCreator(market_data_postfix=postfix,
                                            csv_market_data=os.path.join(folder, 'small_test_market_df_eikon.csv.gz'),
                                            write_to_db=False)

        # Create randomised trade/order data
        trade_order = data_test_creator.create_test_trade_order(ticker_trades, start_date=start_date, finish_date=finish_date)

        trade_order['trade_df'].to_csv(os.path.join(folder, 'small_test_trade_df_eikon.csv'))
        trade_order['order_df'].to_csv(os.path.join(folder, 'small_test_order_df_eikon.csv'))

if __name__ == '__main__':
    create_market_trade_data_eikon()
