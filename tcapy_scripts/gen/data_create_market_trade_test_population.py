"""This script populates various test databases

- test database for market data (Arctic)
- test database for randomised trades (SQL Server)

"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os

from tcapy.data.datatestcreator import DataTestCreator
from tcapy.conf.constants import Constants
from tcapy.data.databasesource import DatabaseSourceCSV, DatabaseSourceArctic, DatabaseSourceMSSQLServer

if __name__ == '__main__':
    COPY_MARKET_CSV_DATA = False
    GENERATE_RANDOM_TRADE_ORDER_CSV_DATA = True
    COPY_TRADE_ORDER_CSV_DATA = False

    # Load market data
    data_source_csv = DatabaseSourceCSV()

    data_source = 'dukascopy'
    data_test_creator = DataTestCreator(market_data_postfix=data_source)

    # Use database source as Arctic for market data and SQL Server for trade/order data
    data_test_creator._database_source_market = DatabaseSourceArctic(postfix=data_source)
    data_test_creator._database_source_trade = DatabaseSourceMSSQLServer()

    # data_test_creator.fetch_test_database()

    # Load up market data from CSV and dump into SQL database
    ticker = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDNOK', 'USDSEK', 'EURNOK', 'EURSEK',
              'USDTRY', 'USDJPY']

    # Trades for these tickers
    ticker_trade = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'EURNOK', 'EURSEK',
              'USDJPY', 'AUDJPY', 'NZDCAD', 'EURJPY']

    # folder = '/ext_data/tcapy_data/'
    folder = None

    if folder is None:
        folder = Constants().test_data_folder

    # Copy market data from flat files (can be either .csv or .h5 file - much quicker to use .h5 files)
    # to Arctic market database

    # This relies on you have market data stored in H5 files already (eg. by downloading from DukasCopy)
    # note: whilst free FX data can be used for testing (in particular for generating randomised trades),
    # we recommend using higher quality data for actual benchmark
    if COPY_MARKET_CSV_DATA :
        csv_file = ['test_market_' + x + '.h5' for x in ticker]
        csv_market_data = [os.path.join(folder, x) for x in csv_file]

        data_test_creator.populate_test_database_with_csv(csv_market_data = csv_market_data, ticker = ticker,
                                                          csv_trade_data = None,
                                                          if_exists_table = 'append', if_exists_ticker = 'replace')

    # Create randomised trade/order data, dump to CSV (and then read back those CSVs to dump to database)
    # want to have CSVs so easier to check output
    if GENERATE_RANDOM_TRADE_ORDER_CSV_DATA:
        # Store the test trade/order data in different CSV files (and database tables)
        csv_trade_data = \
            {'trade_df' : os.path.join(folder, 'trade_open_df.csv'),
             'order_df': os.path.join(folder, 'order_open_df.csv'),
             }

        trade_order = data_test_creator.create_test_trade_order(ticker_trade, start_date = '01 Apr 2017',
                                                                finish_date = '05 Jun 2017')

        # We do not need to specify the ticker
        # We assume that each of the following (with multiple tickers)
        # is stored in a table by itself, trade_df or order_df
        for c in csv_trade_data.keys():
            trade_order[c].to_csv(csv_trade_data[c])


    if COPY_TRADE_ORDER_CSV_DATA:
        # Users may want to modify these paths to their own trade/order data, when running in production
        csv_trade_mapping = {'trade': os.path.join(folder, 'trade_open_df.csv'),
                             'order': os.path.join(folder, 'order_open_df.csv'),
                                 }

        # Should we append to our existing trade data, or should we replace it
        # if we are just updating our trade data we should seek to 'append'
        if_exists_trade_table = 'replace'  # 'replace' or 'append'

        data_test_creator.populate_test_database_with_csv(csv_market_data=None, ticker=None,
                                                              csv_trade_data=csv_trade_mapping,
                                                              if_exists_trade_table=if_exists_trade_table)


