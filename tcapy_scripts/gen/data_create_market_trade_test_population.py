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
from tcapy.data.databasesource import DatabaseSourceCSV, DatabaseSourceArctic, DatabaseSourceMSSQLServer, \
    DatabaseSourceMySQL

constants = Constants()

if __name__ == '__main__':
    COPY_MARKET_CSV_DATA = False
    GENERATE_RANDOM_TRADE_ORDER_CSV_DATA = True
    COPY_TRADE_ORDER_CSV_DATA = False

    # Load market data
    data_source_csv = DatabaseSourceCSV()

    data_source = 'dukascopy'

    csv_marker = 'small_test'
    start_date_trade_generation = '01 Apr 2017'; finish_date_trade_generation = '05 Jun 2017'

    # csv_marker = 'large_test'
    # start_date_trade_generation = '01 Apr 2016'; finish_date_trade_generation = '31 Mar 2020'

    # 'mysql' or 'ms_sql_server'
    sql_trade_database_type = 'mysql'

    data_test_creator = DataTestCreator(market_data_postfix=data_source,
                                        sql_trade_database_type=sql_trade_database_type)

    # Use database source as Arctic for market data and SQL Server for trade/order data
    data_test_creator._database_source_market = DatabaseSourceArctic(postfix=data_source)

    # data_test_creator.fetch_test_database()

    # Load up market data from CSV and dump into SQL database
    ticker = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDNOK', 'USDSEK', 'EURNOK', 'EURSEK',
              'USDTRY', 'USDJPY']

    # Generate trades/orders for these _tickers
    ticker_trade = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'EURNOK', 'EURSEK',
                    'USDJPY', 'AUDJPY', 'NZDCAD', 'EURJPY']

    market_data_folder = '/data/csv_dump/' + data_source
    trade_order_folder = '/data/csv_dump/trade_order'

    if trade_order_folder is None:
        trade_order_folder = constants.test_data_harness_folder

    # Copy market data from flat files (can be either .csv or .h5 or .parquet file - much quicker to use .parquet files)
    # to Arctic market database

    # This relies on you have market data stored in Parquet files already (eg. by downloading from DukasCopy)
    # Note: whilst free FX data can be used for testing (in particular for generating randomised trades),
    # you may use any other data you'd like
    if COPY_MARKET_CSV_DATA:
        csv_file = ['test_market_' + x + '.parquet' for x in ticker]
        csv_market_data = [os.path.join(market_data_folder, x) for x in csv_file]

        data_test_creator.populate_test_database_with_csv(csv_market_data=csv_market_data, ticker=ticker,
                                                          csv_trade_data=None,
                                                          if_exists_table='append', if_exists_ticker='replace')

    # Create randomised trade/order data, dump to CSV (and then read back those CSVs to dump to database)
    # want to have CSVs so easier to check output
    # Note: won't overwrite the premade test trade/order CSVs
    if GENERATE_RANDOM_TRADE_ORDER_CSV_DATA:
        # Store the test trade/order data in different CSV files (and database tables)
        csv_trade_data = \
            {'trade_df': os.path.join(trade_order_folder, csv_marker + '_trade_df_generated.csv'),
             'order_df': os.path.join(trade_order_folder, csv_marker + '_order_df_generated.csv'),
             }

        trade_order = data_test_creator.create_test_trade_order(ticker_trade, start_date=start_date_trade_generation,
                                                                finish_date=finish_date_trade_generation)

        # We do not need to specify the ticker
        # We assume that each of the following (with multiple _tickers)
        # is stored in a table by itself, trade_df or order_df
        for c in csv_trade_data.keys():
            trade_order[c].to_csv(csv_trade_data[c])

    if COPY_TRADE_ORDER_CSV_DATA:
        # Users may want to modify these paths to their own trade/order data, when running in production
        # This will copy premade files we just produced now
        # In this case 'trade' and 'order' are the tables on the SQL database
        database_table_trade_mapping = {'trade': os.path.join(trade_order_folder, csv_marker + '_trade_df_generated.csv'),
                                        'order': os.path.join(trade_order_folder, csv_marker + '_order_df_generated.csv'),
                                        }

        # Should we append to our existing trade data, or should we replace it
        # if we are just updating our trade data we should seek to 'append'
        if_exists_trade_table = 'replace'  # 'replace' or 'append'

        data_test_creator.populate_test_database_with_csv(csv_market_data=None, ticker=None,
                                                          csv_trade_data=database_table_trade_mapping,
                                                          if_exists_trade_table=if_exists_trade_table)
