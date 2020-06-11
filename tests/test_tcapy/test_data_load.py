"""Tests functions for loading market and trade/order data (CSVs, SQL, Arctic, InfluxDB and KDB). Check that your database has
market and trade data for these before running the test. test_data_write will write data into a test database first
and then read - so that can be used instead, if your production market and trade databases are not yet populated.

This will also involve setting your database IP, username, password etc.
"""

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import pandas as pd

try:
    from pandas.testing import assert_frame_equal
except:
    from pandas.util.testing import assert_frame_equal

import os

from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager

from tcapy.analysis.tcarequest import TCARequest
from tcapy.data.datafactory import MarketRequest, TradeRequest

from collections import OrderedDict
from tcapy.util.mediator import Mediator

logger = LoggerManager().getLogger(__name__)

constants = Constants()

logger.info('Make sure you have created folder ' + constants.csv_folder + ' & ' + constants.temp_data_folder +
            ' otherwise tests will fail')

########################################################################################################################
# YOU MAY NEED TO CHANGE THESE

# 'ncfx' or 'dukascopy'
data_source = 'ncfx'

arctic_market_data_store = 'arctic-' + data_source
ms_sql_server_trade_data_store = 'ms_sql_server'

ms_sql_server_trade_data_database_name = constants.ms_sql_server_trade_data_database_name

kdb_market_data_store = 'kdb-' + data_source
kdb_market_data_database_table = constants.kdb_market_data_database_table

influxdb_market_data_store = 'kdb-' + data_source
influxdb_market_data_database_table = constants.influxdb_market_data_database_table

run_arctic_tests = True
run_influx_db_tests = False
run_kdb_tests = False
run_ms_sql_server_tests = True

start_date = '26 Apr 2017'
finish_date = '05 Jun 2017'
ticker = 'EURUSD'

test_harness_arctic_market_data_store = 'arctic-testharness'
test_harness_ms_sql_server_trade_data_database_name = 'trade_database_test_harness'

########################################################################################################################
folder = constants.test_data_harness_folder

trade_order_list = ['trade_df', 'order_df']

ms_sql_server_trade_order_mapping = OrderedDict(
    [('trade_df', '[dbo].[trade]'),     # name of table which has broker messages to client
     ('order_df', '[dbo].[order]')])    # name of table which has orders from client

eps = 10 ** -5

invalid_start_date = '01 Jan 1999'
invalid_finish_date = '01 Feb 1999'

csv_market_data_store = os.path.join(folder, 'small_test_market_df.csv.gz')
csv_reverse_market_data_store = os.path.join(folder, 'small_test_market_df_reverse.csv.gz')

csv_trade_order_mapping = OrderedDict([('trade_df', os.path.join(folder, 'small_test_trade_df.csv')),
                                       ('order_df', os.path.join(folder, 'small_test_order_df.csv'))])

use_multithreading = False

def test_fetch_market_trade_data_csv():
    """Tests downloading of market and trade/order data from CSV files
    """

    ### Get market data
    market_loader = Mediator.get_tca_market_trade_loader()

    market_request = MarketRequest(
        start_date=start_date, finish_date=finish_date, ticker=ticker, data_store=csv_market_data_store)

    market_df = market_loader.get_market_data(market_request)

    assert not(market_df.empty) \
           and market_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
           and market_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

    # For a high level trade data request, we need to use TCA request, because it usually involves some
    # market data download (we are assuming that the market data is being downloaded from our arctic database)
    # eg. for converting notionals to reporting currency
    tca_request = TCARequest(
        start_date=start_date, finish_date=finish_date, ticker=ticker,
        trade_data_store='csv', market_data_store=arctic_market_data_store,
        trade_order_mapping=csv_trade_order_mapping
    )

    for t in trade_order_list:
        trade_order_df = market_loader.get_trade_order_data(tca_request, t)

        try:
            trade_order_df = Mediator.get_volatile_cache().get_dataframe_handle(trade_order_df)
        except:
            pass

        assert not trade_order_df.empty \
               and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
               and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

    ### Test using DataFactory and DatabaseSource
    from tcapy.data.datafactory import DataFactory

    data_factory = DataFactory()

    for t in trade_order_list:
        ### Test using DataFactory
        trade_request = TradeRequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                     data_store='csv', trade_order_mapping=csv_trade_order_mapping,
                                     trade_order_type=t)

        trade_order_df = data_factory.fetch_table(trade_request)

        assert not trade_order_df.empty \
                          and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
                          and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

        ### Test using DatabaseSourceCSV
        from tcapy.data.databasesource import DatabaseSourceCSV

        database_source = DatabaseSourceCSV()

        trade_order_df = database_source.fetch_trade_order_data(start_date, finish_date, ticker,
                                                          table_name=csv_trade_order_mapping[t])

        assert not trade_order_df.empty \
                             and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
                             and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')


def test_sql_server_connection():
    """Tests the connection with the MS SQL Server database
    """
    if not (run_ms_sql_server_tests): return

    from tcapy.data.databasesource import DatabaseSourceMSSQLServer

    dbs = DatabaseSourceMSSQLServer()

    engine, connection_expression = dbs._get_database_engine(database_name=ms_sql_server_trade_data_database_name)
    connection = engine.connect()

    # try running simple query (getting the table names of the database)
    logger.info("Table names = " + str(engine.table_names()))
    connection.close()

    assert connection is not None

def test_fetch_trade_data_ms_sql_server():
    """Tests that we can fetch data from the Microsoft SQL Server database. Note you need to populate the database
    first before running this for the desired dates.
    """
    if not (run_ms_sql_server_tests): return

    from tcapy.data.datafactory import DataFactory
    from tcapy.data.databasesource import DatabaseSourceMSSQLServer

    ### Test using TCAMarketTradeLoader
    market_loader = Mediator.get_tca_market_trade_loader()

    for t in trade_order_list:
        trade_order_mapping = {t : ms_sql_server_trade_order_mapping[t]}

        trade_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                   trade_data_store=ms_sql_server_trade_data_store, trade_order_mapping=trade_order_mapping,
                                   market_data_store=arctic_market_data_store, use_multithreading=use_multithreading)

        trade_order_df = market_loader.get_trade_order_data(trade_request, t)

        try:
            trade_order_df = Mediator.get_volatile_cache().get_dataframe_handle(trade_order_df)
        except:
            pass

        assert not trade_order_df.empty \
               and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
               and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

        ### Test using DataFactory
        data_factory = DataFactory()

        trade_request = TradeRequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                     data_store=ms_sql_server_trade_data_store, trade_order_mapping=trade_order_mapping,
                                     trade_order_type=t)

        trade_order_df = data_factory.fetch_table(trade_request)

        assert not trade_order_df.empty \
               and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
               and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

        ### Test using DatabaseSourceSQL
        database_source = DatabaseSourceMSSQLServer()

        trade_order_df = database_source.fetch_trade_order_data(start_date, finish_date, ticker,
                                                                database_name=ms_sql_server_trade_data_database_name,
                                                                table_name=trade_order_mapping[t])

        assert not trade_order_df.empty \
               and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
               and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

def test_fetch_market_trade_data_dataframe():
    """Tests downloading of market and trade/order data from dataframe
    """

    from tcapy.data.databasesource import DatabaseSourceCSV

    ### Get market data
    market_loader = Mediator.get_tca_market_trade_loader()

    market_data_store = DatabaseSourceCSV(market_data_database_csv=csv_market_data_store).fetch_market_data(
        ticker=ticker, start_date=start_date, finish_date=finish_date)

    dataframe_trade_order_mapping = OrderedDict()

    for k in csv_trade_order_mapping.keys():
        dataframe_trade_order_mapping[k] = DatabaseSourceCSV(trade_data_database_csv=csv_trade_order_mapping[k]).fetch_trade_order_data(
            ticker=ticker, start_date=start_date, finish_date=finish_date)

    # for a high level trade data request, we need to use TCA request, because it usually involves some
    # market data download (we are assuming that the market data is being downloaded from our arctic database)
    # eg. for converting notionals to reporting currency
    tca_request = TCARequest(
        start_date=start_date, finish_date=finish_date, ticker=ticker,
        trade_data_store='dataframe', market_data_store=market_data_store,
        trade_order_mapping=dataframe_trade_order_mapping
    )

    for t in trade_order_list:
        trade_order_df = market_loader.get_trade_order_data(tca_request, t)

        try:
            trade_order_df = Mediator.get_volatile_cache().get_dataframe_handle(trade_order_df)
        except:
            pass

        assert not trade_order_df.empty \
               and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
               and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

    ### Test using DataFactory and DatabaseSource
    from tcapy.data.datafactory import DataFactory

    data_factory = DataFactory()

    for t in trade_order_list:
        ### Test using DataFactory
        trade_request = TradeRequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                     data_store='dataframe', trade_order_mapping=dataframe_trade_order_mapping,
                                     trade_order_type=t)

        trade_order_df = data_factory.fetch_table(trade_request)

        assert not trade_order_df.empty \
                          and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
                          and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

        ### Test using DatabaseSourceDataFrame
        from tcapy.data.databasesource import DatabaseSourceDataFrame

        database_source = DatabaseSourceDataFrame()

        trade_order_df = database_source.fetch_trade_order_data(start_date, finish_date, ticker,
                                                          table_name=dataframe_trade_order_mapping[t])

        assert not trade_order_df.empty \
                             and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
                             and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

### Arctic/KDB/InfluxDB #################################################################################################

def _get_db_market_data_store():
    market_data_store_list = [];

    if run_arctic_tests:
        market_data_store_list.append(arctic_market_data_store)

    if run_kdb_tests:
        market_data_store_list.append(kdb_market_data_store)

    if run_influx_db_tests:
        market_data_store_list.append(influxdb_market_data_store)

    return market_data_store_list

def test_fetch_market_data_db():
    """Tests that we can fetch data from Arctic/KDB/InfluxDB. Note you need to populate the database first before running this for
    the desired dates.
    """
    market_loader = Mediator.get_tca_market_trade_loader()

    market_data_store_list = _get_db_market_data_store()

    for market_data_store in market_data_store_list:
        market_request = MarketRequest(
            start_date=start_date, finish_date=finish_date, ticker=ticker, data_store=market_data_store)

        market_df = market_loader.get_market_data(market_request)

        try:
            market_df = Mediator.get_volatile_cache().get_dataframe_handle(market_df)
        except:
            pass

        assert not(market_df.empty) \
               and market_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
               and market_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

        market_request.start_date = invalid_start_date; market_request.finish_date = invalid_finish_date

        market_empty_df = market_loader.get_market_data(market_request)
        
        try:
            market_empty_df = Mediator.get_volatile_cache().get_dataframe_handle(market_empty_df)
        except:
            pass

        assert market_empty_df.empty

if __name__ == '__main__':
    test_fetch_market_trade_data_csv()

    test_sql_server_connection()
    test_fetch_trade_data_ms_sql_server()
    test_fetch_market_trade_data_dataframe()

    # import pytest; pytest.main()
