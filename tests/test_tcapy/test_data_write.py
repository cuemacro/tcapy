"""Tests functions for writing market data to Arctic, InfluxDB, KDB etc and trade/order data to SQL Server.

It writes test trade/market data to a test database in all instances.
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
from tcapy.util.timeseries import TimeSeriesOps
from tcapy.util.utilfunc import UtilFunc

from tcapy.analysis.tcarequest import MarketRequest

from tcapy.data.databasesource import DatabaseSourceCSVBinary as DatabaseSourceCSV
from tcapy.data.databasesource import \
    DatabaseSourceMSSQLServer, DatabaseSourceMySQL, DatabaseSourceSQLite, \
    DatabaseSourceArctic, DatabaseSourceKDB, DatabaseSourceInfluxDB, DatabaseSourcePyStore

import datetime

from collections import OrderedDict

from tcapy.util.customexceptions import *

logger = LoggerManager().getLogger(__name__)

constants = Constants()

from tcapy.util.mediator import Mediator

tcapy_version = constants.tcapy_version

# check that your database has market and trade data for these before running the test

logger.info('Make sure you have created folder ' + constants.csv_folder + ' & ' + constants.temp_data_folder +
            ' otherwise tests will fail')

########################################################################################################################
# YOU MAY NEED TO CHANGE THESE

start_date = '26 Apr 2017'
finish_date = '05 Jun 2017'
ticker = 'EURUSD'

# Market data parameters for tables/databases
test_harness_arctic_market_data_table = 'market_data_table_test_harness'
test_harness_arctic_market_data_store = 'arctic-testharness'

test_harness_kdb_market_data_table = 'market_data_table_test_harness'
test_harness_kdb_data_store = 'kdb-testharness'

test_harness_influxdb_market_data_table = 'market_data_table_test_harness' # InfluxDB database
test_harness_influxdb_data_store = 'influxdb-testharness' # InfluxDB measurement

test_harness_pystore_market_data_table = 'market_data_table_test_harness' # InfluxDB database
test_harness_pystore_data_store = 'pystore-testharness' # InfluxDB measurement

arctic_lib_type = ['CHUNK_STORE', 'TICK_STORE', 'VERSION_STORE']

# Trade data for tables/databases
# Note: make sure you have already created the test_harness databases beforehand
test_harness_ms_sql_trade_data_database = 'trade_database_test_harness'
test_harness_ms_sql_data_store = 'ms_sql_server'

test_harness_mysql_trade_data_database = 'trade_database_test_harness'
test_harness_mysql_data_store = 'mysql'

test_harness_sqlite_trade_data_database = '/home/tcapyuser/db/trade_database_test_harness.db'
test_harness_sqlite_data_store = 'sqlite'

# Mainly just to speed up tests - note: you will need to generate the Parquet files using convert_csv_to_h5.py from the CSVs
use_parquet_market_files = True

run_arctic_tests = True
run_pystore_tests = False
run_influx_db_tests = False
run_kdb_tests = True

run_ms_sql_server_tests = True
run_mysql_server_tests = True
run_sqlite_server_tests = True

########################################################################################################################
folder = Constants().test_data_harness_folder

trade_order_list = ['trade_df', 'order_df']

sql_trade_order_mapping = {
    'ms_sql_server' :   {'trade_df' : '[dbo].[trade]',      # Name of table which has broker messages to client
                         'order_df' : '[dbo].[order]'},             # Name of table which has orders from client
    'mysql':            {'trade_df': 'trade_database_test_harness.trade',   # Name of table which has broker messages to client
                         'order_df': 'trade_database_test_harness.order'},  # Name of table which has orders from client
    'sqlite':           {'trade_df': 'trade_table',  # Name of table which has broker messages to client
                         'order_df': 'order_table'}  # Name of table which has orders from client
}

eps = 10 ** -5

invalid_start_date = '01 Jan 1999'
invalid_finish_date = '01 Feb 1999'

if use_parquet_market_files:
    csv_market_data_store = os.path.join(folder, 'small_test_market_df.parquet')
    csv_reverse_market_data_store = os.path.join(folder, 'small_test_market_df_reverse.parquet')
else:
    csv_market_data_store = os.path.join(folder, 'small_test_market_df.csv.gz')
    csv_reverse_market_data_store = os.path.join(folder, 'small_test_market_df_reverse.csv.gz')

csv_trade_order_mapping = OrderedDict([('trade_df', os.path.join(folder, 'small_test_trade_df.csv')),
                                       ('order_df', os.path.join(folder, 'small_test_order_df.csv'))])

use_multithreading = False

### Arctic #############################################################################################################

def test_write_market_data_arctic():
    """Tests we can write market data to Arctic
    """
    if not (run_arctic_tests): return

    market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    ### Test we can read data from CSV and dump to Arctic (and when read back it matches CSV)
    db_start_date = '01 Jan 2016'; db_finish_date = pd.Timestamp(datetime.datetime.utcnow())

    replace_append = ['replace', 'append']

    # Check first when replacing full table and then appending
    for a in arctic_lib_type:
        for i in replace_append:
            database_source = DatabaseSourceArctic(postfix='testharness', arctic_lib_type=a)

            # Write CSV to Arctic
            database_source.convert_csv_to_table(csv_market_data_store, ticker,
                                                 test_harness_arctic_market_data_table,
                                                 if_exists_table=i,
                                                 if_exists_ticker='replace', market_trade_data='market',
                                                 remove_duplicates=False)

            # Fetch data directly from CSV
            database_source_csv = DatabaseSourceCSV(market_data_database_csv=csv_market_data_store)

            market_df_csv = database_source_csv.fetch_market_data(
                start_date=db_start_date, finish_date=db_finish_date, ticker=ticker)

            # Read back data from Arctic and compare with CSV
            market_request = MarketRequest(start_date=db_start_date, finish_date=db_finish_date, ticker=ticker,
                                           data_store=database_source, # test_harness_arctic_market_data_store,
                                           market_data_database_table=test_harness_arctic_market_data_table)

            market_df_load = market_loader.get_market_data(market_request=market_request)

            diff_df = market_df_csv['mid'] - market_df_load['mid']

            diff_df.to_csv('test' + i + '.csv')
            assert all(diff_df < eps)

def test_append_market_data_arctic():
    """Tests we can append market data to arctic (we will have already written data to the test harness database)
    """
    if not (run_arctic_tests): return

    market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    ### Test we can append (non-overlapping) data to Arctic
    arctic_start_date = '01 Jan 2016'; arctic_finish_date = pd.Timestamp(datetime.datetime.utcnow())

    # Load data from CSV for comparison later
    database_source_csv = DatabaseSourceCSV(market_data_database_csv=csv_market_data_store)

    market_df_csv = database_source_csv.fetch_market_data(
        start_date=arctic_start_date, finish_date=arctic_finish_date, ticker=ticker)

    market_df_list = TimeSeriesOps().split_array_chunks(market_df_csv, chunks=2)

    for a in arctic_lib_type:

        database_source = DatabaseSourceArctic(postfix='testharness', arctic_lib_type=a)

        market_df_lower = market_df_list[0];
        market_df_higher = market_df_list[1]

        database_source.append_market_data(market_df_lower, ticker, table_name=test_harness_arctic_market_data_table,
                                           if_exists_table='replace', if_exists_ticker='replace', remove_duplicates=False)

        overlap_error = False

        ## Try to append overlapping data (this will fail!)
        try:
            database_source.append_market_data(market_df_lower, ticker,
                                               table_name=test_harness_arctic_market_data_table,
                                               if_exists_table='append', if_exists_ticker='append', remove_duplicates=False)
        except ErrorWritingOverlapDataException as e:
            overlap_error = True

        assert overlap_error

        # Append non-overlapping data which follows (writing overlapping data into Arctic will mess up the datastore!)
        database_source.append_market_data(market_df_higher, ticker, table_name=test_harness_arctic_market_data_table,
                                           if_exists_table='append', if_exists_ticker='append', remove_duplicates=False)

        # Use this market request later when reading back from Arctic
        market_request = MarketRequest(start_date=arctic_start_date, finish_date=arctic_finish_date, ticker=ticker,
                                       data_store=database_source,
                                       market_data_database_table=test_harness_arctic_market_data_table)

        market_df_all_read_back = market_loader.get_market_data(market_request=market_request)

        diff_df = abs(market_df_all_read_back['mid'] - market_df_csv['mid'])

        outside_bounds = diff_df[diff_df >= eps]

        assert len(outside_bounds) == 0


def test_delete_market_data_arctic():
    """Tests we can delete a section of a data for a particular
    """
    if not (run_arctic_tests): return

    for a in arctic_lib_type:
        database_source = DatabaseSourceArctic(postfix='testharness', arctic_lib_type=a)

        ### Test we can read data from CSV and dump to Arctic (and when read back it matches CSV)
        db_start_date = '01 Jan 2016';
        db_finish_date = pd.Timestamp(datetime.datetime.utcnow())

        # Write test market data CSV to arctic first
        database_source.convert_csv_to_table(csv_market_data_store, ticker,
                                             test_harness_arctic_market_data_table,
                                             if_exists_table='replace',
                                             if_exists_ticker='replace', market_trade_data='market',
                                             remove_duplicates=False)

        db_start_cut_off = '26 Apr 2017 00:00';
        db_finish_cut_off = '27 Apr 2017 00:50'

        market_df_old = database_source.fetch_market_data(
            start_date=db_start_date, finish_date=db_finish_date, ticker=ticker, table_name=test_harness_arctic_market_data_table)

        market_df_old = market_df_old.loc[
            (market_df_old.index <= db_start_cut_off) | (market_df_old.index >= db_finish_cut_off)]

        # Do it with Arctic (note: underneath this will just use pandas, as can't do on database deletion with Arctic)
        database_source.delete_market_data(ticker, start_date=db_start_cut_off, finish_date=db_finish_cut_off,
                                           table_name=test_harness_arctic_market_data_table)

        # read back data from database (will exclude the deleted records)
        market_df_new = database_source.fetch_market_data(start_date=db_start_date, finish_date=db_finish_date,
                                                          ticker=ticker,
                                                          table_name=test_harness_arctic_market_data_table)

        # sort columns so they are same order
        market_df_old = market_df_old.sort_index(axis=1)
        market_df_new = market_df_new.sort_index(axis=1)

        assert_frame_equal(market_df_old, market_df_new)

def test_write_chunked_market_data_arctic():
    """For very large CSV files we might need to read them in chunks. tcapy supports this and also supports CSVs
    which are sorted in reverse (ie. descending). We need to enable chunking and reverse reading with flags.

    This tests whether chunked data is written correctly to Arctic, comparing it with that read from CSV directly
    """

    if not (run_arctic_tests): return

    market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    arctic_start_date = '01 Jan 2016'; arctic_finish_date = pd.Timestamp(datetime.datetime.utcnow())

    # Load data from CSVs directly (for comparison later)
    market_df_csv_desc = DatabaseSourceCSV(market_data_database_csv=csv_reverse_market_data_store).fetch_market_data(
        start_date=arctic_start_date, finish_date=arctic_finish_date, ticker=ticker)

    market_df_csv_asc = DatabaseSourceCSV(market_data_database_csv=csv_market_data_store).fetch_market_data(
        start_date=arctic_start_date, finish_date=arctic_finish_date, ticker=ticker)

    for a in arctic_lib_type:
        database_source = DatabaseSourceArctic(postfix='testharness', arctic_lib_type=a)

        ### Write CSV data to Arctic which is sorted ascending (default!)
        database_source.convert_csv_to_table(csv_market_data_store, ticker,
                                             test_harness_arctic_market_data_table,
                                             if_exists_table='replace',
                                             if_exists_ticker='replace', market_trade_data='market',
                                             csv_read_chunksize=100000, remove_duplicates=False)

        market_request = MarketRequest(start_date=arctic_start_date, finish_date=arctic_finish_date, ticker=ticker,
                                       data_store=database_source,
                                       market_data_database_table=test_harness_arctic_market_data_table)

        market_df_load = market_loader.get_market_data(market_request=market_request)

        # Compare reading directly from the CSV vs. reading back from arctic
        assert all(market_df_csv_asc['mid'] - market_df_load['mid'] < eps)

        ### Write CSV data to Arctic which is sorted descending
        database_source.convert_csv_to_table(csv_reverse_market_data_store, ticker,
                                             test_harness_arctic_market_data_table,
                                             if_exists_table='append',
                                             if_exists_ticker='replace', market_trade_data='market',
                                             csv_read_chunksize=100000, read_in_reverse=True, remove_duplicates=False)

        market_request = MarketRequest(start_date=arctic_start_date, finish_date=arctic_finish_date, ticker=ticker,
                                       data_store=database_source,
                                       market_data_database_table=test_harness_arctic_market_data_table)

        market_df_load = market_loader.get_market_data(market_request=market_request)

        # Compare reading directly from the CSV vs. reading back from arctic
        assert all(market_df_csv_desc['mid'] - market_df_load['mid'] < eps)

def test_write_multiple_wildcard_market_data_csvs_arctic():
    """Tests we can write sequential market data CSVs (or HDF5) whose path has been specified by a wildcard (eg. EURUSD*.csv).
    It is assumed that the CSVs are in chronological orders, from their filenames.
    """
    if not (run_arctic_tests): return

    market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    arctic_start_date = '01 Jan 2016';
    arctic_finish_date = pd.Timestamp(datetime.datetime.utcnow())

    for a in arctic_lib_type:
        database_source = DatabaseSourceArctic(postfix='testharness', arctic_lib_type=a)

        ### Read CSV data which is sorted ascending (default!)
        database_source.convert_csv_to_table(csv_market_data_store, ticker,
                                             test_harness_arctic_market_data_table,
                                             if_exists_table='replace',
                                             if_exists_ticker='replace', market_trade_data='market',
                                             csv_read_chunksize=10 ** 6, remove_duplicates=False)

        database_source_csv = DatabaseSourceCSV(market_data_database_csv=csv_market_data_store)

        market_df_csv = database_source_csv.fetch_market_data(
            start_date=arctic_start_date, finish_date=arctic_finish_date, ticker=ticker)

        # Prepare the CSV folder first
        csv_folder = os.path.join(constants.test_data_harness_folder, 'csv_arctic_mult')

        # Empty the CSV test harness folder, where we shall dump the mini CSVs
        UtilFunc().forcibly_create_empty_folder(csv_folder)

        # Split the CSV file into several mini CSV files (and also HDF5 files)
        market_df_list = TimeSeriesOps().split_array_chunks(market_df_csv, chunks=3)

        chunk_no = 0

        for m in market_df_list:
            m.to_csv(os.path.join(csv_folder, "EURUSD" + str(chunk_no) + '.csv'))
            UtilFunc().write_dataframe_to_binary(m, os.path.join(csv_folder, "EURUSD" + str(chunk_no) + '.parquet'),
                                                 format='parquet')

            chunk_no = chunk_no + 1

        file_ext = ['csv', 'parquet']

        for f in file_ext:
            ### Read CSV data from the mini CSVs (using wildcard char) and dump to Arctic
            database_source.convert_csv_to_table(os.path.join(csv_folder, "EURUSD*." + f), ticker,
                                                 test_harness_arctic_market_data_table,
                                                 if_exists_table='append',
                                                 if_exists_ticker='replace', market_trade_data='market',
                                                 csv_read_chunksize=10 ** 6, remove_duplicates=False)

            market_request = MarketRequest(start_date=arctic_start_date, finish_date=arctic_finish_date, ticker=ticker,
                                           data_store=database_source,
                                           market_data_database_table=test_harness_arctic_market_data_table)

            # Read back from Arctic
            market_df_load = market_loader.get_market_data(market_request=market_request)

            # Compare reading directly from the original large CSV vs. reading back from arctic (which was dumped from split CSVs)
            diff_df = abs(market_df_load['mid'] - market_df_csv['mid'])

            outside_bounds = diff_df[diff_df >= eps]

            assert len(outside_bounds) == 0
            # assert all(market_df_csv['mid'] - market_df_load['mid'] < eps)

### SQL dialects #######################################################################################################

def _get_db_trade_database_source():
    database_source_list = [];
    test_harness_trade_database_list = [];
    test_harness_data_store_list = []

    if run_ms_sql_server_tests:
        database_source_list.append(DatabaseSourceMSSQLServer())
        test_harness_trade_database_list.append(test_harness_ms_sql_trade_data_database)
        test_harness_data_store_list.append(test_harness_ms_sql_data_store)

    if run_mysql_server_tests:
        database_source_list.append(DatabaseSourceMySQL())
        test_harness_trade_database_list.append(test_harness_mysql_trade_data_database)
        test_harness_data_store_list.append(test_harness_mysql_data_store)

    if run_sqlite_server_tests:
        database_source_list.append(DatabaseSourceSQLite())
        test_harness_trade_database_list.append(test_harness_sqlite_trade_data_database)
        test_harness_data_store_list.append(test_harness_sqlite_data_store)

    return database_source_list, test_harness_trade_database_list, test_harness_data_store_list

def test_write_trade_data_sql():
    """Tests that trade data can be read from CSV and dumped to various SQL dialect
    """

    database_source_list, test_harness_trade_database_list, test_harness_data_store_list = _get_db_trade_database_source()

    for i in range(0, len(database_source_list)):

        database_source = database_source_list[i]

        test_harness_trade_database = test_harness_trade_database_list[i]
        test_harness_data_store = test_harness_data_store_list[i]

        for t in trade_order_list:
            # Dump trade_df to SQL test harness database and overwrite
            database_source.convert_csv_to_table(csv_trade_order_mapping[t], None,
                                                 (sql_trade_order_mapping[test_harness_data_store])[t],
                                                 test_harness_trade_database,
                                                 if_exists_table='replace', market_trade_data='trade')

            trade_order_df_sql = database_source.fetch_trade_order_data(
                start_date=start_date, finish_date=finish_date, ticker=ticker,
                table_name=sql_trade_order_mapping[test_harness_data_store][t],
                database_name=test_harness_trade_database)

            database_source_csv = DatabaseSourceCSV()

            trade_order_df_csv = database_source_csv.fetch_trade_order_data(
                start_date=start_date, finish_date=finish_date, ticker=ticker, table_name=csv_trade_order_mapping[t])

            comp_fields = ['executed_price', 'notional', 'side']

            # Check that the data read back from SQL database matches that from the original CSV
            for c in comp_fields:
                if c in trade_order_df_sql.columns and c in trade_order_df_csv.columns:
                    exec_sql = trade_order_df_sql[c]#.dropna()
                    exec_csv = trade_order_df_csv[c]#.dropna()

                    exec_diff = exec_sql - exec_csv

                    assert all(exec_diff < eps)


### KDB/InfluxDB #######################################################################################################

def _get_db_market_database_source():
    database_source_list = [];
    test_harness_market_data_table_list = [];
    test_harness_data_store_list = []

    if run_kdb_tests:
        database_source_list.append(DatabaseSourceKDB(postfix='testharness'))
        test_harness_market_data_table_list.append(test_harness_kdb_market_data_table)
        test_harness_data_store_list.append(test_harness_kdb_data_store)

    if run_influx_db_tests:
        database_source_list.append(DatabaseSourceInfluxDB(postfix='testharness'))
        test_harness_market_data_table_list.append(test_harness_influxdb_market_data_table)
        test_harness_data_store_list.append(test_harness_influxdb_data_store)

    if run_pystore_tests:
        database_source_list.append(DatabaseSourcePyStore(postfix='testharness'))
        test_harness_market_data_table_list.append(test_harness_pystore_market_data_table)
        test_harness_data_store_list.append(test_harness_pystore_data_store)

    return database_source_list, test_harness_market_data_table_list, test_harness_data_store_list

def test_write_market_data_db():
    """Tests we can write market data to KDB/Influxdb/PyStore
    """

    database_source_list, test_harness_market_data_table_list, test_harness_data_store_list = _get_db_market_database_source()

    market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    for i in range(0, len(database_source_list)):

        database_source = database_source_list[i]
        test_harness_market_data_table = test_harness_market_data_table_list[i]
        test_harness_data_store = test_harness_data_store_list[i]

        ### Test we can read data from CSV and dump to InfluxDB/KDB/PyStore (and when read back it matches CSV)
        db_start_date = '01 Jan 2016'; db_finish_date = pd.Timestamp(datetime.datetime.utcnow())

        replace_append = ['replace', 'append']

        database_source_csv = DatabaseSourceCSV(market_data_database_csv=csv_market_data_store)

        market_df_csv = database_source_csv.fetch_market_data(
             start_date=db_start_date, finish_date=db_finish_date, ticker=ticker)

        # Check first when replacing full table and then appending (will still replace ticker though)
        for i in replace_append:

            database_source.convert_csv_to_table(csv_market_data_store, ticker,
                 test_harness_market_data_table,
                 if_exists_table=i,
                 if_exists_ticker='replace', market_trade_data='market', remove_duplicates=False)

            market_request = MarketRequest(start_date=db_start_date, finish_date=db_finish_date, ticker=ticker,
                data_store=test_harness_data_store, market_data_database_table=test_harness_market_data_table)

            market_df_load = market_loader.get_market_data(market_request=market_request)

            diff_df = market_df_csv['mid'] - market_df_load['mid']

            assert all(diff_df < eps)

def test_append_market_data_db():
    """Tests we can append market data to KDB/InfluxDB/PyStore.
    """
    database_source_list, test_harness_market_data_table_list, test_harness_data_store_list = _get_db_market_database_source()

    market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    for i in range(0, len(database_source_list)):

        database_source = database_source_list[i]
        test_harness_market_data_table = test_harness_market_data_table_list[i]
        test_harness_data_store = test_harness_data_store_list[i]

        ### Test we can append (non-overlapping) data to InfluxDB/KDB/PyStore
        db_start_date = '01 Jan 2016'; db_finish_date = pd.Timestamp(datetime.datetime.utcnow())

        # TODO
        market_request = MarketRequest(start_date=db_start_date, finish_date=db_finish_date, ticker=ticker,
                                       data_store=test_harness_data_store, market_data_database_table=test_harness_market_data_table)

        market_df_load = market_loader.get_market_data(market_request=market_request)

        market_df_list = TimeSeriesOps().split_array_chunks(market_df_load, chunks=2)

        market_df_lower = market_df_list[0];
        market_df_higher = market_df_list[1]

        database_source.append_market_data(market_df_lower, ticker, table_name=test_harness_market_data_table,
                                           if_exists_table='replace', if_exists_ticker='replace', remove_duplicates=False)

        overlap_error = False

        ## Try to append overlapping data (this will fail!)
        try:
            database_source.append_market_data(market_df_lower, ticker,
                                               table_name=test_harness_market_data_table,
                                               if_exists_table='append', if_exists_ticker='append', remove_duplicates=False)
        except ErrorWritingOverlapDataException as e:
            overlap_error = True

        assert overlap_error

        # Append non-overlapping data which follows (writing overlapping data can end up with duplicated values - although
        # KDB/InfluxDB/PyStore will allow this)
        database_source.append_market_data(market_df_higher, ticker, table_name=test_harness_market_data_table,
                                           if_exists_table='append', if_exists_ticker='append', remove_duplicates=False)

        market_df_all_read_back = market_loader.get_market_data(market_request=market_request)

        assert all(market_df_all_read_back['mid'] - market_df_load['mid'] < eps)

def test_delete_market_data_db():
    """Tests that we can delete a section of a ticker by start/finish date in KDB/InfluxDB/PyStore
    """

    database_source_list, test_harness_market_data_table_list, _ = _get_db_market_database_source()

    # market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    for i in range(0, len(database_source_list)):
        database_source = database_source_list[i]
        test_harness_market_data_table = test_harness_market_data_table_list[i]
        # test_harness_data_store = test_harness_data_store_list[i]

        ### Test we can read data from CSV and dump to KDB/InfluxDB (and when read back it matches CSV)
        db_start_date = '01 Jan 2016';
        db_finish_date = pd.Timestamp(datetime.datetime.utcnow())

        # Write test market CSV to InfluxDB/KDB/PyStore first replacing any other test data (ie. deleting full folder)
        database_source.convert_csv_to_table(csv_market_data_store, ticker,
                                                 test_harness_market_data_table,
                                                 if_exists_table='replace',
                                                 if_exists_ticker='replace', market_trade_data='market',
                                                 remove_duplicates=False)

        db_start_cut_off = '26 Apr 2017 00:00'; db_finish_cut_off = '27 Apr 2017 00:50'

        # Read back from InfluxDB/KDB/PyStore test database
        market_df_old = database_source.fetch_market_data(
             start_date=db_start_date, finish_date=db_finish_date, ticker=ticker, table_name=test_harness_market_data_table)

        # Now cutaway the bit to delete using pandas
        market_df_old = market_df_old.loc[(market_df_old.index <= db_start_cut_off) | (market_df_old.index >= db_finish_cut_off)]

        # Use our delete method in KDB/InfluxDB and later check if it matches
        database_source.delete_market_data(ticker, start_date=db_start_cut_off, finish_date=db_finish_cut_off,
                                           table_name=test_harness_market_data_table)

        market_df_new = database_source.fetch_market_data(start_date=db_start_date, finish_date=db_finish_date, ticker=ticker,
                                                          table_name=test_harness_market_data_table)

        # Both pandas and KDB/InfluxDB implementation should be the same
        assert_frame_equal(market_df_old, market_df_new)

###
if __name__ == '__main__':

    # Arctic (tests multiple stores - VERSION_STORE and TICK_STORE)
    test_write_market_data_arctic()
    test_append_market_data_arctic()
    test_delete_market_data_arctic()
    test_write_chunked_market_data_arctic()
    test_write_multiple_wildcard_market_data_csvs_arctic()

    # SQL dialects
    test_write_trade_data_sql()

    # for PyStore/KDB/InfluxDB
    test_write_market_data_db()
    test_append_market_data_db()
    test_delete_market_data_db()

    # import pytest; pytest.main()
