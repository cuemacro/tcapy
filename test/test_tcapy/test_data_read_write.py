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

import datetime
import os
from collections import OrderedDict

import pandas as pd
from pandas.testing import assert_frame_equal

from tcapy.util.timeseries import TimeSeriesOps
from tcapy.util.utilfunc import UtilFunc
from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager

from tcapy.analysis.tcarequest import TCARequest
from tcapy.data.datafactory import MarketRequest, TradeRequest

from tcapy.data.databasesource import DatabaseSourceCSVBinary as DatabaseSourceCSV
from tcapy.data.databasesource import \
    DatabaseSourceMSSQLServer, DatabaseSourceMySQL, DatabaseSourceSQLite, \
    DatabaseSourceArctic, DatabaseSourceKDB, DatabaseSourceInfluxDB, DatabaseSourcePyStore

from tcapy.util.mediator import Mediator
from tcapy.util.customexceptions import *

from test.config import *

logger = LoggerManager().getLogger(__name__)

constants = Constants()

logger.info('Make sure you have created folder ' + constants.csv_folder + ' & ' + constants.temp_data_folder +
            ' otherwise tests will fail')

Mediator.get_volatile_cache().clear_cache()

########################################################################################################################
# YOU MAY NEED TO CHANGE THESE

# For market data
run_arctic_tests = True
run_pystore_tests = False
run_influx_db_tests = False
run_kdb_tests = False

# For trade data
run_ms_sql_server_tests = False
run_mysql_server_tests = True
run_sqlite_server_tests = True

start_date = '26 Apr 2017'
finish_date = '05 Jun 2017'
ticker = 'EURUSD'

########################################################################################################################
#### WRITING DATA ######################################################################################################
########################################################################################################################

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
    """Tests we can delete a section of a data for a particular time section
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

        # Read back data from database (will exclude the deleted records)
        market_df_new = database_source.fetch_market_data(start_date=db_start_date, finish_date=db_finish_date,
                                                          ticker=ticker,
                                                          table_name=test_harness_arctic_market_data_table)

        # Sort columns so they are same order
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
        csv_folder = resource('csv_arctic_mult')

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
    test_harness_trade_data_store_list = []

    if run_ms_sql_server_tests:
        database_source_list.append(DatabaseSourceMSSQLServer())
        test_harness_trade_database_list.append(test_harness_ms_sql_server_trade_data_database)
        test_harness_trade_data_store_list.append(test_harness_ms_sql_server_trade_data_store)

    if run_mysql_server_tests:
        database_source_list.append(DatabaseSourceMySQL())
        test_harness_trade_database_list.append(test_harness_mysql_trade_data_database)
        test_harness_trade_data_store_list.append(test_harness_mysql_trade_data_store)

    if run_sqlite_server_tests:
        database_source_list.append(DatabaseSourceSQLite())
        test_harness_trade_database_list.append(test_harness_sqlite_trade_data_database)
        test_harness_trade_data_store_list.append(test_harness_sqlite_trade_data_store)

    return database_source_list, test_harness_trade_database_list, test_harness_trade_data_store_list

def test_write_trade_data_sql():
    """Tests that trade data can be read from CSV and dumped to various SQL dialect
    """

    for database_source, test_harness_trade_database, test_harness_data_store in zip(*_get_db_trade_database_source()):

        for t in trade_order_list:
            # Dump trade_df to SQL test harness database and overwrite
            database_source.convert_csv_to_table(csv_trade_order_mapping[t], None,
                                                 (sql_trade_order_mapping[test_harness_data_store])[t],
                                                 database_name=test_harness_trade_database,
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


# ### KDB/InfluxDB #######################################################################################################

def _get_db_market_database_source():
    database_source_list = [];
    test_harness_market_data_table_list = [];
    test_harness_data_store_list = []

    if run_kdb_tests:
        database_source_list.append(DatabaseSourceKDB(postfix='testharness'))
        test_harness_market_data_table_list.append(test_harness_kdb_market_data_table)
        test_harness_data_store_list.append(test_harness_kdb_market_data_store)

    if run_influx_db_tests:
        database_source_list.append(DatabaseSourceInfluxDB(postfix='testharness'))
        test_harness_market_data_table_list.append(test_harness_influxdb_market_data_table)
        test_harness_data_store_list.append(test_harness_influxdb_market_data_store)

    if run_pystore_tests:
        database_source_list.append(DatabaseSourcePyStore(postfix='testharness'))
        test_harness_market_data_table_list.append(test_harness_pystore_market_data_table)
        test_harness_data_store_list.append(test_harness_pystore_market_data_store)

    return database_source_list, test_harness_market_data_table_list, test_harness_data_store_list

def test_write_market_data_db():
    """Tests we can write market data to KDB/Influxdb/PyStore
    """
    market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    for database_source, test_harness_market_data_table, test_harness_data_store in zip(*_get_db_market_database_source()):

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

    market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    for database_source, test_harness_market_data_table, test_harness_data_store in zip(*_get_db_market_database_source()):

        ### Test we can append (non-overlapping) data to InfluxDB/KDB/PyStore
        db_start_date = '01 Jan 2016'; db_finish_date = pd.Timestamp(datetime.datetime.utcnow())

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

    for database_source, test_harness_market_data_table, _ in zip(*_get_db_market_database_source()):

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

########################################################################################################################
#### READING DATA ######################################################################################################
########################################################################################################################
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
    # eg. for converting notionals to reporting currency)
    tca_request = TCARequest(
        start_date=start_date, finish_date=finish_date, ticker=ticker,
        trade_data_store='csv', market_data_store=test_harness_arctic_market_data_store,
        trade_order_mapping=csv_trade_order_mapping,
        market_data_database_table=test_harness_arctic_market_data_table
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
        database_source = DatabaseSourceCSV()

        trade_order_df = database_source.fetch_trade_order_data(start_date, finish_date, ticker,
                                                          table_name=csv_trade_order_mapping[t])

        assert not trade_order_df.empty \
                             and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
                             and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')


def test_trade_data_connection():
    """Tests the connection with the trade databases
    """
    for database_source, test_harness_trade_database, _ in zip(*_get_db_trade_database_source()):

        engine, connection_expression = database_source._get_database_engine(database_name=test_harness_trade_database)
        connection = engine.connect()

        # Try running simple query (getting the table names of the database)
        logger.info("Table names = " + str(engine.table_names()))
        connection.close()

        assert connection is not None

### SQL dialects #######################################################################################################

def _get_db_trade_database_source():
    database_source_list = [];
    test_harness_trade_database_list = [];
    test_harness_trade_data_store_list = []

    if run_ms_sql_server_tests:
        database_source_list.append(DatabaseSourceMSSQLServer())
        test_harness_trade_database_list.append(test_harness_ms_sql_server_trade_data_database)
        test_harness_trade_data_store_list.append(test_harness_ms_sql_server_trade_data_store)

    if run_mysql_server_tests:
        database_source_list.append(DatabaseSourceMySQL())
        test_harness_trade_database_list.append(test_harness_mysql_trade_data_database)
        test_harness_trade_data_store_list.append(test_harness_mysql_trade_data_store)

    if run_sqlite_server_tests:
        database_source_list.append(DatabaseSourceSQLite())
        test_harness_trade_database_list.append(test_harness_sqlite_trade_data_database)
        test_harness_trade_data_store_list.append(test_harness_sqlite_trade_data_store)

    return database_source_list, test_harness_trade_database_list, test_harness_trade_data_store_list

def test_fetch_trade_data_sql():
    """Tests that we can fetch data from the Microsoft SQL Server database. Note you need to populate the database
    first before running this for the desired dates.
    """

    from tcapy.data.datafactory import DataFactory

    ### Test using TCAMarketTradeLoader
    market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    # database_source_list, test_harness_trade_database_list, test_harness_trade_data_store_list =

    for database_source, test_harness_trade_database, test_harness_trade_data_store in zip(*_get_db_trade_database_source()):

        for t in trade_order_list:
            trade_order_mapping = {t : sql_trade_order_mapping[test_harness_trade_data_store][t]}

            trade_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                                       trade_data_store=test_harness_trade_data_store, trade_order_mapping=trade_order_mapping,
                                       trade_data_database_name=test_harness_trade_database,
                                       market_data_store=test_harness_arctic_market_data_store,
                                       market_data_database_table=test_harness_arctic_market_data_table,
                                       use_multithreading=use_multithreading)

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
                                         data_store=test_harness_trade_data_store,
                                         trade_data_database_name=test_harness_trade_database,
                                         trade_order_mapping=trade_order_mapping,
                                         trade_order_type=t)

            trade_order_df = data_factory.fetch_table(trade_request)

            assert not trade_order_df.empty \
                   and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
                   and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

            ### Test using DatabaseSource
            trade_order_df = database_source.fetch_trade_order_data(start_date, finish_date, ticker,
                                                                    database_name=test_harness_trade_database,
                                                                    table_name=trade_order_mapping[t])

            assert not trade_order_df.empty \
                   and trade_order_df.index[0] >= pd.Timestamp(start_date).tz_localize('utc') \
                   and trade_order_df.index[-1] <= pd.Timestamp(finish_date).tz_localize('utc')

def test_fetch_market_trade_data_dataframe():
    """Tests downloading of market and trade/order data from dataframe
    """

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
    market_data_database_table_list = [];

    if run_arctic_tests:
        market_data_store_list.append(test_harness_arctic_market_data_store)
        market_data_database_table_list.append(test_harness_arctic_market_data_table)

    if run_kdb_tests:
        market_data_store_list.append(test_harness_kdb_market_data_store)
        market_data_database_table_list.append(test_harness_kdb_market_data_table)

    if run_influx_db_tests:
        market_data_store_list.append(test_harness_influxdb_market_data_store)
        market_data_database_table_list.append(test_harness_influxdb_market_data_table)

    return market_data_store_list, market_data_database_table_list

def test_fetch_market_data_db():
    """Tests that we can fetch data from Arctic/KDB/InfluxDB. Note you need to populate the database first before running this for
    the desired dates.
    """
    market_loader = Mediator.get_tca_market_trade_loader()

    market_data_store_list, market_data_database_table_list  = _get_db_market_data_store()

    for market_data_store, market_data_database_table in zip(market_data_store_list, market_data_database_table_list):
        market_request = MarketRequest(
            start_date=start_date, finish_date=finish_date, ticker=ticker, data_store=market_data_store,
            market_data_database_table=market_data_database_table)

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
