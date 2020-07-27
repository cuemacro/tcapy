import os
import pandas as pd

from collections import OrderedDict

use_multithreading = False

def resource(name):
    return os.path.join(os.path.dirname(__file__), "resources", name)

def read_pd(name, **kwargs):
    return pd.read_csv(resource(name), **kwargs)

tcapy_version = 'test_tcapy'

#### Trade/order mapping to database tables/CSVs

trade_order_list = ['trade_df', 'order_df']

sql_trade_order_mapping = {
    'ms_sql_server' :   {'trade_df' : '[dbo].[test_trade]',      # Name of table which has broker messages to client
                         'order_df' : '[dbo].[test_order]'},     # Name of table which has orders from client
    'mysql':            {'trade_df': 'trade_database_test_harness.trade',   # Name of table which has broker messages to client
                         'order_df': 'trade_database_test_harness.order'},  # Name of table which has orders from client
    'sqlite':           {'trade_df': 'test_trade_table',  # Name of table which has broker messages to client
                         'order_df': 'test_order_table'}  # Name of table which has orders from client
}

csv_trade_order_mapping = {'trade_df' : resource('small_test_trade_df.csv'),
                           'order_df' : resource('small_test_order_df.csv')}

#### Flat file market data (Parquet)

csv_market_data_store = resource('small_test_market_df.parquet')
csv_reverse_market_data_store = resource('small_test_market_df_reverse.parquet')

#### Database tables and parameters

# Market data parameters for tables/databases
test_harness_arctic_market_data_table = 'market_data_table_test_harness'
test_harness_arctic_market_data_store = 'arctic-testharness'

test_harness_kdb_market_data_table = 'market_data_table_test_harness'
test_harness_kdb_market_data_store = 'kdb-testharness'

test_harness_influxdb_market_data_table = 'market_data_table_test_harness' # InfluxDB database
test_harness_influxdb_market_data_store = 'influxdb-testharness' # InfluxDB measurement

test_harness_pystore_market_data_table = 'market_data_table_test_harness' # PyStore
test_harness_pystore_market_data_store = 'pystore-testharness' # PyStore folder

# Default format is CHUNK_STORE, so should be last, so we can read in later
arctic_lib_type = ['TICK_STORE', 'VERSION_STORE', 'CHUNK_STORE']

# Trade data parameters
test_harness_ms_sql_server_trade_data_database = 'trade_database_test_harness'
test_harness_ms_sql_server_trade_data_store = 'ms_sql_server'
test_harness_mysql_trade_data_database = 'trade_database_test_harness'
test_harness_mysql_trade_data_store = 'mysql'
test_harness_sqlite_trade_data_database = resource('trade_database_test_harness.db')
test_harness_sqlite_trade_data_store = 'sqlite'

#### Tolerance for errors

eps = 10 ** -5

invalid_start_date = '01 Jan 1999'
invalid_finish_date = '01 Feb 1999'

# So we are not specifically testing the database of tcapy - can instead use CSV in the test harness folder
use_trade_test_csv = False
use_market_test_csv = False

use_multithreading = False

if use_market_test_csv:
    market_data_store = csv_market_data_store

if use_trade_test_csv:
    trade_data_store = 'csv'

    trade_order_mapping = csv_trade_order_mapping
    venue_filter = 'venue1'
else:
    trade_order_mapping = sql_trade_order_mapping

