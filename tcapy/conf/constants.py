from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os
import platform
from collections import OrderedDict

import pickle

import ssl

# We can specify different variables if we are in a Docker container or not
# Also accepts environment variables which are denoted with dollar ($) symbols
def docker_var(docker_var, normal_var, default_value=None):

    if default_value is None:
        default_value = normal_var

    # For cases where we are running from Docker
    if 'APP_ENV' in os.environ:
        if os.environ.get('APP_ENV') == 'docker':
            if '$' in str(docker_var):
                if docker_var.replace('$', '') in os.environ:
                    return os.environ.get(docker_var.replace('$', ''))
                else:
                    return default_value

            return docker_var

    # Other cases
    if normal_var is not None:
        if '$' in str(normal_var):
            if normal_var.replace('$', '') in os.environ:
                return os.environ.get(normal_var.replace('$', ''))
            else:
                return default_value

    return normal_var

class Constants(object):
    """The constants of tcapy are stored here. They govern behavior such as: _tickers, settings, markout
    and resampling settings, asset list, logging functionality and chart settings.

    These can be overrriden (in order) for you to specify settings which are unlikely to change very much (and excluding
    usernames/passwords etc.)
    - constantsgen.py
    - constantsuser.py

    For credentials and sensitive data (typically usernames and passwords), we recommend a separate file (or setting them
    as environment variables and referring to those here):
    - constantscredgen.py
    - constantscreduser.py

    (in particular, usernames and passwords which should NEVER be committed to the Git repo - we can also fetch these
    in constantscredgen/constantscreduser from Linux environment variables)

    """
    from os.path import dirname as up

    env = 'default'

    tcapy_version = docker_var('$TCAPY_VERSION', 'gen')  # 'user' for user specific or 'gen' for generic version (also for future usage 'test_tcapy')
    tcapy_provider = docker_var('$TCAPY_PROVIDER', 'internal_tcapy') # Will add external providers

    root_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_folder = os.path.join(os.path.join(os.path.dirname(root_folder), 'log'))

    # Where is the test data held?
    test_data_folder = os.path.join(up(up(os.path.dirname(root_folder))), 'tcapy_tests_data')

    temp_data_folder = os.path.join(os.path.join(up(up(os.path.dirname(root_folder))), 'tcapy_tests_data'), 'temp2')

    # _temp_data_folder = os.path.join(test_data_folder, 'temp')
    temp_large_data_folder = os.path.join(test_data_folder, 'large')

    # For backward compatibility with Python 2
    pickle.DEFAULT_PROTOCOL = 2
    pickle.HIGHEST_PROTOCOL = 2

    # Default format for writing small binary files to disk (eg. by DatabasePopulator, UtilFunc..)
    # 'parquet' (or 'hdf5', but that requires pytables tables Python package installed, which is not done by default)
    binary_default_dump_format = 'parquet'

    # 'snappy' or 'gzip'
    parquet_compression = 'gzip'

    # 'fastparquet' or 'pyarrow' (can have issues with pyarrow in Jupyter)
    parquet_engine = 'fastparquet'

    ###### FOR TEST HARNESS ONLY
    test_data_harness_folder = os.path.join(os.path.dirname(root_folder), 'test/resources')
    test_harness_sql_trade_data_database_name = 'trade_database_test_harness'
    test_harness_tickers = {'EURUSD' : 'EURUSD', 'USDJPY' : 'USDJPY'}

    ###### SETUP ENVIRONMENT VARIABLES ######
    plat = str(platform.platform()).lower()

    if 'linux' in plat:
        generic_plat = 'linux'
    elif 'windows' in plat:
        generic_plat = 'windows'

    default_instrument = 'spot'
    default_asset_class = 'fx'

    friday_close_utc_hour = 22; sunday_open_utc_hour = 20
    friday_close_nyc_hour = 17;

    weekend_period_seconds = 48 * 3600

    ##### Tickers for external data providers ##############################################################################

    ### NCFX
    ncfx_url = docker_var('$NCFX_URL', None)  # "OVERWRITE_IN_ConstantsCred"

    # sample format for NCFX data - 0112 2016100002;0112 2016100003;EURGBP;,CSV,username,password
    ncfx_username = docker_var('$NCFX_USER', None)   # "OVERWRITE_IN_ConstantsCred"
    ncfx_password = docker_var('$NCFX_PASS', None)  # "OVERWRITE_IN_ConstantsCred"
    ncfx_tickers = {'EURUSD': 'EURUSD',  # tcapy ticker name vs. NCFX ticker name (should generally be the same)
                    'GBPUSD': 'GBPUSD',
                    'AUDUSD': 'AUDUSD',
                    'NZDUSD': 'NZDUSD',
                    'USDCAD': 'USDCAD',
                    'USDCHF': 'USDCHF',
                    'EURNOK': 'EURNOK',
                    'EURSEK': 'EURSEK',
                    'USDJPY': 'USDJPY'}

    ncfx_threads = 2
    ncfx_sleep_seconds = 30
    ncfx_retry_times = 20
    ncfx_chunk_min_size = 60     # Minute size for downloading NCFX
    ncfx_data_store = 'arctic-ncfx'

    csv_folder = docker_var('/tmp/csv/', '/tmp/csv/')
    temp_data_folder = docker_var('/tmp/tcapy/', '/tmp/tcapy/')

    ### Dukascopy _tickers
    dukascopy_tickers = {'EURUSD' : 'EURUSD',
                         'GBPUSD' : 'GBPUSD', 'AUDUSD': 'AUDUSD', 'NZDUSD': 'NZDUSD', 'USDCAD' : 'USDCAD', 'USDCHF' : 'USDCHF',
                         'USDJPY' : 'USDJPY',
                         'EURNOK' : 'EURNOK', 'EURSEK': 'EURSEK', 'USDNOK' : 'USDNOK', 'USDSEK' : 'USDSEK',
                         'USDTRY' : 'USDTRY', 'USDZAR': 'USDZAR',
                         'EURPLN' : 'EURPLN', 'USDMXN': 'USDMXN', 'EURHUF' : 'EURHUF', 'EURJPY' : 'EURJPY'}

    dukascopy_data_store = 'arctic-dukascopy'
    dukascopy_threads = 1 # Dukascopy downloader not thread-safe so only use 1!

    ### Eikon tickers
    eikon_tickers = {'EURUSD' : 'EUR=',
                         'GBPUSD' : 'GBP=', 'AUDUSD': 'AUD=', 'NZDUSD': 'NZD=', 'USDCAD' : 'CAD=', 'USDCHF' : 'CHF=',
                         'USDJPY' : 'JPY=',
                         'EURNOK' : 'EURNOK=', 'EURSEK': 'EURSEK=', 'USDNOK' : 'NOK=', 'USDSEK' : 'SEK=',
                         'USDTRY' : 'TRY=', 'USDZAR': 'ZAR=',
                         'EURPLN' : 'EURPLN=', 'USDMXN': 'MXN=', 'EURHUF' : 'EURHUF=', 'EURJPY' : 'EURJPY='}

    eikon_data_store = 'arctic-eikon'
    eikon_api_key = 'TYPE_HERE'
    eikon_threads = 2

    ##### Metric parameters/hard constants for TCA calculation #########################################################

    ##### Hard constants
    MILLION = 1000000

    ##### Resample freq for input data in ms (or can be set to None, which is recommended)
    resample_ms = None  # 200

    ##### 1.0 implies that if the price goes higher, and it's a buy trade, we have a positive market impact/markout number
    market_impact_multiplier = 1.0

    transient_market_impact_gap = {'s': 60}
    permanent_market_impact_gap = {'h': 1}

    ##### Defining FX conventions ######################################################################################

    g10 = ['EUR', 'GBP', 'AUD', 'NZD', 'USD', 'CAD', 'CHF', 'NOK', 'SEK', 'JPY']
    em = ['TRY', 'ZAR']
    metals = ['XAU', 'XPT', 'XAG']
    cryptocurrencies = ['XBT']

    quotation_order = ['XAU', 'XPT', 'XAG', 'EUR', 'GBP', 'AUD', 'NZD', 'USD', 'CAD', 'CHF', 'TRY', 'NOK', 'SEK',
                       'ZAR', 'JPY']

    reporting_currency = 'USD'

    ##### Logging ######################################################################################################

    logging_file = os.path.join(log_folder, plat + "_tcapy.log")

    logging_parameters = {
        'version': 1,
        'root': {
            'handlers': ['console', 'rotate_file'],
            'level': 'DEBUG',
        },
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s; %(levelname)s:%(name)s: %(message)s '
                          '(%(filename)s:%(lineno)d)',
                # 'datefmt': "%Y-%m-%d %H:%M:%S ",
            }
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
            },
            'rotate_file': {
                'level': 'DEBUG',
                'formatter': 'standard',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': logging_file,
                'encoding': 'utf8',
                'maxBytes': 100000000,
                'backupCount': 1,
            }
        },
        # 'loggers': {
        #     '': {
        #         'handlers': ['console', 'rotate_file'],
        #         'level': 'DEBUG',
        #     },
        # }
    }

    ##### Assets & venues etc ##########################################################################################

    test_available_tickers_dictionary = {
        'All' : ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDJPY', 'AUDJPY', 'NZDCAD', 'AUDSEK', 'EURJPY',
                 'EURSEK', 'SEKJPY', 'GBPSEK', 'USDSEK'],
        'G10' : ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDJPY', 'AUDJPY', 'NZDCAD', 'AUDSEK', 'EURJPY',
                 'EURSEK', 'SEKJPY', 'GBPSEK', 'USDSEK'],
        'EM'  :  ['USDZAR', 'USDTRY']
    }

    test_venues_dictionary = \
        {
            'All' : ['venue1', 'venue2', 'venue3', 'venue4'],
            'Venue Group' : ['venue1', 'venue2'],
            'Venue Group 2' : ['venue3', 'venue4']
        }

    test_portfolios_dictionary = {
        'All' : ['portfolio1', 'portfolio2', 'portfolio3', 'portfolio4', 'portfolio5']
    }

    test_brokers_dictionary = {
        'All' : ['broker1', 'broker2', 'broker3', 'broker4', 'broker5', 'broker6'],
        'Broker Group' : ['broker1', 'broker2', 'broker3'],
        'Broker Group 2' : ['broker4', 'broker5', 'broker6']
    }

    test_sub_brokers_dictionary = {
        'All' : ['subbroker1', 'subbroker2', 'subbroker3', 'subbroker4', 'subbroker5', 'subbroker6']
    }

    test_portfolio_managers_dictionary = {
        'All' : ['pm1', 'pm2', 'pm3', 'pm4', 'pm5', 'pm6']
    }

    test_traders_dictionary = {
        'All' : ['trader1', 'trader2', 'trader3', 'trader4', 'trader5', 'trader6']
    }

    test_accounts_dictionary = {
        'All' : ['account1', 'account2', 'account3', 'account4', 'account5', 'account6']
    }

    test_algos_dictionary = {
        'All' : ['algo1', 'algo2', 'algo3', 'algo4', 'algo5', 'algo6', 'algo7'],
        'Algo Group' : ['algo1', 'algo2', 'algo3'],
        'Algo Group 2' : ['algo4', 'algo5', 'algo6']
    }

    # Recommend overriding these for production in your cred file, these dictionaries need to be set this way for tests
    # to run, with the randomised test data

    # Customise the types of assets traders, venues etc. (for testing purposes can just use the dummy variables above)
    available_market_data = ['arctic-dukascopy', 'arctic-ncfx', 'kdb-ncfx', 'dukascopy', 'ncfx']  # market data sources
    available_tickers_dictionary = test_available_tickers_dictionary  # which _tickers do we trade, and want to do TCA in
    available_venues_dictionary = test_venues_dictionary  # trading venues
    available_portfolios_dictionary = test_portfolios_dictionary  # portfolio IDs
    available_portfolio_managers_dictionary = test_portfolio_managers_dictionary  # portfolio manager IDs
    available_traders_dictionary = test_traders_dictionary  # execution trader IDs
    available_brokers_dictionary = test_brokers_dictionary  # broker IDs
    available_algos_dictionary = test_algos_dictionary  # trading algos

    available_metrics = ['slippage', 'transient market impact', 'permanent market impact']
    available_event_types = ['trade', 'cancel', 'placement', 'cancel/replace']

    chart_max_time_points = 10000

    ##### PROXY ACCESS (currently supported by NCFX external downloaders ###############################################

    web_proxies = {"http" : None,
                   "https" : None}

    # Below is a proxy example with username, password, server IP and server_port
    # web_proxies = {
    #     "http": "http://user:pass@1.1.1.10:1111/",
    #     "https" : "https://user:pass@1.1.1.10:1111"
    # }

    # Only attempt to start Flask directly in DEV environment, in PROD use eg. nginx/gunicorn!
    debug_start_flask_server_directly = False

    valid_data_stores = ['csv',
                         'mysql',
                         'ms_sql_server',
                         'sqlite',
                         'arctic-dukascopy', 'arctic-ncfx', 'arctic-testharness',
                         'pystore-dukascopy', 'pystore-ncfx', 'pystore-testharness',
                         'kdb-testharness',
                         'questdb-dukasacopy', 'questdb-ncfx', 'questdb-testharness'
                         'influxdb-dukasacopy', 'influxdb-ncfx', 'influxdb-testharness']

    ##### database configuration #######################################################################################

    # name of the table with market tick data
    # trade_data_database_table = 'trade_data_table'          # Name of the table with actual executions by client

    default_data_store = 'csv'  # 'sql', 'csv'

    # Should usually specify our own sources (eg. CSV filenames)
    default_market_data_store = 'arctic-dukascopy'  # The default database store for market data
    default_trade_data_store = 'mysql'              # The default database store for trade data

    ### CSV defaults (we can override these)
    # market_data_database_csv = os.path.join(test_data_folder, "test_market_data.csv")
    # trade_data_database_csv = os.path.join(test_data_folder, "test_trade_data.csv")

    # Trades first then orders
    trade_order_list = ['trade_df', 'order_df']
    test_trade_order_list = ['trade_df', 'order_df']

    pretty_trade_order_mapping = {'trade_df' : 'trades', 'order_df' : 'orders'}

    order_name = 'ancestor'

    # Downsample data internally to save memory/speed up TCA computation
    downsample_floats = False

    # Possible date columns
    date_columns = ['benchmark_date_start', 'benchmark_date_end']

    # Possible numeric fields
    numeric_columns = ['price_limit']

    # Possible string columns
    string_columns = ['broker_id', 'venue', 'algo_id']

    avoid_removing_duplicate_columns = ['volume']

    # Might be necessary if trying to parallelize computations
    re_sort_market_data_when_assembling = False

    # Line chunks to read csv when parsing into database
    csv_read_chunksize = 10 ** 6  # 10 ** 2 # very small chunk size

    ### SQL

    ## Generic SQL

    sql_dump_record_chunksize = 100000

    ## SQL Server specific
    ms_sql_server_host = docker_var('$MS_SQL_SERVER_HOST', 'localhost', default_value='sqlserver')
    ms_sql_server_port = docker_var('$MS_SQL_SERVER_HOST', '1433')

    ms_sql_server_odbc_driver = "ODBC+Driver+17+for+SQL+Server"

    ms_sql_server_username = docker_var('$MS_SQL_SERVER_USER', 'tcapyuser')
    ms_sql_server_password = docker_var('$MS_SQL_SERVER_PASSWORD', 'tcapyuser')

    ms_sql_server_python_package = 'pyodbc'  # 'pyodbc' ('pymssql' no longer supported)

    # For kerberos in Linux (for SQL server)
    ms_sql_server_kinit_path = '/usr/bin/kinit'
    ms_sql_server_realm = 'realm.com'
    ms_sql_server_use_custom_kerberos_script = True
    ms_sql_server_use_krb5_conf_default_realm = False
    ms_sql_server_kinit_custom_script_path = '/usr/bin/kinit_custom.sh'  # User will need to write this

    ms_sql_server_use_trusted_connection = False

    ms_sql_server_trade_data_database_name = 'trade_database'

    ms_sql_server_trade_order_mapping = \
        {'trade_df' : '[dbo].[trade]',  # Name of table which has broker messages to client
         'order_df' : '[dbo].[order]'}  # Name of table which has orders from client

    ## Postgres specific
    postgres_host = docker_var('$POSTGRES_HOST', 'localhost')
    postgres_port = docker_var('$POSTGRES_PORT', '1033')

    postgres_trade_data_database_name = 'trade_database'

    postgres_username = 'TODO'
    postgres_password = 'TODO'

    ## MySQL
    mysql_host = docker_var('$MYSQL_HOST', '127.0.0.1', default_value='mysql')
    mysql_port = docker_var('$MYSQL_PORT', '3306')

    mysql_trade_data_database_name = docker_var('$MYSQL_DATABASE', 'trade_database')

    # OVERWRITE_IN_ConstantsCred or set env var
    mysql_username = docker_var("$MYSQL_USER", 'tcapyuser')
    mysql_password = docker_var("$MYSQL_PASSWORD", 'tcapyuser')

    mysql_dump_record_chunksize = 10000    # Making the chunk size very big for MySQL can slow down inserts significantly

    mysql_trade_order_mapping = \
        {'trade_df' : 'trade_database.trade',     # Name of the table which holds broker messages to clients
         'order_df' : 'trade_database.order'}     # Name of the table which has orders from client

    ## sqlite
    sqlite_trade_data_database_name = '/data/sqlite/trade_database.db'

    sqlite_trade_order_mapping = \
        {'trade_df' : 'trade_table',    # Name of the table which holds broker messages to clients
         'order_df' : 'order_table'}    # Name of the table which has orders from client

    ### PyStore settings
    pystore_path = '/data/pystore'

    pystore_data_store = 'tcapy_store'
    pystore_market_data_database_table = 'market_data_table'

    ### Arctic/MongoDB
    arctic_host = docker_var("$MONGO_HOST", '127.0.0.1', default_value='mongo')
    arctic_port = docker_var("$MONGO_PORT", 27017, default_value=27017)

    # OVERWRITE_IN_ConstantsCred or set env var
    arctic_username = docker_var("$MONGO_INITDB_ROOT_USERNAME", 'tcapyuser')
    arctic_password = docker_var("$MONGO_INITDB_ROOT_PASSWORD", 'tcapyuser')

    # Set this if you want to use an external MongoDB instance, eg. MongoDB Atlas
    arctic_connection_string = docker_var("$MONGO_CONNECTION_STRING", None)

    arctic_ssl = False
    arctic_tlsAllowInvalidCertificates = True

    # NOTE: database name not currently used in Arctic
    arctic_market_data_database_name = 'fx'
    arctic_trade_data_database_name = 'fx'
    arctic_market_data_database_table = 'market_data_table'  # Name of the table with market tick data

    arctic_timeout_ms = 10 * 1000  # How many millisections should we have a timeout for in Arctic/MongoDB

    # https://arctic.readthedocs.io/en/latest/ has more discussion on the differences between the
    # various storage engines, and the various pros and cons
    # By default we use 'CHUNK_STORE' with 'D' (daily) bucketing - corresponding to our 'daily' fetching when using
    # with Celery
    arctic_lib_type = 'CHUNK_STORE'  # storage engines: VERSION_STORE or TICK_STORE or CHUNK_STORE (default)

    # 'D' is for daily, you can also choose 'M' and other chunk sizes (depends on how you wish to cache data)
    arctic_chunk_store_freq = 'D'

    arctic_quota_market_data_GB = 80

    ### KDB
    kdb_tickers = {'EURUSD': 'EURUSD'}

    kdb_host = 'localhost'
    kdb_port = 5000
    kdb_username = None
    kdb_password = None
    kdb_market_data_database_table = 'market_data_table'

    ### InfluxDB
    influxdb_host = docker_var('influxdb', 'localhost')
    influxdb_port = 8086
    influxdb_username = None
    influxdb_password = None
    influxdb_protocol = 'line'      # 'json' or 'line'
    influxdb_chunksize = 500000     # Number of records to write at once
    influxdb_time_precision = 'n'   # 'n' for nanosecond

    influxdb_market_data_database_table = 'market_data_table'

    ### QuestDB
    questdb_host = docker_var('questdb', 'localhost')
    questdb_port = 9009
    questdb_username = None
    questdb_password = None
    # questdb_protocol = 'line'      # 'json' or 'line'
    # questdb_chunksize = 500000     # Number of records to write at once
    # questdb_time_precision = 'n'   # 'n' for nanosecond

    questdb_market_data_database_table = 'market_data_table'

    ### ClickHouse
    clickhouse_host = docker_var("$CLICKHOUSE_HOST", '127.0.0.1', default_value='clickhouse')
    clickhouse_port = docker_var("$CLICKHOUSE_PORT", 9000, default_value=9000)

    # OVERWRITE_IN_ConstantsCred or set env var
    clickhouse_username = docker_var("$CLICKHOUSE_USERNAME", 'tcapyuser')
    clickhouse_password = docker_var("$CLICKHOUSE_PASSWORD", 'tcapyuser')

    clickhouse_market_data_database_name = 'fx'
    clickhouse_market_data_database_table = 'market_data_table'  # Name of the table with market tick data

    ########################################################################################################################

    GB = 1073741824  # bytes (do not change)

    #### defining parallelisation ##########################################################################################

    # choices for multiprocessing library are 'celery'
    # NOTE: standard Python multiprocessing has problems with pickling complicated functions, so it is not supported

    # for LINUX recommend 'celery' or 'multiprocess'

    # NOTE: that only celery is supported in a production environment ('multiprocess' and all similar libraries
    # don't work properly when Python is deployed with web server like Apache)

    # (multiprocessing has issues with pickling, multiprocess cannot be reused)

    # for databasesource usage (eg. when making calls to NCFX or an external data provider, where we are likely to have
    # a lot of IO blocking)
    database_source_threading_library = 'thread'

    # number of threads to use (note: won't impact celery, where we specify the number of workers when kicking off script)
    thread_no = 3

    # Celery keeps workers open anyway

    # On Windows recommend using 'thread', whilst on Linux preferred to use 'multiprocess'
    database_populator_threading_library = 'multiprocess'

    # Allow use of use_multithreading in general (if set to False, will avoid eg. using Celery)
    use_multithreading = True

    parallel_library = 'multiprocess' # For the default swim

    # This will split the TCA request by date, and work on those dates independently
    # may end up (very rarely) having slightly different output if trades are right at the start/end of a monthly boundary
    # such as orders which straddle the end of the month
    # currently experimental given that it doesn't pass tests for these edge cases (it is disabled for any _calculations
    # involving anything other than point in time executions (ie. trade_df)
    multithreading_params = {# True or False
                             # Note: if orders go over 'day/week'/'month' boundaries will have inaccurate answers with "True"
                             # So recommend to leave this False
                             # around boundaries, often the cache period will be more important thing to change
                             'splice_request_by_dates': False,

                             'cache_period': 'day',  # 'month' or 'week' or 'day'

                             # Cache trade data in monthly/periodic chunks in Redis (reduces database calls a lot)
                             'cache_period_trade_data': True,

                             # Cache market data in monthly/periodic chunks in Redis (reduces database calls a lot)
                             'cache_period_market_data': True,

                             # Return trade data internally as handles (usually necessary for Celery)
                             'return_cache_handles_trade_data' : True,

                             # Return market data internally as handles (usually necessary for Celery)
                             'return_cache_handles_market_data' : True,

                             # Recommend using Celery, which allows us to reuse Python processes
                             # 'single' should only be used for debugging purposes
                             'parallel_library': 'celery'
                             }

    ##### Volatile cache settings ##########################################################################################

    # Note that if you change any of these settings, make sure to empty the in-memory cache
    volatile_cache_engine = 'redis' # 'redis' (TODO 'plasma')

    # Redis settings (for internal usage - not Celery message broker - those settings need to be specified seperately)
    volatile_cache_host_redis = docker_var('$REDIS_HOST', 'localhost', default_value='redis')
    volatile_cache_port_redis = docker_var('$REDIS_PORT', '6379', default_value='6379')
    volatile_cache_timeout_redis = 3000
    volatile_cache_redis_internal_database = 1  # For storing tcapy data (as opposed to for message broker reasons with Celery)
    volatile_cache_redis_password = None  # Can set optional password (Redis doesn't have username)
    volatile_cache_redis_ssl = False  # uUse SSL to connect to Redis cache
    volatile_cache_redis_ssl_ca_certs = 'some_file.pem'  # key for SSL

    # Expiry time for user data (60 minutes)
    volatile_cache_expiry_seconds = 60 * 60

    # Note: msgpack is slightly faster, but is not supported in Pandas in later versions
    # at current stage arrow is not fully tested
    volatile_cache_redis_format = 'arrow' # 'msgpack' or 'arrow'

    volatile_cache_redis_compression = {'msgpack' : 'blosc',
                                        'arrow' : 'snappy'} # 'lz4' or 'snappy'

    # Above this size we need to break apart our keys into different chunks before pushing into Redis
    # Redis has a maximum size of what we can store in a single value (512 is maximum, can tweak lower)
    volatile_cache_redis_max_cache_chunk_size_mb = 500

    ##### celery settings ##################################################################################################

    # Make sure the broker/result backends are properly setup, ie. need Redis and memcached to be installed

    # Recommended to use Redis as the broker and to use memcached as results backend
    # Using Redis as both can result in race conditions, because tcapy extensively uses Redis to cache datasets
    # alternatively: amqp/RabbitMQ as the result_backend, but this is deprecated on Celery and is more difficult to configure

    celery_broker_url = docker_var('$CELERY_BROKER_URL', 'redis://localhost:6379/0',
                                   default_value='redis://redis:6379/0')
    celery_result_backend = docker_var("$CELERY_RESULT_BACKEND", "cache+memcached://localhost:11211/",
                                       default_value='cache+memcached://memcached:11211/')

    # You might need to adjust this depending on the size of tasks you end up running
    celery_timeout_seconds = 600

    # When using celery we usually need to send around "return_cache_handles" to the market data, which are keys to the dataframe
    # as stored in Redis in a highly compressed fashion (as opposed to pickling these very large dataframes back and
    # forth, which can cause memory issues)
    #
    # When returning trade or market data cache in Redis first, before returning as a handle
    # use_cache_handles = True

    use_compression_with_handles = True
    use_compression_with_period_caches = True

    ##### Webserver parameters #############################################################################################

    secret_key = 'gSYX3Tsvr4rVbnrztkJ1MluTMia-dmM6'  # for encoding user session IDs (create your own randomly generated version)

    url_prefix = 'tcapy'  # eg. if hosted on "http://localhost/tcapy"
    routes_pathname_prefix = '/tcapy/'  # note: can sometimes be empty or simply '/'
    requests_pathname_prefix = '/tcapy/'  # note: can sometimes be empty or simply '/'

    ##### OVERRIDES ########################################################################################################

    ### We can override default parameters by creating a separate constants file called constantscred.py in the tcapy.conf package, below
    ### we have a sample ConstantsCred file. This has different settings for the candlestick style we can use
    ### in the plot. We can override as many variables as we want. We would recommend creating a ConstantsCred file in same folder
    ### to store servers/usernames etc. and hence, do NOT put it in version control (you can also use something like
    ### the keyring Python library to manage passwords in constantscred.py
    ###
    ### class ConstantsCred(object):
    ###     chart_type_candlesticks = 'ohlc'
    ###

    # Overwrite field variables with those listed in ConstantsOverride
    def __init__(self):

        # Which backend have you used 'user' or 'test_tcapy'
        if Constants.tcapy_version == 'user':
            # override fields related to assets etc. which are proprietary to each client
            try:
                from tcapygen.constantsgen import ConstantsGen
                cred_keys = ConstantsGen.__dict__.keys()

                for k in ConstantsGen.__dict__.keys():
                    if k in cred_keys and '__' not in k:
                        setattr(Constants, k, getattr(ConstantsGen, k))
            except:
                pass

            # Override fields related to assets etc. which are proprietary to each client
            try:
                from tcapyuser.constantsuser import ConstantsUser
                cred_keys = ConstantsUser.__dict__.keys()

                for k in ConstantsUser.__dict__.keys():
                    if k in cred_keys and '__' not in k:
                        setattr(Constants, k, getattr(ConstantsUser, k))
            except:
                pass

        elif Constants.tcapy_version == 'test_tcapy':
            # Override fields related to assets etc. which are proprietary to each client
            try:
                from tcapygen.constantsgen import ConstantsGen
                cred_keys = ConstantsGen.__dict__.keys()

                for k in ConstantsGen.__dict__.keys():
                    if k in cred_keys and '__' not in k:
                        setattr(Constants, k, getattr(ConstantsGen, k))
            except:
                pass

        elif Constants.tcapy_version == 'gen':
            # Override fields related to assets etc. which are proprietary to each client
            try:
                from tcapygen.constantsgen import ConstantsGen
                cred_keys = ConstantsGen.__dict__.keys()

                for k in ConstantsGen.__dict__.keys():
                    if k in cred_keys and '__' not in k:
                        setattr(Constants, k, getattr(ConstantsGen, k))
            except:
                pass

        # Override password/server details which are stored in the ConstantsCred file (for open version)
        try:
            from tcapy.conf.constantscred import ConstantsCred
            cred_keys = ConstantsCred.__dict__.keys()

            for k in ConstantsCred.__dict__.keys():
                if k in cred_keys and '__' not in k:
                    setattr(Constants, k, getattr(ConstantsCred, k))
        except Exception as e:
            pass
            # print(str(e))

        # Final override password/server details which are stored in the ConstantsCredUser file (for pro version)
        try:
            from tcapyuser.constantscreduser import ConstantsCredUser
            cred_keys = ConstantsCredUser.__dict__.keys()

            for k in ConstantsCredUser.__dict__.keys():
                if k in cred_keys and '__' not in k:
                    setattr(Constants, k, getattr(ConstantsCredUser, k))
        except Exception as e:
            pass
            # print(str(e))

        ### set these variables *after* overwriting (may need changing)
        try:
            ### trade order mapping
            self.trade_order_mapping = {'mysql': self.mysql_trade_order_mapping,
                                        'ms_sql_server': self.ms_sql_server_trade_order_mapping,
                                        'sqlite' : self.sqlite_trade_order_mapping
                                        }

            ### these are the FX crosses which are available in each set of market data
            self.market_data_tickers = {'arctic-ncfx': self.ncfx_tickers,
                                        'arctic-dukascopy': self.dukascopy_tickers,
                                        'arctic-eikon': self.eikon_tickers,
                                        'arctic-testharness': self.test_harness_tickers,
                                        'pystore-ncfx': self.ncfx_tickers,
                                        'pystore-dukascopy': self.dukascopy_tickers,
                                        'pystore-testharness': self.test_harness_tickers,
                                        'influxdb-ncfx' : self.ncfx_tickers,
                                        'influxdb-dukascopy': self.dukascopy_tickers,
                                        'influxdb-testharness' : self.test_harness_tickers,
                                        'questdb-ncfx': self.ncfx_tickers,
                                        'questdb-dukascopy': self.dukascopy_tickers,
                                        'questdb-testharness': self.test_harness_tickers,
                                        'kdb-ncfx': self.ncfx_tickers,
                                        'kdb-testharness': self.test_harness_tickers,
                                        'ncfx': self.ncfx_tickers,
                                        'dukascopy' : self.dukascopy_tickers,
                                        'eikon' : self.eikon_tickers
                                        }
        except:
            pass
