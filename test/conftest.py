"""Tests functions for loading market and trade/order data (CSVs, SQL, Arctic, InfluxDB and KDB). Check that your database has
market and trade data for these before running the test. test_data_write will write data into a test database first
and then read - so that can be used instead, if your production market and trade databases are not yet populated.

This will also involve setting your database IP, username, password etc.
"""

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import pytest

import os

from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.mediator import Mediator

from tcapy.data.databasesource import DatabaseSourceMySQL, DatabaseSourceArctic

from test.config import *

logger = LoggerManager().getLogger(__name__)

constants = Constants()

logger.info('Make sure you have created folder ' + constants.csv_folder + ' & ' + constants.temp_data_folder +
            ' otherwise tests will fail.')

if not(os.path.exists(constants.csv_folder)):
    try:
        os.mkdir(constants.csv_folder)
    except:
        logger.warn('Could not create ' + constants.csv_folder)

if not(os.path.exists(constants.temp_data_folder)):
    try:
        os.mkdir(constants.temp_data_folder)
    except:
        logger.warn('Could not create ' + constants.temp_data_folder)



########################################################################################################################

start_date = '26 Apr 2017'
finish_date = '05 Jun 2017'
ticker_arctic = ['EURUSD', 'USDJPY']

# Market data parameters for tables/databases
market_data_table = 'market_data_table_test_harness'
market_data_store = 'arctic-testharness'

# Default format is CHUNK_STORE, so should be last, so we can read in later
arctic_lib_type = 'CHUNK_STORE'

# Trade data parameters
trade_data_database_name = 'trade_database_test_harness'
trade_data_store = 'mysql'

########################################################################################################################

########################################################################################################################
#### FILLING MARKET AND TRADE DATABASES FOR LATER TESTS ################################################################
########################################################################################################################

@pytest.fixture(scope="module")
def fill_market_trade_databases():
    """Fills market and trade data with test data
    """
    Mediator.get_volatile_cache().clear_cache()

    replace_append = 'replace'

    # Fill market data (assume: CHUNK_STORE as our default format!)
    for ticker in ticker_arctic:
        database_source = DatabaseSourceArctic(postfix='testharness', arctic_lib_type=arctic_lib_type)

        # Write CSV to Arctic
        database_source.convert_csv_to_table(csv_market_data_store, ticker,
                                             market_data_table,
                                             if_exists_table=replace_append,
                                             if_exists_ticker='replace', market_trade_data='market',
                                             remove_duplicates=False)

        replace_append = 'append'

    # Fill trade/order data
    database_source = DatabaseSourceMySQL()

    for t in trade_order_list:
        # Dump trade_df to SQL test harness database and overwrite
        database_source.convert_csv_to_table(csv_trade_order_mapping[t], None,
                                            (trade_order_mapping[trade_data_store])[t],
                                            database_name=trade_data_database_name,
                                            if_exists_table='replace', market_trade_data='trade')