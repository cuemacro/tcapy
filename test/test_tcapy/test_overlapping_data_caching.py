"""Tests out the caching when we have requests with overlapping dates
"""

from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#
import pandas as pd
import os

from tcapy.analysis.tcaengine import TCAEngineImpl

from tcapy.analysis.tcarequest import TCARequest

from tcapy.analysis.algos.benchmark import *
from tcapy.analysis.algos.resultsform import *

from collections import OrderedDict

from test.config import *

constants = Constants()
logger = LoggerManager().getLogger(__name__)

logger.info('Make sure you have created folder ' + constants.csv_folder + ' & ' + constants.temp_data_folder +
            ' otherwise tests will fail')

########################################################################################################################
# YOU MAY NEED TO CHANGE TESTING PARAMETERS IF YOUR DATABASE DOESN'T COVER THESE DATES
start_date = '20 May 2017'
finish_date = '25 May 2017'

trade_data_store = 'mysql'
trade_data_database_name = 'trade_database_test_harness'
trade_order_mapping = {
    'ms_sql_server' :   {'trade_df' : '[dbo].[trade]',      # Name of table which has broker messages to client
                         'order_df' : '[dbo].[order]'},     # Name of table which has orders from client
    'mysql':            {'trade_df': 'trade_database_test_harness.trade',   # Name of table which has broker messages to client
                         'order_df': 'trade_database_test_harness.order'},  # Name of table which has orders from client
    'sqlite':           {'trade_df': 'trade_table',  # Name of table which has broker messages to client
                         'order_df': 'order_table'}  # Name of table which has orders from client
}

trade_order_mapping = trade_order_mapping[trade_data_store]

market_data_store = 'arctic-testharness'
market_data_database_table = 'market_data_table_test_harness'

ticker = 'EURUSD'
reporting_currency = 'USD'
tca_type = 'aggregated'
venue_filter = 'venue1'

# So we are not specifically testing the database of tcapy - can instead use CSV in the test harness folder
use_trade_test_csv = False
use_market_test_csv = False

use_multithreading = False

########################################################################################################################

eps = 10 ** -5

if use_market_test_csv:
    market_data_store = resource('small_test_market_df.parquet')

if use_trade_test_csv:
    trade_data_store = 'csv'

    trade_order_mapping = OrderedDict([('trade_df', resource('small_test_trade_df.csv')),
                                       ('order_df', resource('small_test_order_df.csv'))])
    venue_filter = 'venue1'
else:
    # Define your own trade order mapping
    pass

def test_overlapping_full_detailed_tca_calculation():
    """Tests a detailed TCA calculation works with caching and overlapping dates, checking that it has the right tables returned.
    """

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             tca_type='detailed',
                             trade_data_store=trade_data_store,
                             trade_data_database_name=trade_data_database_name,
                             market_data_store=market_data_store,
                             market_data_database_table=market_data_database_table,
                             trade_order_mapping=trade_order_mapping, use_multithreading=use_multithreading)

    tca_engine = TCAEngineImpl(version=tcapy_version)

    # Extend sample
    tca_request.start_date = pd.Timestamp(start_date) - timedelta(days=10)

    dict_of_df = tca_engine.calculate_tca(tca_request=tca_request)

    sparse_market_trade_df = dict_of_df['sparse_market_trade_df']

    assert len(sparse_market_trade_df.index[sparse_market_trade_df.index < '01 Jun 2017']) > 0
