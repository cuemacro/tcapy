"""Tests out the code for generating randomised test trades/orders.
"""

from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import os

from tcapy.conf.constants import Constants

from tcapy.data.datatestcreator import DataTestCreator
from tcapy.data.databasesource import DatabaseSourceCSVBinary as DatabaseSourceCSV
from tcapy.data.databasesource import DatabaseSourceArctic
from tcapy.util.mediator import Mediator
from tcapy.util.loggermanager import LoggerManager

logger = LoggerManager().getLogger(__name__)

constants = Constants()

postfix = 'testharness'
ticker = ['EURUSD']
start_date = '01 May 2017'
finish_date = '31 May 2017'

use_market_data_test_csv = True

from test.config import *

logger.info('Make sure you have created folder ' + constants.csv_folder + ' & ' + constants.temp_data_folder +
            ' otherwise tests will fail')

Mediator.get_volatile_cache().clear_cache()

########################################################################################################################

# You can change the test_data_harness_folder to one on your own machine with real data
folder = constants.test_data_harness_folder

eps = 10 ** -5

if use_market_data_test_csv:
    # Only contains limited amount of EURUSD and USDJPY in Apr/Jun 2017
    market_data_store = resource('small_test_market_df.parquet')

def test_randomized_trade_data_generation(fill_market_trade_databases):
    """Tests randomized trade generation data (and writing to database)
    """
    data_test_creator = DataTestCreator(write_to_db=False)

    # Use database source as Arctic for market data (assume we are using market data as a source)
    if use_market_data_test_csv:
        data_test_creator._database_source_market = DatabaseSourceCSV(market_data_database_csv=market_data_store)
    else:
        data_test_creator._database_source_market = DatabaseSourceArctic(postfix=postfix)

    # Create randomised trade/order data
    trade_order = data_test_creator.create_test_trade_order(ticker, start_date=start_date, finish_date=finish_date)

    # Trade_order has dictionary of trade_df and order_df

    # Make sure the number of trades > number of orders
    assert (len(trade_order['trade_df'].index) > len(trade_order['order_df'].index))
