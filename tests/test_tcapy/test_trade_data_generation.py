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
from tcapy.util.loggermanager import LoggerManager

logger = LoggerManager().getLogger(__name__)

constants = Constants()

postfix = 'dukascopy'
ticker = ['EURUSD']
start_date = '01 May 2017'
finish_date = '31 May 2017'

use_test_csv = True

# mainly just to speed up tests - note: you will need to generate the HDF5 files using convert_csv_to_h5.py from the CSVs
use_hdf5_market_files = False


logger.info('Make sure you have created folder ' + constants.csv_folder + ' & ' + constants.temp_data_folder +
            ' otherwise tests will fail')

########################################################################################################################

# you can change the test_data_harness_folder to one on your own machine with real data
folder = constants.test_data_harness_folder

eps = 10 ** -5

if use_test_csv:

    # only contains limited amount of EURUSD and USDJPY in Apr/Jun 2017
    if use_hdf5_market_files:
        market_data_store = os.path.join(folder, 'small_test_market_df.h5')
    else:
        market_data_store = os.path.join(folder, 'small_test_market_df.csv.gz')

def test_randomized_trade_data_generation():
    """Tests randomized trade generation data (and writing to database)
    """
    data_test_creator = DataTestCreator(write_to_db=False)

    # use database source as Arctic for market data (assume we are using market data as a source)
    if use_test_csv:
        data_test_creator._database_source_market = DatabaseSourceCSV(market_data_database_csv=market_data_store)
    else:
        data_test_creator._database_source_market = DatabaseSourceArctic(postfix=postfix)

    # create randomised trade/order data
    trade_order = data_test_creator.create_test_trade_order(ticker, start_date=start_date, finish_date=finish_date)

    # trade_order has dictionary of trade_df and order_df

    # make sure the number of trades > number of orders
    assert (len(trade_order['trade_df'].index) > len(trade_order['order_df'].index))

if __name__ == '__main__':
    test_randomized_trade_data_generation()

    # import pytest; pytest.main()