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

from tests.config import resource

constants = Constants()
logger = LoggerManager().getLogger(__name__)


tcapy_version = constants.tcapy_version

logger.info('Make sure you have created folder ' + constants.csv_folder + ' & ' + constants.temp_data_folder +
            ' otherwise tests will fail')

########################################################################################################################
# YOU MAY NEED TO CHANGE TESTING PARAMETERS IF YOUR DATABASE DOESN'T COVER THESE DATES
start_date = '20 Jan 2018'
finish_date = '25 Jan 2018'

# Current data vendors are 'ncfx' or 'dukascopy'
data_source = 'ncfx'
trade_data_store = 'ms_sql_server'
market_data_store = 'arctic-' + data_source

ticker = 'EURUSD'
reporting_currency = 'USD'
tca_type = 'aggregated'
venue_filter = 'venue1'

# Mainly just to speed up tests - note: you will need to generate the HDF5 files using convert_csv_to_h5.py from the CSVs
use_hdf5_market_files = False

# So we are not specifically testing the database of tcapy - can instead use CSV in the test harness folder
use_trade_test_csv = False
use_market_test_csv = False

########################################################################################################################

# you can change the test_data_harness_folder to one on your own machine with real data
folder = constants.test_data_harness_folder

eps = 10 ** -5

trade_order_mapping = constants.test_trade_order_list
trade_df_name = trade_order_mapping[0] # usually 'trade_df'
order_df_name = trade_order_mapping[1] # usually 'order_df'

if use_market_test_csv:

    # Only contains limited amount of EURUSD and USDJPY in Apr/Jun 2017
    # todo: Where are those files!
    if use_hdf5_market_files:
        market_data_store = os.path.join(folder, 'small_test_market_df.h5')
    else:
        market_data_store = os.path.join(folder, 'small_test_market_df.csv.gz')

if use_trade_test_csv:
    trade_data_store = 'csv'
    # todo: better with resource function
    trade_order_mapping = OrderedDict([(trade_df_name, resource('small_test_trade_df.csv')),
                                       (order_df_name, resource('small_test_order_df.csv'))])
    venue_filter = 'venue1'
else:
    # Define your own trade order mapping
    pass

def test_overlapping_full_detailed_tca_calculation():
    """Tests a detailed TCA calculation works with caching and overlapping dates, checking that it has the right tables returned.
    """

    #logger = LoggerManager.getLogger(__name__)

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             tca_type='detailed',
                             trade_data_store=trade_data_store,
                             market_data_store=market_data_store,
                             trade_order_mapping=trade_order_mapping, use_multithreading=True)

    tca_engine = TCAEngineImpl(version=tcapy_version)

    #dict_of_df = tca_engine.calculate_tca(tca_request=tca_request)

    #sparse_market_trade_df = dict_of_df['sparse_market_' + trade_df_name]

    #logger.info("Running second TCA calculation, extending dates...")

    # Extend sample
    tca_request.start_date = pd.Timestamp(start_date) - timedelta(days=10)

    dict_of_df = tca_engine.calculate_tca(tca_request=tca_request)

    sparse_market_trade_df = dict_of_df['sparse_market_' + trade_df_name]

    assert len(sparse_market_trade_df.index[sparse_market_trade_df.index < '01 Feb 2018']) > 0
