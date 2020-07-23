"""This shows how we can connect to an instance of MongoDB Atlas to read/write market tick data

Note, that you will need to get a MongoDB Atlas cloud account, and change the connection string below for it to work
"""

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import datetime
import time

from tcapy.util.loggermanager import LoggerManager
from tcapy.conf.constants import Constants

from tcapy.data.datafactory import MarketRequest

from tcapy.data.databasesource import DatabaseSourceArctic

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

start_date = '26 Apr 2017'
finish_date = '05 Jun 2017'
ticker = 'EURUSD'

# Market data parameters for tables/databases
test_harness_arctic_market_data_table = 'market_data_table_test_harness'
test_harness_arctic_market_data_store = 'arctic-testharness'

csv_market_data_store = resource('small_test_market_df.parquet')
csv_reverse_market_data_store = resource('small_test_market_df_reverse.parquet')

# Note, you'll need to get your own connection string!
# You can setup your own MongoDB instance on the cloud using MongoDB Atlas https://www.mongodb.com/cloud/atlas
# It will give you the connection string to use
arctic_connection_string = "mongodb+srv://<username>:<password>@cluster0.blah-blah.mongodb.net/?retryWrites=true&w=majority"

def write_mongo_db_atlas_arctic():
    """Tests we can write market data to Arctic/MongoDB on Atlas (cloud)
    """

    market_loader = Mediator.get_tca_market_trade_loader(version=tcapy_version)

    ### Test we can read data from CSV and dump to Arctic (and when read back it matches CSV)
    db_start_date = '01 Jan 2016';
    db_finish_date = pd.Timestamp(datetime.datetime.utcnow())

    database_source = DatabaseSourceArctic(postfix='testharness', arctic_lib_type='CHUNK_STORE', connection_string=arctic_connection_string)

    # Write CSV to Arctic
    database_source.convert_csv_to_table(csv_market_data_store, ticker,
                                         test_harness_arctic_market_data_table,
                                         if_exists_table='replace', if_exists_ticker='replace', market_trade_data='market',
                                         remove_duplicates=False)

    # Read back data from Arctic and compare with CSV
    market_request = MarketRequest(start_date=db_start_date, finish_date=db_finish_date, ticker=ticker,
                                   data_store=database_source,  # test_harness_arctic_market_data_store,
                                   market_data_database_table=test_harness_arctic_market_data_table)

    market_df_load = market_loader.get_market_data(market_request=market_request)

    print(market_df_load)

if __name__ == '__main__':
    start = time.time()

    write_mongo_db_atlas_arctic()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
