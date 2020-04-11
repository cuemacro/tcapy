"""Can be used to populate the market and trade/order databases. Populate market database with market data in H5/CSVs (Arctic).
Users can modify the H5/CSV paths, so they can dump their own trade/order data into the trade database.
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#


import os

from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager
from tcapy.data.databasesource import DatabaseSourceCSV
from tcapy.data.databasesource import DatabaseSourceArctic, DatabaseSourcePyStore, DatabaseSourceInfluxDB, DatabaseSourceKDB

constants = Constants()

if __name__ == '__main__':

    logger = LoggerManager.getLogger(__name__)

    PLOT_BACK_DATA = False
    data_vendor = 'dukascopy' # 'dukascopy' or 'ncfx'

    # Either use 'arctic' or 'pystore' to store market tick data
    market_data_store = 'influxdb'

    logger.info("About to upload data to " + market_data_store)

    ## YOU WILL NEED TO CHANGE THE BELOW LINES #########################################################################

    # Parameters for testing
    if True:
        data_vendor = 'testharness'

        # Load up market data from CSV and dump into Arctic database
        ticker_mkt = ['AUDUSD']

        csv_folder = None # What is the folder of the CSV of h5 file

        if_exists_table = 'replace'
        if_append_replace_ticker = 'replace'   # 'replace' will remove all market data for this ticker!
                                            # 'append' will append it to the end (assumes no overlap in dataset!)

        file_extension = 'csv' # csv or h5

        # NOTE: we can use wildcard characters eg. AUDUSD*.csv - we assume that the filenames are however correctly arranged
        # in time (ie. AUDUSD1.csv is before AUDUSD2.csv etc)
        csv_file = ['test_market_' + x + '.' + file_extension for x in ticker_mkt] # assume that ALL TIME IN UTC!

        date_format = None
        # date_format='%d-%m-%Y %H:%M:%S.%f'  # you may need to change this

        # Should we read the file in reverse?
        read_in_reverse = True

        # Should we remove consecutive duplicates (safe for most TCA operations, NOT when volume is involved)
        remove_duplicates = True

    # dukascopy or ncfx style parameters
    if True:
        data_vendor = 'dukascopy' # 'ncfx' or 'dukascopy'

        ticker_mkt = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF',
                      'EURNOK', 'EURSEK',
                      'USDJPY',
                      'USDNOK', 'USDSEK', 'EURJPY',
                      'USDMXN', 'USDTRY', 'USDZAR', 'EURPLN']

        csv_folder = '/home/tcapyuser/csv_dump/'

        if_exists_table = 'append' #
        if_append_replace_ticker = 'replace'

        file_extension = 'parquet'  # 'parquet' or 'csv' or 'h5' on disk

        # Files dumped by DatabasePopulator look like this
        ## 'AUDUSD_dukascopy_2016-01-03_22_00_01.868000+00_002016-01-31_23_59_57.193000+00_00.parquet'

        csv_file = [x + '_' + data_vendor + '_2016-01*.' + file_extension for x in
                    ticker_mkt]  # assume that ALL TIME IN UTC!

        date_format = None
        read_in_reverse = False
        remove_duplicates = False

    ####################################################################################################################

    # Load market data
    data_source_csv = DatabaseSourceCSV()

    # Create market data store for database and associated data vendor
    if market_data_store == 'arctic':
        database_source = DatabaseSourceArctic(postfix=data_vendor)
        market_data_database_table = constants.arctic_market_data_database_table

    if market_data_store == 'pystore':
        database_source = DatabaseSourcePyStore(postfix=data_vendor)
        market_data_database_table = constants.pystore_market_data_database_table

    if market_data_store == 'influxdb':
        database_source = DatabaseSourceInfluxDB(postfix=data_vendor)
        market_data_database_table = constants.influxdb_market_data_database_table

    if market_data_store == 'kdb':
        database_source = DatabaseSourceKDB(postfix=data_vendor)
        market_data_database_table = constants.kdb_market_data_database_table

    if csv_folder is None:
        csv_folder = Constants().test_data_folder

    # This relies on you have market data stored in Parquet/H5/CSV files already (eg. by downloading from DukasCopy)
    # note: whilst free FX data can be used for testing (in particular for generating randomised trades),
    # you may to choose other high frequency quality data for actual benchmark

    csv_market_data = [os.path.join(csv_folder, x) for x in csv_file]

    # for each ticker, read in the H5/CSV file and then dump into tick database
    for i in range(0, len(ticker_mkt)):
        ticker = ticker_mkt[i]
        csv_file = csv_market_data[i]

        # On the second time through the loop, we make sure to append to table
        # otherwise will keep overwriting!
        if if_exists_table == 'replace':
            if i >= 1:
                if_exists_table = 'append'
            else:
                if_exists_table = 'replace'

        database_source.convert_csv_to_table(csv_file, ticker, market_data_database_table,
                                             if_exists_table=if_exists_table, remove_duplicates=remove_duplicates,
                                             if_exists_ticker=if_append_replace_ticker, date_format=date_format,
                                             read_in_reverse=read_in_reverse)

        # It is worth plotting the data to check validity sometimes (make sure you choose appropriate start/finish dates
        # loading a *very* large tick history into memory will result in your computer running out of memory
        if PLOT_BACK_DATA:
            from chartpy import Chart

            import datetime
            import pandas as pd

            df = database_source.fetch_market_data(
                start_date='01 Jan 2000', finish_date=datetime.datetime.utcnow(), ticker=ticker)

            df = pd.DataFrame(df.resample('5min').mean())

            if 'mid' not in df.columns:
                df['mid'] = (df['bid'] + df['ask']) / 2.0

            df = pd.DataFrame(df['mid'])

            Chart(engine='plotly').plot(df)

            print(df)

    logger.info("Finished uploading data to " + market_data_store)

