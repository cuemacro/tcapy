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
from tcapy.data.databasesource import DatabaseSourceArctic, DatabaseSourceKDB

constants = Constants()

if __name__ == '__main__':

    logger = LoggerManager.getLogger(__name__)

    PLOT_BACK_DATA = False
    data_vendor = 'dukascopy' # 'dukascopy' or 'ncfx'

    logger.info("About to upload data to arctic")

    ## YOU WILL NEED TO CHANGE THE BELOW LINES #########################################################################

    # parameters for testing
    if True:
        source = 'testharness'

        # Load up market data from CSV and dump into Arctic database
        ticker_mkt = ['AUDUSD']

        folder = None # What is the folder of the CSV of h5 file

        exists_table = 'replace'
        append_replace_ticker = 'replace'   # 'replace' will remove all market data for this ticker!
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

    # More parameters for testing
    if True:
        source = 'testharness'

        # Load up market data from CSV and dump into Arctic database
        ticker_mkt = ['EURUSD']

        folder = '/home/redhat/tcapy_tests_data/csv_dump/' # What is the folder of the CSV of h5 file

        exists_table = 'replace'
        append_replace_ticker = 'replace'   # 'replace' will remove all market data for this ticker!
                                            # 'append' will append it to the end (assumes no overlap in dataset!)

        file_extension = 'h5' # csv or h5

        # NOTE: we can use wildcard characters eg. AUDUSD*.csv - we assume that the filenames are however correctly arranged
        # in time (ie. AUDUSD1.csv is before AUDUSD2.csv etc)
        csv_file = [x + '_' + data_vendor + '_*.' + file_extension for x in ticker_mkt]  # assume that ALL TIME IN UTC!

        date_format = None
        # date_format='%d-%m-%Y %H:%M:%S.%f'  # you may need to change this

        # Should we read the file in reverse?
        read_in_reverse = False

        # Should we remove consecutive duplicates (safe for most TCA operations, NOT when volume is involved)
        remove_duplicates = True

    # dukascopy or ncfx style parameters
    if False:
        source = 'ncfx'

        ticker_mkt = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDJPY']

        folder = '/home/redhat/tcapy_tests_data/csv_dump/'

        exists_table = 'append'
        append_replace_ticker = 'replace'

        file_extension = 'h5'  # csv or h5

        csv_file = [x + '_' + data_vendor + '_*.' + file_extension for x in ticker_mkt]  # assume that ALL TIME IN UTC!

        date_format = None

        read_in_reverse = False

        remove_duplicates = True

        ####################################################################################################################

    # Load market data
    data_source_csv = DatabaseSourceCSV()

    market_data_store = 'arctic'

    if market_data_store == 'arctic':
        database_source = DatabaseSourceArctic(postfix=source)
        market_data_database_table = constants.arctic_market_data_database_table

    if folder is None:
        folder = Constants().test_data_folder

    # This relies on you have market data stored in H5/CSV files already (eg. by downloading from DukasCopy)
    # note: whilst free FX data can be used for testing (in particular for generating randomised trades),
    # we recommend using higher quality data for actual benchmark

    csv_market_data = [os.path.join(folder, x) for x in csv_file]

    # for each ticker, read in the H5/CSV file and then dump into tick database
    for i in range(0, len(ticker_mkt)):
        ticker = ticker_mkt[i]
        csv_file = csv_market_data[i]

        database_source.convert_csv_to_table(csv_file, ticker, market_data_database_table,
                                            if_exists_table=exists_table, remove_duplicates=remove_duplicates,
                                            if_exists_ticker=append_replace_ticker, date_format=date_format,
                                            read_in_reverse=read_in_reverse)

        # It is worth plotting the data to check validity sometimes (make sure you choose appropriate start/finish dates
        # loading a *very* large tick history into memory will result in your computer running out of memory
        if PLOT_BACK_DATA:
            from chartpy import Chart

            import datetime
            import pandas as pd

            df = DatabaseSourceArctic(postfix=source).fetch_market_data(
                start_date='01 Jan 2000', finish_date=datetime.datetime.utcnow(), ticker=ticker)

            df = pd.DataFrame(df.resample('5min').mean())

            if 'mid' not in df.columns:
                df['mid'] = (df['bid'] + df['ask']) / 2.0

            df = pd.DataFrame(df['mid'])

            Chart(engine='plotly').plot(df)

            print(df)

    logger.info("Finished uploading data to arctic")

