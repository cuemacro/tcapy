"""Downloads new market data from data vendor for writing to CSV
"""

from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

if __name__ == '__main__':
    import time

    start = time.time()

    from tcapy.conf.constants import Constants

    data_vendor = 'dukascopy' # 'ncfx' or 'dukascopy'
    write_large_csv = False
    write_large_hdf5_parquet = True
    return_df = False # returns the dataframe (DO NOT DO this for large datasets, as it will cause you to run out of memory)
    remove_duplicates = False   # Removes duplicate data points (the vast proportion of datapoints will be duplicates
                                # this will make the CSV files much bigger, which is ok for archival purposes
                                # however, when finally copying to the Arctic database, we recommend removing duplicates
                                # otherwise, it quickly results in going out of memory

    csv_folder = '/data/csv_dump/' + data_vendor + '/'
    constants = Constants()

    # Where should we dump the temporary FX data mini files and large H5/Parquet files
    # sometimes we might want to specify just a small section to download and specific _tickers

    # Usual default parameters
    start_date = None; finish_date = None

    # You may need to change these folders
    temp_data_folder = constants.temp_data_folder; temp_large_data_folder = constants.temp_large_data_folder
    temp_data_folder = '/data/csv_dump/temp/'
    temp_large_data_folder = '/data/csv_dump/temp/large/'

    start_date_csv = '01 Apr 2016'; finish_date_csv = '01 May 2020'; split_size = 'monthly' # 'daily' or 'monthly'
    # start_date_csv = '01 Jun 2020'; finish_date_csv = '01 Sep 2020';

    if data_vendor == 'ncfx':
        from tcapy.data.databasepopulator import DatabasePopulatorNCFX as DatabasePopulator

        tickers = constants.ncfx_tickers
    elif data_vendor == 'dukascopy':
        from tcapy.data.databasepopulator import DatabasePopulatorDukascopy as DatabasePopulator

        tickers = constants.dukascopy_tickers

    # Example of manually specifying _tickers
    # tickers = {'EURUSD' : 'EURUSD', 'GBPUSD': 'GBPUSD', 'USDCAD': 'USDCAD', 'NZDUSD': 'NZDUSD', 'USDCHF' : 'USDCHF',
    #            'USDJPY' : 'USDJPY'}

    db_populator = DatabasePopulator(temp_data_folder=temp_data_folder, temp_large_data_folder=temp_large_data_folder,
                                              tickers=tickers)

    # Writes a CSV/Parquet to disk from data vendor (does not attempt to write anything to the database)
    # Will also dump temporary HDF5 files to disk (to avoid reloading them)
    msg, df_dict = db_populator.download_to_csv(start_date_csv, finish_date_csv, tickers, split_size=split_size,
        csv_folder=csv_folder, return_df=False, remove_duplicates=False, write_large_csv=write_large_csv,
        write_large_hdf5_parquet=write_large_hdf5_parquet)

    print(msg)
    print(df_dict)

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
