"""Calls data vendor via API and then dumps to internal database (Arctic/MongoDB) for fetching later, which is much faster than
storing the data as CSV files. Can also optionally dump the dataset to CSV. Repeatedly calling the external API is time
consuming and should be avoided. Will also return a string for any missing data during market hours (which are assumed
to be between Sunday evening till Friday evening).
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

    start = time.clock()

    from tcapy.conf.constants import Constants

    remove_duplicate_follow_on = False

    data_vendor = 'dukascopy' # 'dukascopy' or 'ncfx'
    write_csv = True
    csv_folder = '/home/tcapyuser/csv_dump' # Make sure this folder exists!
    constants = Constants()

    # How the data should be downloaded?
    FIRST_DOWNLOAD = 0      # First time we want to populate database
    DAILY_RUN_APPEND = 0    # Can be run daily/regularly to download data from external -> disk -> database
    ONE_OFF = 0             # One off upload of files
    WRITE_CSV = 0           # Purely for downloading from the external source to CSV/Parquet/HDF5 file

    # Where should we dump the temporary FX data mini files and large H5 files
    # sometimes we might want to specify just a small section to download and specific _tickers
    # _temp_data_folder = 'E:/tcapy_tests_data/temp_01Jun2018'; _temp_large_data_folder = 'E:/tcapy_tests_data/temp_01Jun2018/large'
    # _temp_data_folder = 'E:/tcapy_tests_data/temp'; _temp_large_data_folder = 'E:/tcapy_tests_data/large'
    # _tickers = {'EURUSD' : 'EURUSD'}
    # start_date = '04 Nov 2017'; finish_date = '04 Nov 2017 09:00'
    # delete_cached_files = True

    # Usual default parameters
    start_date = None; finish_date = None

    # You may need to change these
    temp_data_folder = constants.temp_data_folder; temp_large_data_folder = constants.temp_large_data_folder

    start_date_csv = '01 May 2018'; finish_date_csv = '02 May 2018'

    # It's often worth keeping the binary files on disk, in case you need to redump to database later
    delete_cached_files = False

    if data_vendor == 'ncfx':
        from tcapy.data.databasepopulator import DatabasePopulatorNCFX as DatabasePopulator

        tickers = constants.ncfx_tickers
        data_store = 'arctic-ncfx'

    elif data_vendor == 'dukascopy':
        from tcapy.data.databasepopulator import DatabasePopulatorDukascopy as DatabasePopulator

        tickers = constants.dukascopy_tickers
        data_store = 'arctic-dukascopy'

    db_populator = DatabasePopulator(temp_data_folder=temp_data_folder, temp_large_data_folder=temp_large_data_folder,
                                     tickers=tickers, data_store=data_store)

    # The first time we are downloading data from the data vendor
    if FIRST_DOWNLOAD:
        # WARNING: for very first time we download choose the specified dates
        # you may need to change the delete cached files temporarily on disk
        msg = db_populator.download_from_external_source(append_data=True, start_date=start_date, finish_date=finish_date,
                                                             remove_duplicates=remove_duplicate_follow_on,
                                                             delete_cached_files=delete_cached_files, write_to_disk_db=True,
                                                             if_exists_ticker='replace', if_exists_table='append')

    # Download NEW data from data vendor server and then append to Arctic/MongoDB (if no data exists, then will download
    # past 7 months of data by default)
    if DAILY_RUN_APPEND:
        # Append market data (this should be run every day typically using a cron job, for appending new data)
        msg = db_populator.download_from_external_source(append_data=True, number_of_days=7 * 30,
                                                   remove_duplicates=remove_duplicate_follow_on,
                                                   delete_cached_files=delete_cached_files, write_to_disk_db=True,
                                                   start_date=None, finish_date=None,
                                                   if_exists_ticker='append', if_exists_table='append')

    # One off copy to a database from mini HDF5/Parquet files already dumped on disk
    if ONE_OFF:
        # Alternatively, we can copy mini HDF5/Parquet files from disk to Arctic/MongoDB (typically done during debugging)
        # that have already been generated when we run download_from_external_source
        db_populator.combine_mini_df_from_disk(tickers=tickers, remove_duplicates=remove_duplicate_follow_on)

        # Will overwrite the ticker for the market data (but WON'T delete the whole market database)
        db_populator.write_df_to_db(tickers=tickers, remove_duplicates=remove_duplicate_follow_on,
                                    if_exists_table = 'append', if_exists_ticker = 'replace')


    # Writes a CSV to disk from data vendor (does not attempt to write anything to the database)
    # will also dump temporary HDF5/Parquet files to disk (to avoid reloading them)

    # If you just want to write a CSV or HDF5/Parquet file might be easier to use eg. DatabaseSourceDukascopy and then
    # dump to disk with .to_csv from Pandas
    if WRITE_CSV:
        msg = db_populator.download_from_external_source(append_data=False, start_date=start_date_csv,
                                                         finish_date=finish_date_csv,
                                                         remove_duplicates=remove_duplicate_follow_on,
                                                         delete_cached_files=delete_cached_files,
                                                         write_temp_to_disk=True,
                                                         write_to_disk_db=False,
                                                         write_large_hdf5_parquet=True,
                                                         write_large_csv=True, csv_folder=csv_folder)

        print(msg)

    finish = time.clock()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
