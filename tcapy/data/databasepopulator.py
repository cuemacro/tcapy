from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc
import pytz

import datetime
from datetime import timedelta
import pandas as pd
import os
import glob

from tcapy.conf.constants import Constants
from tcapy.data.databasesource import AccessControl
from tcapy.util.timeseries import TimeSeriesOps
from tcapy.util.loggermanager import LoggerManager

from tcapy.util.utilfunc import UtilFunc

# Need this for WINDOWS machines, to ensure multiprocessing stuff works properly
from tcapy.util.swim import Swim;

constants = Constants()

# Compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

binary_format=constants.binary_default_dump_format # 'hdf5' or 'parquet'

if binary_format == 'hdf5':
    fileformat = 'h5' # 'h5' or 'gzip'
elif binary_format == 'parquet':
    fileformat = 'parquet'

class DatabasePopulator(ABC):
    """DatabasePopulator connects from one data source (typically an external one via a DatabaseSource eg. DatabaseNCFX)
    downloads historical data from that and then dumps it locally
    """

    def __init__(self, temp_data_folder=constants.temp_data_folder,
                 temp_large_data_folder=constants.temp_large_data_folder,
                 tickers=None, data_store=None, access_control=AccessControl()):

        self._temp_data_folder = temp_data_folder
        self._temp_large_data_folder = temp_large_data_folder
        self._tickers = None
        self._util_func = UtilFunc()
        self._time_series_ops = TimeSeriesOps()
        self._data_store = data_store
        self._access_control = access_control

        logger = LoggerManager().getLogger(__name__)

        if not(os.path.isdir(self._temp_data_folder)):
            logger.warning("Temp data folder " + self._temp_data_folder + " does not exist")

        if not(os.path.isdir(self._temp_large_data_folder)):
            logger.warning("Temp large data folder " + self._temp_data_folder + " does not exist")

        if tickers is not None:
            self._tickers = tickers

    @abc.abstractmethod
    def _fetch_market_data(self, start, finish, ticker, web_proxies=constants.web_proxies):
        """Fetches market data in a single download for a ticker. We need to be careful not to specify chunks which are
        too large, as many external sources will have a limit on how much data we can download in one chunk.

        Parameters
        ----------
        start : datetime
            Start date/time of the download

        finish : datetime
            Finish date/time of the download

        ticker : str
            Ticker to be downloaded

        web_proxies : dict
            Addresses for web proxies

        Returns
        -------

        """
        pass

    def _get_postfix(self):
        """The postfix which represents this data source, eg. 'ncfx' for New Change FX or 'dukascopy' for Dukascopy

        Returns
        -------
        str
        """
        pass

    @abc.abstractmethod
    def _get_output_data_source(self):
        """Gets the DatabaseSource object which represents how we wish to store the market data internally

        Returns
        -------
        DatabaseSource
        """
        return

    def _remove_weekend_points(self):
        return True

    @abc.abstractmethod
    def _get_input_data_source(self):
        """Gets the DatabaseSource object which represents how we input the market data (typically, this will be from
        an external data source)

        Returns
        -------
        DatabaseSource
        """
        return

    @abc.abstractmethod
    def _get_tickers(self):
        """List of _tickers that can accessedd from the external/input DatabaseSource

        Returns
        -------
        str (list)
        """
        return

    @abc.abstractmethod
    def _get_threads(self, start_data_hist, finish_date_hist):
        """How many threads to use when downloading from our external/input DatabaseSource

        Returns
        -------
        int
        """
        return

    def download_to_csv(self, start_date, finish_date, tickers, remove_duplicates=True, split_size='monthly',
                        chunk_int_min=None,
                        include_partial_periods=False,
                        write_temp_to_disk=True, write_large_csv=True, write_large_hdf5_parquet=True,
                        csv_folder=constants.csv_folder, csv_compression=None, return_df=False, web_proxies=constants.web_proxies):

        start_date = self._time_series_ops.date_parse(start_date)
        finish_date = self._time_series_ops.date_parse(finish_date)

        dates = self._util_func.split_date_single_list(start_date, finish_date, split_size=split_size,
                                                       add_partial_period_start_finish_dates=include_partial_periods)

        df_dict = {}
        msg = []

        for i in range(0, len(dates) - 1):
            msg_list, df_dict_list = self.download_from_external_source(
                start_date=dates[i], finish_date=dates[i+1], tickers=tickers,
                chunk_int_min=chunk_int_min,
                append_data=False, remove_duplicates=remove_duplicates,
                write_temp_to_disk=write_temp_to_disk,
                write_to_disk_db=False, write_large_csv=write_large_csv, write_large_hdf5_parquet=write_large_hdf5_parquet,
                csv_folder=csv_folder, csv_compression=csv_compression, return_df=return_df, web_proxies=web_proxies)

            if msg_list != []:
                msg.append(msg_list)

            if return_df:
                for k in df_dict_list.keys():
                    if k in df_dict.keys():
                        df_dict[k] = df_dict[k].append(df_dict_list[k])
                    else:
                        df_dict[k] = df_dict_list[k]

        return self._util_func.flatten_list_of_lists(msg), df_dict


    def download_from_external_source(self, append_data=True, remove_duplicates=True, if_exists_table='append',
                                      if_exists_ticker='append', number_of_days=30 * 7, chunk_int_min=None,
                                      start_date=None, finish_date=None, delete_cached_files=False, tickers=None,
                                      write_temp_to_disk=True,
                                      write_to_disk_db=True, read_cached_from_disk=True, write_large_csv=False, write_large_hdf5_parquet=True,
                                      csv_folder=constants.csv_folder, csv_compression=None, return_df=False, web_proxies=constants.web_proxies):
        """Downloads market data from an external source and then dumps to HDF5/Parquet files for temporary storage which is cached.
        If HDF5/Parquet cached files already exist for a time segment we read them in, saving us to make an external data call.

        Lastly, dumps it to an internal database.

        Parameters
        ----------
        append_data : bool
            True - only start collecting later data not already in database (ignoring number_of_days parameter)
            False - start collecting all data, ignoring anything stored in database

        remove_duplicates : bool
            True (default) - remove values which are repeated
            False - leave in repeated values

        if_exists_table : str
            'append' - if database table already exists append data to it
            'replace' - remove existing database table

        if_exists_ticker : str
            'append' - if ticker already exists in the database, append to it
            'replace' - replace any data for this ticker

        number_of_days : int
            Number of days to download data for

        chunk_int_min : int (None)
            Size of each download (default - specified in constants)

        Returns
        -------

        """
        # Swim()

        logger = LoggerManager.getLogger(__name__)

        if write_to_disk_db:
            data_source_local = self._get_output_data_source()

        if write_large_csv:
            if not (os.path.isdir(csv_folder)):
                logger.warn("CSV folder " + self._temp_data_folder + " where we are about to write does not exist")

        # What chunk size in minutes do we want for this data provider?
        if chunk_int_min is None:
            chunk_int_min = self._get_download_chunk_min_size()

        if chunk_int_min is None:
            chunk_size_str = None
        else:
            chunk_size_str = str(chunk_int_min) + "min"

        if tickers is None:
            tickers = self._get_tickers()

        if isinstance(tickers, str):
            tickers = [tickers]

        # If there's no start or finish date, choose a default start finish data
        if start_date is None and finish_date is None:
            finish_date = datetime.datetime.utcnow()
            finish_date = datetime.datetime(finish_date.year, finish_date.month, finish_date.day, 0, 0, 0, 0)

            start_date = finish_date - timedelta(days=number_of_days)  # 30*7
        else:
            start_date = self._time_series_ops.date_parse(start_date)
            finish_date = self._time_series_ops.date_parse(finish_date)

        if finish_date < start_date:
            logger.error("Download finish date is before start data!")

            return

        now = pd.Timestamp(datetime.datetime.utcnow(), tz='utc')

        # Do not allow downloading of future data!
        if finish_date > now:
            finish_date = now

        df_dict = {}

        # Loop through each ticker
        for ticker in tickers:

            has_old = False

            if delete_cached_files and write_to_disk_db:
                logger.info("Deleting all cached temp files for " + ticker)

                for name in glob.glob(self._temp_data_folder + '/*' + ticker + "*"):
                    try:
                        os.remove(name)
                    except:
                        logger.warn("Couldn't delete file " + name)

                logger.info("Finished deleting cached files for " + ticker)

            # If we have been asked to append data, load up what you can from the internal database
            # find the last point
            if append_data and if_exists_ticker == 'append' and write_to_disk_db:
                logger.info("Trying to download old data first for " + ticker)

                try:
                    df_old = data_source_local.fetch_market_data(start_date, finish_date, ticker, web_proxies=web_proxies)

                    # This will vary between _tickers (in particular if we happen to add a new ticker)
                    start_date = df_old.index[-1]

                    has_old = True

                    # Remove reference - big file!
                    df_old = None

                except Exception as e:
                    logger.info("No data found for ticker " + ticker + " with error: " + str(e))
            else:
                logger.info("Downloading new data for " + ticker + ".")

            # Date range may not work with timezones, so strip these back as naive -
            # but we assume timezone is UTC later on when deciding to strip weekends
            start_date = pd.Timestamp(start_date.replace(tzinfo=None))
            finish_date = pd.Timestamp(finish_date.replace(tzinfo=None))

            if finish_date - start_date < pd.Timedelta(days=1):
                start_date_list = [start_date, finish_date]
            else:
                # download from that last point to the present day
                start_date_list = pd.date_range(start_date, finish_date)

                start_date_list = [pd.Timestamp(x.to_pydatetime()) for x in start_date_list]

                if finish_date > start_date_list[-1]:
                    start_date_list.append(finish_date)

            df = None
            filename = os.path.join(self._temp_data_folder, ticker) + '.' + fileformat

            try:
                # df = UtilFunc().read_dataframe_from_hdf(filename)
                pass
            except:
                logger.info("Couldn't read HDF5/Parquet file for " + ticker)

            # Create downloads in x minute chunks (if we request very large chunks of data with certain data providers,
            # we could cause problems!)
            if df is None:
                df_remote_list = []

                # Loop by day (otherwise can end up with too many open files!)
                for i in range(0, len(start_date_list) - 1):

                    # Specifying a chunk size can also be helpful for use_multithreading a request
                    if chunk_size_str is not None:
                        start_date_hist, finish_date_hist = UtilFunc().split_into_freq(
                            start_date_list[i], start_date_list[i + 1], freq=chunk_size_str, chunk_int_min=chunk_int_min)
                    else:
                        start_date_hist = [start_date_list[i]]
                        finish_date_hist = [start_date_list[i + 1]]

                    # For FX and most other markets we should remove weekends (Note: cryptocurrencies do have weekend data)
                    if self._remove_weekend_points():
                        start_date_hist, finish_date_hist = UtilFunc().remove_weekend_points(start_date_hist, finish_date_hist,
                                                                                             timezone='UTC')

                    output = []

                    if constants.use_multithreading:

                        # Create a multiprocess object for downloading data
                        swim = Swim(parallel_library=constants.database_populator_threading_library)
                        pool = swim.create_pool(thread_no=self._get_threads())

                        result = [];

                        for i in range(0, len(start_date_hist)):
                            # output.append(self._fetch_market_data(start_date_hist[i], finish_date_hist[i], ticker))

                            result.append(
                                pool.apply_async(self._fetch_market_data,
                                                 args=(start_date_hist[i], finish_date_hist[i], ticker, write_temp_to_disk,
                                                       read_cached_from_disk, web_proxies)))

                        output = [p.get() for p in result]

                        swim.close_pool(pool, True)
                    else:
                        # Otherwise run in single threaded fashion
                        for i in range(0, len(start_date_hist)):
                            output.append(self._fetch_market_data(start_date_hist[i], finish_date_hist[i], ticker,
                                                                  write_to_disk=write_temp_to_disk,
                                                                  read_cached_from_disk=read_cached_from_disk,
                                                                  web_proxies=web_proxies))

                    # Get all the dataframe chunks and returned messages
                    df_list = [self._remove_duplicates_time_series(x, remove_duplicates, field='mid')
                               for x, y in output if x is not None]
                    msg_list = [y for x, y in output if x is not None and y is not None]

                    # Concatenate all the 5 (or larger) minute data chunks
                    try:
                        if df_list != []:
                            df_temp = pd.concat(df_list)

                            if df_temp is not None:
                                if not (df_temp.empty):
                                    df_remote_list.append(df_temp)

                    except Exception as e:
                        logger.error(str(e))

                if df_remote_list != []:
                    df = pd.concat(df_remote_list)

                    # Need to sort data (database assumes sorted data for chunking/searches)
                    df = df.sort_index()
                    df = self._time_series_ops.localize_as_UTC(df)

                    if write_large_hdf5_parquet:
                        if df is not None:
                            if not(df.empty):
                                key =  '_' + self._get_postfix() + "_" + \
                                       (str(df.index[0]) + str(df.index[-1])).replace(":", '_').replace(" ", '_')
                                filename = os.path.join(csv_folder, ticker + key) + '.' + fileformat

                                logger.debug("Writing file... " + filename)

                                # Temporary cache for testing purposes (also if the process crashes, we can read this back in)
                                UtilFunc().write_dataframe_to_binary(df, filename, format=binary_format)

            if df is not None:
                # Assume UTC time (don't want to mix UTC and non-UTC in database!)
                df = self._time_series_ops.localize_as_UTC(df)

            # write CSV
            if write_large_csv:
                if df is not None:
                    if not(df.empty):
                        key = '_' + self._get_postfix() + "_" + \
                              (str(df.index[0]) + str(df.index[-1])).replace(":", '_').replace(" ", '_')

                        if csv_compression is 'gzip':
                            df.to_csv(os.path.join(csv_folder, ticker + key + ".csv.gz"), compression='gzip')
                        else:
                            df.to_csv(os.path.join(csv_folder, ticker +  key + ".csv"))

            if return_df:
                df_dict[ticker] = df

            # Dump what we have locally (or whatever DatabaseSource we have defined)
            try:

                start_date = start_date.replace(tzinfo=pytz.utc)

                # Remove first point if matches last point from dataset
                if has_old:
                    if df.index[0] == start_date:
                        df = df[-1:]

                if df is not None:
                    df = df.sort_index()

                    df = self._remove_duplicates_time_series(df, remove_duplicates, field='mid')

                if write_to_disk_db and df is not None:
                    data_source_local.append_market_data(df, ticker,
                                                         if_exists_table=if_exists_table,
                                                         if_exists_ticker=if_exists_ticker)

                    logger.info("Wrote to database for " + ticker)

            except Exception as e:
                final_err = "Data was missing for these dates " + str(start_date) + " - " + str(finish_date) + " for " \
                            + str(tickers) + " Didn't write anything to disk or return any valid dataframe: " + str(e)

                logger.error(final_err)


            if df is None:
                msg_list.append("No downloaded data for " + str(start_date) + " - " + str(finish_date)
                                + ". Is this a holiday?")

        # Returns a status containing any failed downloads, which can be read by a user
        return msg_list, df_dict

    def _remove_duplicates_time_series(self, df, remove_duplicates, field='mid'):

        if remove_duplicates:
            df = self._time_series_ops.drop_consecutive_duplicates(df, field)

        return df

    def combine_mini_df_from_disk(self, tickers=None, remove_duplicates=True):
        """Combines the mini HDF5/Parquet files for eg. 5 min chunks and combine into a very large HDF5/Parquet file, which is likely to be
        for multiple months of data. Uses use_multithreading to speed up, by using a thread for each different ticker.

        Parameters
        ----------
        tickers : str (list or ditc)
            Ticker of each ticker

        remove_duplicates : bool
            Remove duplicated market prices, which follow one another

        Returns
        -------

        """

        if tickers is None: tickers = self._tickers.keys()

        if isinstance(tickers, dict): tickers = tickers.keys()

        if not (isinstance(tickers, list)):
            tickers = [tickers]

        if constants.use_multithreading:
            swim = Swim(parallel_library=constants.database_populator_threading_library)
            pool = swim.create_pool(thread_no=self._get_threads())

            result = []

            for i in range(0, len(tickers)):
                result.append(
                    pool.apply_async(self._combine_mini_df_from_disk_single_thread,
                                     args=(tickers[i], remove_duplicates,)))

            output = [p.get() for p in result]

            swim.close_pool(pool, True)

        else:
            for i in range(0, len(tickers)):
                self._combine_mini_df_from_disk_single_thread(tickers[i], remove_duplicates)

    def _combine_mini_df_from_disk_single_thread(self, ticker, remove_duplicates=True):

        logger = LoggerManager.getLogger(__name__)
        time_series_ops = TimeSeriesOps()

        logger.info('Getting ' + ticker + ' filenames...')
        temp_data_folder = self._temp_data_folder

        filename_list = []

        for root, dirnames, filenames in os.walk(temp_data_folder):

            for filename in filenames:
                if ticker in filename and '.' + fileformat in filename:
                    filename_h5_parquet = os.path.join(root, filename)

                    # if filename is less than 10MB add (otherwise likely a very large aggregated file!)
                    if os.path.getsize(filename_h5_parquet) < 10 * 1024 * 1024:
                        filename_list.append(filename_h5_parquet)

        df_list = []

        util_func = UtilFunc()

        logger.info('Loading ' + ticker + ' mini dataframe into  memory')

        i = 0

        if len(filename_list) == 0:
            logger.warn("Looks like there are no files for " + ticker + " in " + temp_data_folder +
                        ". Are you sure path is correct?")

        # Go through each mini file which represents a few minutes of data and append it
        for filename in filename_list:
            filesize = 0

            try:
                filesize = os.path.getsize(filename) / 1024.0
                df = util_func.read_dataframe_from_binary(filename, format=binary_format)

                i = i + 1

                # every 100 files print reading output@
                if i % 100 == 0:
                    logger.info('Reading ' + filename + ' number ' + str(i))

                if df is not None:
                    df = df.sort_index()
                    df = self._remove_duplicates_time_series(df, remove_duplicates, time_series_ops, field='mid')

                    df_list.append(df)
            except Exception as e:
                logger.warn('Failed to parse ' + filename + " of " + str(filesize) + "KB")  # + str(e))

            # if i > 1000:
            #    break

        # Assume UTC time (don't want to mix UTC and non-UTC in database!)
        if df_list == []:
            logger.warn('No dataframe read for ' + ticker + ', cannot combine!')

            return

        logger.info('About to combine ' + ticker + ' into large dataframe to write to disk...')

        df = pd.concat(df_list)
        df = time_series_ops.localize_as_UTC(df)

        df = df.sort_index()

        df = self._remove_duplicates_time_series(df, remove_duplicates, time_series_ops, field='mid')

        postfix = '-' + self._get_postfix() + '-with-duplicates'

        if remove_duplicates:
            postfix = '-' + self._get_postfix() + '-no-duplicates'

        filename = os.path.join(self._temp_large_data_folder, ticker + postfix) + '.' + fileformat

        df = time_series_ops.localize_as_UTC(df)
        util_func.write_dataframe_to_binary(df, filename, format=binary_format)

    def write_df_to_db(self, tickers=None, remove_duplicates=True, if_exists_table='append', if_exists_ticker='replace',
                       use_multithreading=constants.use_multithreading):
        """Loads up a large HDF5/Parquet file from disk into a pd DataFrame and then dumps locally.
        Uses use_multithreading to speed it up, by using a thread for each different ticker.

        Parameters
        ----------
        tickers : str (list or dict)
            List of _tickers

        remove_duplicates : bool
            True (default) - removes any follow on duplicates in the dataset

        if_exists_table : str
            'append' - if database table already exists append data to it
            'replace' - remove existing database table

        if_exists_ticker : str
            'append' - if ticker already exists in the database, append to it
            'replace' - replace any data for this ticker

        use_multithreading : bool
            Uses multithreading or not

        Returns
        -------

        """

        if tickers is None: tickers = self._tickers.keys()

        if isinstance(tickers, dict): tickers = tickers.keys()

        if not (isinstance(tickers, list)):
            tickers = [tickers]

        if use_multithreading:

            swim = Swim(parallel_library=constants.database_populator_threading_library)
            pool = swim.create_pool(thread_no=self._get_threads())

            result = []

            for i in range(0, len(tickers)):
                result.append(
                    pool.apply_async(self._write_df_to_db_single_thread,
                                     args=(tickers[i], remove_duplicates, if_exists_table, if_exists_ticker,)))

            output = [p.get() for p in result]

            swim.close_pool(pool, True)
        else:
            for i in range(0, len(tickers)):
                self._write_df_to_db_single_thread(tickers[i], remove_duplicates, if_exists_table, if_exists_ticker)

    def _write_df_to_db_single_thread(self, ticker, remove_duplicates=True, if_exists_table='append',
                                      if_exists_ticker='replace'):

        logger = LoggerManager.getLogger(__name__)

        postfix = '-' + self._get_postfix() + '-with-duplicates'

        if remove_duplicates:
            postfix = '-' + self._get_postfix() + '-no-duplicates'

        filename = os.path.join(self._temp_large_data_folder, ticker + postfix) + '.' + fileformat

        logger.info("Reading " + filename)

        util_func = UtilFunc()
        time_series_ops = TimeSeriesOps()
        data_source_local = self._get_output_data_source()

        df = util_func.read_dataframe_from_binary(filename, format=binary_format)

        if df is not None:
            df = time_series_ops.localize_as_UTC(df)

            data_source_local.append_market_data(df, ticker, if_exists_table=if_exists_table,
                                                 if_exists_ticker=if_exists_ticker)
        else:
            logger.warn("Couldn't write dataframe for " + ticker + " to database, appears it is empty!")

from tcapy.util.mediator import Mediator
from tcapy.analysis.tcarequest import MarketRequest

class DatabasePopulatorNCFX(DatabasePopulator):
    """Implements DatabasePopulator for New Change FX.
    """

    def __init__(self, temp_data_folder=constants.temp_data_folder, temp_large_data_folder=constants.temp_large_data_folder,
        tickers=None, data_store=constants.ncfx_data_store, access_control=AccessControl()):

        super(DatabasePopulatorNCFX, self).__init__(
            temp_data_folder=temp_data_folder, temp_large_data_folder=temp_large_data_folder, tickers=tickers, data_store=data_store,
            access_control=access_control)

    def _get_output_data_source(self):
        return Mediator.get_database_source_picker().get_database_source(MarketRequest(data_store=self._data_store))

    def _get_postfix(self):
        return 'ncfx'

    def _get_tickers(self):
        if self._tickers is None:
            return constants.ncfx_tickers.keys()

        return self._tickers.keys()

    def _get_tickers_vendor(self):
        if self._tickers is None:
            return constants.ncfx_tickers

        return self._tickers

    def _get_threads(self):
        return constants.ncfx_threads

    def _get_download_chunk_min_size(self):
        return constants.ncfx_chunk_min_size

    def _get_input_data_source(self):
        from tcapy.data.databasesource import DatabaseSourceNCFX

        return DatabaseSourceNCFX(username=self._access_control.ncfx_username, password=self._access_control.ncfx_password,
                                  url=self._access_control.ncfx_url)

    def _fetch_market_data(self, start, finish, ticker, write_to_disk=True, read_cached_from_disk=True, web_proxies=constants.web_proxies):
        logger = LoggerManager.getLogger(__name__)

        key = (str(start) + str(finish) + ticker + '_' + self._get_postfix()).replace(":", '_')

        filename = os.path.join(self._temp_data_folder, key) + '.' + fileformat
        util_func = UtilFunc()

        start_time_stamp = pd.Timestamp(start)
        finish_time_stamp = pd.Timestamp(finish)

        if self._remove_weekend_points():
            weekend_data = "Weekend? " + key

            weekday_point = UtilFunc().is_weekday_point(start_time_stamp, finish_time_stamp,
                                                        friday_close_nyc_hour=constants.friday_close_utc_hour,
                                                        sunday_open_utc_hour=constants.sunday_open_utc_hour)

            if not(weekday_point):
                return None, weekend_data

        df = None

        if read_cached_from_disk:
            if os.path.exists(filename):
                df = util_func.read_dataframe_from_binary(filename, format=binary_format)

                if df is not None:
                    logger.debug("Read " + filename + " from disk")

        if df is None:
            # Convert tcapy ticker into vendor ticker
            df = self._get_input_data_source().fetch_market_data(start, finish,
                                                                 ticker=self._get_tickers_vendor()[ticker], web_proxies=web_proxies)

            if df is not None:

                if write_to_disk:
                    # Write a small temporary dataframe to disk (if the process fails later, these can be picked up,
                    # without having a call the external vendor again
                    util_func.write_dataframe_to_binary(df, filename, format=binary_format)

        msg = None

        if df is None:
            msg = "No data? " + key

        return df, msg

class DatabasePopulatorDukascopy(DatabasePopulatorNCFX):
    """Implements DatabasePopulator for Dukascopy
    """

    def __init__(self, temp_data_folder=constants.temp_data_folder, temp_large_data_folder=constants.temp_large_data_folder,
        tickers=None, data_store=constants.dukascopy_data_store, access_control=AccessControl()):

        super(DatabasePopulatorDukascopy, self).__init__(
            temp_data_folder=temp_data_folder, temp_large_data_folder=temp_large_data_folder, tickers=tickers, data_store=data_store,
            access_control=access_control)

    def _get_output_data_source(self):
        return Mediator.get_database_source_picker().get_database_source(MarketRequest(data_store=self._data_store))

    def _get_postfix(self):
        return 'dukascopy'

    def _get_tickers(self):
        if self._tickers is None:
            return constants.dukascopy_tickers.keys()

        return self._tickers.keys()

    def _get_tickers_vendor(self):
        if self._tickers is None:
            return constants.dukascopy_tickers

        return self._tickers

    def _get_threads(self):
        return constants.dukascopy_threads

    def _get_download_chunk_min_size(self):
        return None

    def _get_input_data_source(self):
        from tcapy.data.databasesource import DatabaseSourceDukascopy

        return DatabaseSourceDukascopy()

    def _remove_weekend_points(self):
        return True
