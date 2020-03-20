from __future__ import division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.util.swim import Swim

import abc

import pandas as pd

import datetime

from tcapy.conf.constants import Constants
from tcapy.util.utilfunc import UtilFunc
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.timeseries import TimeSeriesOps
from tcapy.analysis.tcarequest import MarketRequest, TradeRequest

from tcapy.util.customexceptions import *

import os

import abc
import pandas as pd
import numpy as np
import glob
import os
import pytz
import re

import datetime

from tcapy.util.utilfunc import UtilFunc
from tcapy.util.loggermanager import LoggerManager
from tcapy.analysis.tcarequest import MarketRequest, TradeRequest
from tcapy.conf.constants import Constants

from tcapy.util.customexceptions import *

from tcapy.data.accesscontrol import AccessControl

import sqlalchemy

from sqlalchemy import MetaData, Column, Table
from sqlalchemy import String, DateTime, event, create_engine

# Compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

constants = Constants()

class DatabaseSource(ABC):
    """This class acts as low level way to access market and trade data stored in CSV files etc. DB etc.
    It returns pd DataFrames which can be used elsewhere in the project. We do a minimal
    amount of processing inside the database, and instead do it elsewhere in the project.

    Most implementations assume that market data for any particular ticker is stored in its own table. For trade/order
    data, we assume that trade data and order data is stored in separate tables. Unlike for market data, trade data for
    multiple tickers is assumed to be stored in the same table. Obviously, if internally you stored your trade data in a
    different way, you can, for example write an SQL VIEW to return the data in the appropriate form for tcapy.

    """

    def __init__(self, postfix=None):
        self._time_series_ops = TimeSeriesOps()
        self._util_func = UtilFunc()

        self.set_postfix(postfix=postfix)

    @abc.abstractmethod
    def fetch_market_data(self, start_date=None, finish_date=None, ticker=None):
        """Fetches market data for a particular ticker from the database.

        Parameters
        ----------
        start_date : str
            Start date of our market data

        finish_date : str
            Finish date of our market data

        ticker : str
            Ticker of our market data, we'd like to fetch (eg. EURUSD)

        Returns
        -------
        DataFrame
        """
        pass

    @abc.abstractmethod
    def fetch_trade_order_data(self, start_date=None, finish_date=None, ticker=None, table_name=None):
        """Fetches trade/order data from the database. Typically we assume that that trade and order data is stored
        in different tables.

        Parameters
        ----------
        start_date : str
            Start date of the trade/order data

        finish_date : str
            Finish date of the trade/order data

        ticker : str
            Ticker to fetch

        table_name : str
            Table where trade/order data

        Returns
        -------
        DataFrame
        """

        pass

    def convert_csv_to_table(self, csv_file, table_name, database_name, if_exists_table='replace',
                             if_exists_ticker='replace', market_trade_data='trade'):
        """Reads CSV data and then converts into DataFrames internally, before pushing into the database for storage.

        Typically, this is used when are beginning to populate the market and trade data. However, this can also be
        used on an ongoing basis.

        Parameters
        ----------
        csv_file : str
            Path to CSV file

        table_name : str
            Name of database table where we wish to store CSV data

        database_name : str
            Name of database where we wish to store CSV data

        if_exists_table : str
            What to do if the database table already exists
            * 'replace' - replaces the database table (default)
            * 'append' - appends to the database table

        if_exists_ticker : str
            What to do if the ticker already exists
            * 'replace' - replaces the ticker data already there (default)
            * 'append' - appends to the existing ticker data

        Returns
        -------

        """
        raise Exception("Not implemented")

    def append_trade_data(self, trade_df):
        raise Exception("Not implemented")

    def append_market_data(self, market_df, ticker):
        """Appends market data to the database for a particular ticker.

        Parameters
        ----------
        market_df : DataFrame
            Market data to be appended

        ticker : str
            Ticker of instrument

        Returns
        -------

        """
        raise Exception("Not implemented")

    def insert_market_data(self, market_df, ticker):
        """Inserts market data to the database for a particular ticker wherever it happens to be
        even if in the middle of the dataset (first deletes any data in the range of our insertion)

        Parameters
        ----------
        market_df : DataFrame
            Market data to be inserted

        ticker : str
            Ticker of instrument

        Returns
        -------

        """
        raise Exception("Not implemented")

    def _fetch_table(self, path):
        """

        Parameters
        ----------
        path : str
            Path of table

        Returns
        -------

        """
        pass

    def _format_date_time_milliseconds(self, dt):
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    def _parse_start_finish_dates(self, start_date, finish_date, utc=False):
        """Converts string based date/times into pandas.Timestamp objects (if they are strings). Also strips away
        timezone data, which can be problematic when using certain databases.

        Parameters
        ----------
        start_date : str or datetime
            Start date of download

        finish_date : str or datetime
            Finish date of download

        Returns
        -------
        pandas.Timestamp, pandas.Timestamp
        """

        if start_date is None and finish_date is None: return start_date, finish_date

        try:
            start_date = self._util_func.parse_datetime(start_date)
            finish_date = self._util_func.parse_datetime(finish_date)
        except:
            raise Exception("Couldn't parse start/finish dates: " + str(start_date) + " - " + str(finish_date))

        # now try to set the appropriate timezone
        try:
            if utc:
                start_date = start_date.replace(tzinfo=pytz.utc)
                finish_date = finish_date.replace(tzinfo=pytz.utc)
            else:
                start_date = start_date.replace(tzinfo=None)
                finish_date = finish_date.replace(tzinfo=None)
        except:
            pass

        if finish_date < start_date:
            raise Exception("Finish date before start date? " + str(start_date) + ' - ' + str(finish_date))

        return start_date, finish_date

    def mirror_data_timezone(self, df, start_date, finish_date):

        if df.index.tz is not None:
            start_date = start_date.replace(tzinfo=df.index.tz)
            finish_date = finish_date.replace(tzinfo=df.index.tz)

        return start_date, finish_date

    def set_postfix(self, postfix=''):
        """Creates a postfix for each market data ticker. Can be useful for storing multiple ticker sources in the same
        table eg. EURUSD-ncfx

        Parameters
        ----------
        postfix : str
            eg. ncfx

        Returns
        -------

        """
        if postfix is None:
            self.postfix = ''

            return
        elif postfix != '':
            self.postfix = '-' + postfix
            # print(self.postfix)
            return
        else:
            self.postfix = ''

            return

    def _downsample_localize_utc(self, df, convert=False):
        """Downsamples the numerical data in a DataFrame and sets all times to UTC format also checks index exists (and
        converts to DatetimeIndex if necessary.

        Parameters
        ----------
        df : DataFrame
            Dataset

        convert : bool
            Should it try to convert date/time cols if their timezone already exists

        Returns
        -------
        DataFrame
        """
        if df is None: return None

        try:
            if df.index.name != 'Date':
                df = df.set_index('date')

        except:
            pass

        df.index.name = 'Date'

        if not(isinstance(df.index, pd.DatetimeIndex)):
            df.index = pd.DatetimeIndex(df.index)

        df = self._time_series_ops.downsample_time_series_floats(df, constants.downsample_floats)

        return self._time_series_ops.localize_cols_as_UTC(df, constants.date_columns, index=True, convert=convert)

    def _writeable_dataframe(self, df, ticker, data_type):
        """Does the DataFrame have the right fields to be written as market data?

        Parameters
        ----------
        df : DataFrame
            DataFrame with market data

        ticker : str

        data_type : str ('market' or 'trade')
            Does the DataFrame have market data or trade/order data?

        Returns
        -------
        bool
        """
        logger = LoggerManager.getLogger(__name__)

        if data_type == 'market':
            if 'bid' not in df.columns and 'ask' not in df.columns and 'mid' in df.columns:
                pass
            elif 'bid' in df.columns and 'ask' in df.columns:
                pass
            else:
                err_msg = "No data for ticker " + ticker + ". No data to copy to Arctic!"

                logger.error(err_msg)

                raise DataMissingException(err_msg)

        return True

    def _remove_duplicates_market_data(self, df, remove_duplicates):
        """Removes consecutive points which are repeated. For many TCA style calculations this is safe to do. However,
        if we have other columns such as volume, removing these points could cause issues. This method will first check
        that the dataset does not contain any of these columns (like volume), before allowing the removal of duplicates.

        Parameters
        ----------
        df : DataFrame
            Market data

        remove_duplicates : bool
            Should consecutive duplicates be removed?

        Returns
        -------
        DataFrame
        """

        for c in constants.avoid_removing_duplicate_columns:
            if c in df.columns:
                return df

        if 'mid' in df.columns:
            if remove_duplicates:
                df = self._time_series_ops.drop_consecutive_duplicates(df, 'mid')
        elif 'bid' in df.columns and 'ask' in df.columns:
            if remove_duplicates:
                df = self._time_series_ops.drop_consecutive_duplicates(df, ['bid', 'ask'])

        return df

    def _force_utc_timezone_parse(self, df, date_format):
        """Parses the DataFrame for dates and also assigns the timezone to be UTC (removing whatever timezone they were before)

        Parameters
        ----------
        df : DataFrame
            DataFrame to be parsed

        date_format : str
            Date format to be parsed

        Returns
        -------
        DataFrame
        """

        if hasattr(df.index, 'tz'):
            if df.index.tz is None:
                df.index = pd.to_datetime(df.index, format=date_format).tz_localize(None)
                df.index = df.index.tz_localize(pytz.utc)
        else:
            df.index = pd.to_datetime(df.index, format=date_format).tz_localize(None)
            df.index = df.index.tz_localize(pytz.utc)

        for d in constants.date_columns:
            if d in df.columns:
                if hasattr(df[d], 'tz'):
                    if df[d].tz is None:
                        df[d] = pd.to_datetime(df[d], format=date_format).tz_localize(None)
                        df[d] = df[d].tz_localize(pytz.utc)
                else:
                    df[d] = pd.to_datetime(df[d], format=date_format).tz_localize(None)
                    df[d] = df[d].tz_localize(pytz.utc)

        return df

    def _convert_type_columns(self, df, date_format=None, date_columns=constants.date_columns, numeric_columns=constants.numeric_columns,
                              string_columns=constants.string_columns):
        """Converts the types in a DataFrame to numeric, date and strings (user specified)

        Parameters
        ----------
        df : DataFrame
            DataFrame to be converted

        date_format : str
            Format of dates to be parsed

        date_columns : str (arr)
            Columns to be converted into datetimnes

        numeric_columns : str (arr)
            Columns to be converted to numeric values

        string_columns : str (arr)
            Columns to be converted to strings

        Returns
        -------
        DataFrame
        """
        if df is None:
            return df

        # make sure we store date/times in appropriate format (NOT as strings) - we don't attempt to store
        # timezones as this can can cause complications
        for d in date_columns:
            if d in df.columns:
                if date_format is None:
                    df[d] = pd.to_datetime(df[d])  # .tz_localize(pytz.utc)
                else:
                    df[d] = pd.to_datetime(df[d], format=date_format)

        for d in numeric_columns:
            if d in df.columns:
                df[d] = pd.to_numeric(df[d], errors='coerce')

        for d in string_columns:
            if d in df.columns:
                df[d] = df[d].astype(str)

        return df

    def _check_data_integrity(self, df, market_trade_data):
        """Checks the various columns of the dataset for data integrity, such as checking the number of empty strings in
        a string column

        Parameters
        ----------
        df : DataFrame
            Data to be checked

        market_trade_data : str
            'trade' - trade data being dumped
            'market' - market data being dumped

        Returns
        -------

        """

        logger = LoggerManager.getLogger(__name__)

        if market_trade_data == 'trade':

            # check every string column, and warn if there are 'empty' strings
            # these can cause problems with filtering later on
            for c in constants.string_columns:
                if c in df.columns:
                    empty_no = (df[c] == '').sum()

                    if empty_no > 0:
                        logger.warn('Column ' + c + ' has ' + str(empty_no) + " entries, can cause problems with "
                                                                              "filtering! Recommend to fill with 'NA'.")
                    else:
                        logger.debug("No empty strings in column " + c)

########################################################################################################################

class DatabaseSourcePicker(object):
    """Additional database sources can be defined here

    """

    def get_database_source(self, data_request):

        database_source = None

        if isinstance(data_request.data_store, pd.DataFrame) or data_request.data_store == 'dataframe':

            if isinstance(data_request, MarketRequest):
                database_source = DatabaseSourceDataFrame(market_df=data_request.data_store)
            elif isinstance(data_request, TradeRequest):
                database_source = DatabaseSourceDataFrame(trade_df=data_request.data_store)

        else:
            # Postfix is an additional label added to table names
            # implemented only for Arctic
            split_data_source = data_request.data_store.split('-')
            postfix = ''

            data_store = split_data_source[0];

            if len(split_data_source) > 1:
                postfix = split_data_source[1]

            # Has the user specified access control/username/passwords for databases
            if data_request.access_control is None:

                # If not use default username/password in constants
                access_control = AccessControl()
            else:
                access_control = data_request.access_control

            if data_store == 'ms_sql_server':
                database_source = DatabaseSourceMSSQLServer(username=access_control.ms_sql_server_username,
                                                            password=access_control.ms_sql_server_password)
            elif data_store == 'postgres':
                database_source = DatabaseSourcePostgres(username=access_control.postgres_username,
                                                         password=access_control.postgres_password)
            elif data_store == 'mysql':
                database_source = DatabaseSourceMySQL(username=access_control.mysql_username,
                                                      password=access_control.mysql_password)
            elif data_store == 'arctic':
                # allow multiple datasources to be stored in arctic (by using a postfix)
                # eg. EURUSD-ncfx, EURUSD-dukascopy
                database_source = DatabaseSourceArctic(postfix=postfix, username=access_control.arctic_username,
                                                       password=access_control.arctic_password)
            elif data_store == 'influxdb':
                # allow multiple datasources to be stored in influxdb (by using a postfix)
                # eg. EURUSD-ncfx, EURUSD-dukascopy
                database_source = DatabaseSourceInfluxDB(postfix=postfix, username=access_control.influxdb_username,
                                                         password=access_control.influxdb_password)
            elif data_store == 'kdb':
                database_source = DatabaseSourceKDB(postfix=postfix, username=access_control.kdb_username,
                                                    password=access_control.kdb_password)
            elif 'csv' in data_store or '.h5' in data_store or '.gzip' in data_store or '.parquet' in data_store:
                if os.path.isfile(data_store):
                    if isinstance(data_request, MarketRequest):
                        database_source = DatabaseSourceCSVBinary(market_data_database_csv=data_store)
                    elif isinstance(data_request, TradeRequest):
                        database_source = DatabaseSourceCSVBinary(trade_data_database_csv=data_store)
                else:
                    database_source = DatabaseSourceCSVBinary()
            elif data_store == 'ncfx':
                database_source = DatabaseSourceNCFX()
            elif data_store == 'dukascopy':
                database_source = DatabaseSourceDukascopy()

        return database_source

########################################################################################################################

class DatabaseSourceCSV(DatabaseSource):
    """Implements DatabaseSource for CSV datasets, both for market and trade/order data.

    """

    def __init__(self, market_data_database_csv=None, trade_data_database_csv=None):

        super(DatabaseSourceCSV, self).__init__(postfix=None)

        self._market_data_database_csv = market_data_database_csv
        self._trade_data_database_csv = trade_data_database_csv

    def fetch_market_data(self, start_date=None, finish_date=None, ticker=None, table_name=None, date_format=None):
        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)

        if table_name is None:
            table_name = self._market_data_database_csv

        df = self._fetch_table(table_name, date_format=date_format)

        if start_date != None:
            try:
                df = df[df.index >= start_date]
            except:
                return None

        if finish_date != None:
            try:
                df = df[df.index <= finish_date]
            except:
                return None

        if ticker != None:
            try:
                df = df[df['ticker'] == ticker]
            except:
                return None

        return self._downsample_localize_utc(df)

    def fetch_trade_order_data(self, start_date=None, finish_date=None, ticker=None, table_name=None, date_format=None):
        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)

        if table_name is None:
            table_name = self._trade_data_database_csv

        df = self._fetch_table(table_name, date_format=date_format)

        if start_date != None:
            try:
                df = df[df.index >= start_date]
            except:
                return None

        if finish_date != None:
            try:
                df = df[df.index <= finish_date]
            except:
                return None

        if ticker != None:
            try:
                df = df[df['ticker'] == ticker]
            except:
                return None

        return self._downsample_localize_utc(df)

    def _fetch_table(self, csv_file_path, date_format=None):

        # with a CSV path from disk
        if not(os.path.exists(csv_file_path)):
            err_msg = "CSV file not found " + csv_file_path

            raise Exception(err_msg)

        # read CSV file (careful: parse dates correctly)
        df = pd.read_csv(csv_file_path, index_col=0)

        df.index = pd.to_datetime(df.index, format=date_format)

        for c in constants.date_columns:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], format=date_format)

        # we'll add the UTC timezone back later
        # for pandas 0.25.0
        df = df.tz_localize(None)

        return df

########################################################################################################################
import base64, io


class DatabaseSourceCSVBinary(DatabaseSourceCSV):
    """Implements DatabaseSource for CSV/H5/JSON datasets which are read from disk (or are in binary format, such as through
    a web request), both for market and trade/order data.

    """

    def __init__(self, market_data_database_csv=None, trade_data_database_csv=None):

        super(DatabaseSourceCSVBinary, self).__init__(
            market_data_database_csv=market_data_database_csv, trade_data_database_csv=trade_data_database_csv)

    def _set_date_index(self, df):
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'Date'})
            df = df.set_index('Date')
        elif 'Date' in df.columns:
            df = df.set_index('Date')
        elif df.index.name == 'date':
            df.index.name = 'Date'
        elif df.index.name == 'Date':
            pass
        else:
            raise Exception('No column with label Date, cannot create index')

        return df

    def _fetch_table(self, csv_file_path, date_format=None):

        logger = LoggerManager.getLogger(__name__)

        # For data sent via a web request
        accepted_file_tags = ["data:application/vnd.ms-excel;base64", 'data:text/csv;base64']

        # With a CSV path from disk
        if '.csv' in csv_file_path:
            if not (os.path.exists(csv_file_path)):
                err_msg = "CSV file not found " + csv_file_path

                logger.error(err_msg)

                raise Exception(err_msg)

            # Read CSV file (careful: parse dates correctly)
            df = pd.read_csv(csv_file_path)

        elif '.parquet' in csv_file_path or '.gzip' in csv_file_path:
            if not (os.path.exists(csv_file_path)):
                err_msg = "Parquet file not found " + csv_file_path

                logger.error(err_msg)

                raise Exception(err_msg)

            # Read CSV file
            df = pd.read_parquet(csv_file_path)

        elif '.h5' in csv_file_path:
            # For an H5 file
            if not (os.path.exists(csv_file_path)):
                err_msg = "H5 file not found " + csv_file_path

                logger.error(err_msg)

                raise Exception(err_msg)

            df = self._util_func.read_dataframe_from_binary(csv_file_path)
        else:
            df = None

            # With a preloaded CSV/uploaded file
            for acc in accepted_file_tags:
                if acc in csv_file_path:
                    content_type, content_string = csv_file_path.split(',')

                    decoded = base64.b64decode(content_string)

                    df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))

                    break

            if df is None:
                # otherwise assume JSON format
                df = pd.read_json(csv_file_path)

        df = self._set_date_index(df)

        df.index = pd.to_datetime(df.index, format=date_format)

        for c in constants.date_columns:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], format=date_format)

        # We'll add the UTC timezone back later
        df = df.tz_localize(None)

        return df


########################################################################################################################

class DatabaseSourceSQL(DatabaseSource):
    """Abstract class for fetching market/trade data from an SQL style database. This needs to be implemented for the
    SQL database type which you use (eg. MySQL, SQL Server etc.)

    """

    def __init__(self, server_host=None, server_port=None, username=None, password=None, trade_data_database_name=None):
        """Initialises SQL object.

        """

        super(DatabaseSourceSQL, self).__init__(postfix=None)

        self._server_host = server_host
        self._server_port = server_port
        self._username = username
        self._password = password

        self._trade_data_database_name = trade_data_database_name

    @abc.abstractmethod
    def _get_database_engine(self, server_host=None, server_port=None,
                             username=None, password=None,
                             database_name=None, table_name=None):
        """Creates an SQLAlchemy database engine, which later be used to interact with the database. Also outputs the
        connection string used.

        Parameters
        ----------
        server_host : str
            IP of the database server

        server_port : str
            Port of the database on the server

        username : str
            Username for server

        password : str
            Password for server

        database_name : str
            Database name

        table_name : str
            Table name on the database

        Returns
        -------
        Engine, str
        """
        pass

    def fetch_trade_order_data(self, start_date=None, finish_date=None, ticker=None, table_name=None,
                               database_name=None):
        """Fetches trade/order data from an SQL database and returns as a DataFrame

        Parameters
        ----------
        start_date : str
            Start date

        finish_date : str
            Finish date

        ticker : str
            Ticker to be collected

        table_name : str
            Table containing this particular trade/order data

        database_name : str
            Database containing trade/order data

        Returns
        -------
        DataFrame
        """
        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)

        sql_query = 'select * '

        where_clause = ''

        # Create start date condition as part of WHERE clause
        if start_date is not None:
            where_clause = self._combine_where_clause(where_clause, "([date] >= '"
                                                      + self._format_date_time_milliseconds(start_date) + "')")

        # Create start date condition as part of WHERE clause
        if finish_date is not None:
            where_clause = self._combine_where_clause(where_clause, "([date] <= '"
                                                      + self._format_date_time_milliseconds(finish_date) + "')")

        # Ensure ticker is also selected
        if ticker is not None:
            where_clause = self._combine_where_clause(where_clause, "(ticker = '" + ticker + "')")

        if where_clause != '':
            where_clause = 'where ' + where_clause

        if database_name is None:
            database_name = self._trade_data_database_name

        sql_query = sql_query + ' from ' + table_name + ' ' + where_clause

        df = self._fetch_table(database_name, table_name, sql_query)
        df = self._downsample_localize_utc(df)
        df = self._convert_type_columns(df)

        return df

    def _combine_where_clause(self, where_clause, new_where):
        """Appends conditions to an existing SQL WHERE clause

        Parameters
        ----------
        where_clause : str
            Existing SQL WHERE clause

        new_where : str
            Additional condition for WHERE clause

        Returns
        -------
        str
        """

        if where_clause == '':
            return where_clause + ' ' + new_where
        else:
            return where_clause + ' and ' + new_where

    def fetch_market_data(self, start_date=None, finish_date=None, ticker=None):
        logger = LoggerManager.getLogger(__name__)

        err_msg = "SQL market data reader not implemented. Recommend using Arctic to store market data."

        logger.error(err_msg)

        return Exception(err_msg)

    def _fetch_table(self, database_name, table, sql_query):
        """Fetches data from SQL database as a pd DataFrame

        Parameters
        ----------
        database_name : str
            Database name

        table : str
            Table stored data

        sql_query : str
            SQL string used for fetching data

        Returns
        -------
        DataFrame
        """
        logger = LoggerManager.getLogger(__name__)

        # Connect to database
        engine, con_str = self._get_database_engine(database_name=database_name)

        df = None

        try:
            df = pd.read_sql(sql_query, engine)  # , coerce_float=False)

            records = 0

            if df is not None:
                records = len(df.index)

            logger.debug('Excecuted SQL query: ' + sql_query + ", " + str(records) + " returned")

        except Exception as e:
            logger.error("Error fetching SQL query: " + str(e))

        # Careful: don't ask for too many dates at once, otherwise could make the database roll over
        return df

    def convert_csv_to_table(self, csv_file, ticker, table_name, database_name=None,
                             if_exists_table='replace',
                             if_exists_ticker='replace', market_trade_data='trade', date_format=None):
        """Copies data in CSV file and does a bulk dump into an database table. Assumes each CSV file contains at most
        one symbol.

        Parameters
        ----------
        csv_file : str (list)
            CSV file to be read in

        ticker : str (list) - (recommend: None)
            ticker for each CSV file

        table_name : str
            Table name where the dump is to occur

        database_name : str
            Database name to be dumped to

        if_exists_table : str
            What to do if the database table already exists
            * 'replace' - replaces the database table (default)
            * 'append' - appends to the database table

        if_exists_ticker : str
            What to do if the ticker already exists
            * 'replace' - replaces the ticker data already there (default)
            * 'append' - appends to the existing ticker data
        Returns
        -------

        """
        logger = LoggerManager.getLogger(__name__)

        if market_trade_data == 'market':
            err_msg = 'SQL interface not implemented for market data, can only use for trade data'

            logger.error(err_msg)

            raise Exception(err_msg)

        if market_trade_data == 'trade' and database_name is None:
            database_name = self._trade_data_database_name

        # Get database connection
        engine, con_str = self._get_database_engine(database_name=database_name)

        if isinstance(csv_file, str):
            csv_file = [csv_file]

        if isinstance(ticker, str):
            ticker = [ticker]

        for i in range(0, len(csv_file)):

            # Only appropriate to store different tickers in different table for market data!
            # for trade data store in one table
            if ticker is not None:
                tick = ticker[i]

                # con_tick = con_str + "_" + tick.lower()
                table_name_tick = table_name + '_' + tick.lower()

                logger.info("Parsing " + str(csv_file[i]) + " before database dump for symbol " + tick)
            else:
                # con_tick = con_str
                table_name_tick = table_name

                logger.info("Parsing " + str(csv_file[i]) + " before database dump for table " + table_name_tick)

            # Load up CSV into memory and converts certain columns to dates
            if not (os.path.exists(csv_file[i])):
                err_msg = "CSV file not found " + csv_file[i]

                logger.error(err_msg)

                raise Exception(err_msg)

            df = pd.read_csv(csv_file[i], index_col=0)

            df.index = pd.to_datetime(df.index, format=date_format)

            # for pandas 0.25.0
            df = df.tz_localize(None)

            # Shouldn't usually be necessary to store timezone inside database, as we assume all times are in UTC format
            # not every SQL database can store timezones, so can be problematic to include them

            # try:
            #     df.index = df.index.tz_localize(pytz.utc)
            # except:
            #     pass
            #

            df = self._convert_type_columns(df, date_format=date_format)

            # for i in range(0, len(df.index)):
            #     df1 = df.iloc[[i]]
            #
            #     # TODO load CSV into pd by batches (otherwise can run out of memory for large datasets)
            #     df1.to_sql(table_name, engine, if_exists = if_exists_table, index = True)
            #
            #     print(i)

            # print(df.dtypes)
            self._write_to_db(df, engine, table_name_tick, if_exists_table, market_trade_data)

    def append_trade_data(self, trade_df, table_name, database_name, if_exists_table='replace',
                          if_exists_ticker='replace', market_trade_data='trade'):

        # Get database connection
        engine, con_str = self._get_database_engine(database_name=database_name)

        self._write_to_db(trade_df, engine, table_name, database_name, if_exists_table=if_exists_table,
                          if_exists_ticker=if_exists_ticker, market_trade_data=market_trade_data)

        engine.close()

    def _write_to_db(self, df, engine, table_name_tick, if_exists_table, market_trade_data):
        logger = LoggerManager.getLogger(__name__)

        schema = None

        # SQLAlchemy may put double [] around a table name (which causes SQL syntax error), so remove that from
        # the initial table name supplied
        # eg. CREATE TABLE [[dbo].[trade]] ... WRONG
        # but we need to extract the schema from this to add as an additional parameter to give later to SQLAlchemy
        if '.' in table_name_tick:
            schema = self._replace_table_name_chars(table_name_tick.split(".")[0])
            sqlalchemy_table_name_tick = self._replace_table_name_chars(table_name_tick.split(".")[1])
        else:
            sqlalchemy_table_name_tick = table_name_tick

        logger.debug("About to write to SQL database...")

        # Force sorted by date before dumping (otherwise makes searches difficult)
        df = df.sort_index()

        columns_already_in_db = []

        con = engine.connect();

        # To try see if we can get the current columns in this database table (if it exists)
        try:
            metadata = MetaData(con)

            table = Table(sqlalchemy_table_name_tick, metadata,
                          autoload=True, schema=schema
                          )

            columns_already_in_db = [m.key for m in table.columns]
        except:
            pass

        metadata = MetaData(bind=engine)

        cols = self._sql_col_maker(df)

        cols_in_new_data = [c.name for c in cols]

        if market_trade_data == 'trade':
            must_have_columns = ['date', 'id', 'ticker']

            for m in must_have_columns:
                if m not in cols_in_new_data:
                    err_msg = "Column " + m + " not in the new dataset to be dumped in SQL database, check your input data!"

                    logger.error(err_msg)

                    raise DataMissingException(err_msg)
        else:
            err_msg = "Market data writing not implemented in SQL"

            logger.error(err_msg)

            raise Exception(err_msg)
            # must_have_columns = ['date']

        for c in cols:
            if c.name in must_have_columns:
                c.nullable = False

        table = Table(sqlalchemy_table_name_tick, metadata,
                      *cols, schema=schema
                      )

        # if the user has wanted to replace the table, we dump any existing data in it
        if if_exists_table == 'replace':
            try:
                metadata.drop_all(engine)
            except:
                pass
        else:
            if columns_already_in_db != []:
                cols_in_new_data = [c.name for c in cols]

                # check if the columns already in the database match in the new data
                if set(cols_in_new_data) != set(columns_already_in_db):
                    err_msg = "Columns in database, " + str(columns_already_in_db) \
                              + " do not match those in the new dataset, " + str(cols_in_new_data)

                    logger.error(err_msg)

                    raise DataMissingException(err_msg)

        metadata.create_all(engine)

        con = engine.connect()
        from sqlalchemy.sql import text

        # Create an primary on the Date/id/ticker (will fail if there's already an index) for trade data
        # want to prevent a situation where someone mistakenly adds same trade/order again
        try:
            if market_trade_data == 'trade':
                result = con.execute(
                    text('ALTER TABLE ' + table_name_tick + ' ADD CONSTRAINT ' + self._replace_table_name_chars(
                        table_name_tick)
                         + '_PK_trade PRIMARY KEY ("date", id, ticker)'))
        except Exception as e:
            print(str(e))
            logger.warn("Primary key already exists...")

        try:
            result = con.execute(
                text('CREATE INDEX ' + table_name_tick + '_idx_date ON ' + table_name_tick + '("date" ASC)'))
        except:
            logger.warn("Index already exists...")

        con.close()

        # Just before dumping to database check data columns
        self._check_data_integrity(df, market_trade_data=market_trade_data)

        # This will fail if we try to insert the *SAME* trades/orders, which have the same dates/id/tickers
        df.to_sql(sqlalchemy_table_name_tick, engine, if_exists='append', index=True,
                  chunksize=constants.sql_dump_record_chunksize, schema=schema)

    def _replace_table_name_chars(self, table_name):

        for i in ['[', ']', '.']:
            table_name = table_name.replace(i, "")

        return table_name

    def _sql_col_maker(self, df):
        """For a DataFrame creates columns which map pd data types to SQL data types

        Parameters
        ----------
        df : DataFrame
            Data to be stored in the SQL database

        Returns
        -------
        Column (list)
        """
        cols = []

        if df.index is not None:
            cols.append(self._create_sql_column(df.index.name, df.index.dtype))

        for i, j in zip(df.columns, df.dtypes):
            cols.append(self._create_sql_column(i, j))

        return cols

    def _datetime(self):
        return DateTime(6)

    def _create_sql_column(self, field, fieldtype):
        """For a particular field name and its associated data type, creates the corresponding column/SQL data type
        associated with it.

        Parameters
        ----------
        field : str
            Field name

        fieldtype : str
            Field data type
        Returns
        -------
        Column
        """

        if "datetime" in str(fieldtype) or 'date' in field:
            return Column(field, self._datetime())

        elif "object" in str(fieldtype):
            return Column(field, String(100))  # may need to make longer!!

        elif "float" in str(fieldtype):
            return Column(field, sqlalchemy.DECIMAL(25, 10))

        elif "int" in str(fieldtype):
            return Column(field, sqlalchemy.DECIMAL(25, 10))

        return None

    # @abc.abstractmethod
    # def _sql_text_type(self):
    #     pass


class DatabaseSourceMSSQLServer(DatabaseSourceSQL):
    """Implements the DatabaseSourceSQL class for MS SQL Server instances.

    """

    def __init__(self, server_host=constants.ms_sql_server_host, server_port=constants.ms_sql_server_port,
                 username=constants.ms_sql_server_username, password=constants.ms_sql_server_password,
                 trade_data_database_name=constants.ms_sql_server_trade_data_database_name):
        """Initialises SQL object.

        """

        super(DatabaseSourceMSSQLServer, self).__init__(server_host=server_host, server_port=server_port,
                                                        username=username, password=password,
                                                        trade_data_database_name=trade_data_database_name)

    def _get_database_engine(self, database_name=None, table_name=None):
        """Gets an SQLAlchemy engine associated with a MS SQL Server, which can be used elsewhere to interact with the
        database (for example, to fetch tables). Also returns the connection string which was used.

        Parameters
        ----------
        server_host : str
            IP/hostname of MS SQL Server

        server_port : str
            Port of MS SQL Server instance

        database_name : str
            Database name

        table_name : str
            Table name

        Returns
        -------
        Engine, str
        """

        # Official Microsoft pyodbc driver is recommended
        if constants.ms_sql_server_python_package == 'pyodbc':
            if constants.ms_sql_server_use_trusted_connection:
                con_exp = "mssql+pyodbc://" + str(self._server_host) + ":" + str(self._server_port)
            else:
                con_exp = "mssql+pyodbc://" + self._username + ":" + self._password \
                          + "@" + str(self._server_host) + ":" + str(self._server_port)

        elif constants.ms_sql_server_python_package == 'pymssql':

            # This driver is not recommended, as can't deal with enough decimal places
            con_exp = "mssql+pymssql://" + self._username + ":" + self._password \
                      + "@" + str(self._server_host)

        if database_name is not None:
            con_exp = con_exp + "/" + database_name

            if table_name is not None:
                con_exp = con_exp + "::" + table_name

        if constants.ms_sql_server_python_package == 'pyodbc':
            con_exp = con_exp + "?driver=" + constants.ms_sql_server_odbc_driver

        if constants.ms_sql_server_python_package == 'pymssql':
            con_exp = con_exp + "?tds_version=7.3"

        if constants.ms_sql_server_use_trusted_connection and constants.ms_sql_server_python_package == 'pyodbc':
            con_exp = con_exp + ";trusted_connection=yes"
            # <- for Linux clients can't use trusted connection with pyodbc, so do NOT use, instead specify
            # SQL Server username and password!!! To use Windows username and password, use pymssql

            # Reference for installing Kerberos in Linux
            # https://hammadr.wordpress.com/2017/09/26/ms-sql-connecting-python-on-linux-using-active-directory-credentials-and-kerberos/
            if constants.plat == 'linux':
                # Note that if you have krb5.conf file, which has default realms, then it is likely
                # username@realm login approach won't work, can just add username instead
                from subprocess import Popen, PIPE

                # Get fresh Kerberos ticket (using a custom kinit script, which must supply username and password)
                if constants.ms_sql_server_use_custom_kerberos_script:  # so don't store username and password in constants
                    kinit_args = [constants.ms_sql_server_kinit_custom_script_path]
                    kinit = Popen(kinit_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
                else:
                    # Takes username/password from constants
                    if constants.ms_sql_server_use_krb5_conf_default_realm:
                        # use realm from krb5.conf file
                        kinit_args = [constants.ms_sql_server_kinit_path,
                                      '%s@' % (self._username)]
                    else:
                        # Otherwise use realm provided in constants
                        kinit_args = [constants.ms_sql_server_kinit_path,
                                      '%s@%s' % (self._username, constants.ms_sql_server_realm)]
                    kinit = Popen(kinit_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
                    kinit.stdin.write('%s\n' % self._password)

                kinit.wait()

        engine = create_engine(con_exp, pool_size=20, max_overflow=0)

        # Uses a special flag from pyodbc fast_executemany which speeds up SQL inserts 100x including when doing df.to_sql
        # https://gitlab.com/timelord/timelord/blob/master/timelord/utils/connector.py
        # https://stackoverflow.com/questions/48006551/speeding-up-pandas-dataframe-to-sql-with-fast-executemany-of-pyodbc
        @event.listens_for(engine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            # print('Func call')
            if executemany:
                # only supported on pyodbc driver
                try:
                    cursor.fast_executemany = True
                except:
                    pass

        return engine, con_exp

    def _datetime(self):
        return sqlalchemy.dialects.mssql.DATETIME2(precision=6)


class DatabaseSourcePostgres(DatabaseSourceSQL):
    """Implements the DatabaseSourceSQL class for Postgres instances.

    """

    def __init__(self, server_host=constants.postgres_host, server_port=constants.postgres_port,
                 username=constants.postgres_username, password=constants.postgres_password,
                 trade_data_database_name=constants.postgres_trade_data_database_name):
        """Initialises SQL object.

        """

        super(DatabaseSourcePostgres, self).__init__(server_host=server_host, server_port=server_port,
                                                     username=username, password=password,
                                                     trade_data_database_name=trade_data_database_name)

    def _get_database_engine(self, database_name=None, table_name=None):

        con_exp = "postgresql://" + self._username + ":" + self._password \
                  + "@" + self._server_host + ":" + self._server_port

        if database_name is not None:
            con_exp = con_exp + "/" + database_name

            if table_name is not None:
                con_exp = con_exp + "::" + table_name

        return sqlalchemy.create_engine(con_exp), con_exp


class DatabaseSourceMySQL(DatabaseSourceSQL):
    """Implements the DatabaseSourceSQL class for MySQL instances.

    """

    def __init__(self, server_host=constants.mysql_host, server_port=constants.mysql_port,
                 username=constants.mysql_username, password=constants.mysql_password,
                 trade_data_database_name=constants.mysql_trade_data_database_name):
        """Initialises SQL object.

        """

        super(DatabaseSourceMySQL, self).__init__(server_host=server_host, server_port=server_port,
                                                  username=username, password=password,
                                                  trade_data_database_name=trade_data_database_name)

    def _get_database_engine(self, database_name=None, table_name=None):

        con_exp = "mysql+mysqldb://" + self._username + ":" + self._password \
                  + "@" + self._server_host + ":" + self._server_port

        if database_name is not None:
            con_exp = con_exp + "/" + database_name

            if table_name is not None:
                con_exp = con_exp + "::" + table_name

        return sqlalchemy.create_engine(con_exp), con_exp

    # def _sql_text_type(self):
    #     return sqlalchemy.types.NVARCHAR(length=45)


########################################################################################################################

class DatabaseSourceTickData(DatabaseSource):
    """Generic wrapper for storing tick data inside a database which is implemented using databases such as
    Arctic/MongoDB, InfluxDB and KDB.

    """

    def delete_market_data(self, ticker, start_date, finish_date, table_name=None):
        pass

    def _tidy_market_data(self, df, ticker, csv_file, market_trade_data, date_format=None, remove_duplicates=True):
        """Tidies the dataframe with market data to ensure all the columns are normalised and checks vital fields are not
        missing

        Parameters
        ----------
        df : DataFrame
            Market Data

        ticker : str
            Ticker for the market data (eg. EURUSD)

        csv_file : str
            Original path of CSV file

        market_trade_data : str ('market' or 'trade')
            Is this market or trade data?

        date_format : str
            Format for the dates for parsing

        remove_duplicates : bool
            Remove consecutive prices which are the same

        Returns
        -------
        DataFrame
        """

        logger = LoggerManager.getLogger(__name__)

        if df is not None:
            if not (df.empty):
                # Filter by a specific ticker (if the data has a ticker column) and this is market data
                # we assume that for market data tickers are stored individually
                if 'ticker' in df.columns and market_trade_data == 'market':
                    df = df[df['ticker'] == ticker]

                    if df.empty:
                        err_msg = "No data for ticker " + ticker + " in " + csv_file + \
                                  ". No data to copy to tick database! What about earlier chunks?"

                        logger.warn(err_msg)

                        # raise DataMissingException(err_msg)

                # Also accept price column
                other_mids = ['Price', 'PRICE', 'price']

                for o in other_mids:
                    df.columns = [x.replace(o, 'mid') for x in df.columns]

                # Parse date/time strings and force to UTC time
                df = self._force_utc_timezone_parse(df, date_format)

                # Force sorted by date before dumping (otherwise makes searches difficult, if incorrectly ordered)
                # also need correct sorting before we check for duplicates
                df = df.sort_index()

                # Remove consecutive duplicates of market data (even if have different timestamps)
                df = self._remove_duplicates_market_data(df, remove_duplicates)

                if 'bid' not in df.columns and 'ask' not in df.columns and 'mid' in df.columns:
                    pass
                elif 'bid' in df.columns and 'ask' in df.columns:
                    pass
                else:
                    err_msg = "No data for ticker " + ticker + " in " + csv_file + ". No data to copy to Arctic!"

                    logger.error(err_msg)

                    raise DataMissingException(err_msg)

        return df

    def _stream_chunks(self, engine, store, csv_file, ticker, table_name,
                       if_exists_ticker='replace', market_trade_data='market', date_format=None,
                       read_in_reverse=False,
                       csv_read_chunksize=constants.csv_read_chunksize, remove_duplicates=True):
        """Reads in CSV (or a series of CSV files in a path) in chunks and then dumps each chunk to the database

        Parameters
        ----------
        engine : object
            Handle for database

        store : object
            Handle for database store (not used for every database)

        csv_file : str (list)
            List of CSV files

        ticker : str
            Ticker for the dataset (for market data eg. EURUSD, for trade data eg. trade_df

        table_name : str
            Table to store data in

        database_name : str
            Database name

        if_exists_table : str (default: 'replace')
            What to do if the database table already exists
            * 'replace' - replaces the database table (default)
            * 'append' - appends to the database table

        if_exists_ticker : str (default: 'replace')
            What to do if the ticker already exists in a table
            * 'replace' - replaces the ticker data already there (default)
            * 'append' - appends to the existing ticker data

        market_trade_date : str (default: 'market')
            'market' for market data
            'trade' for trade data

        date_format : str (default: None)
            Specify the format of the dates stored in CSV, specifying this speeds up CSV parsing considerably and
            is recommended

        csv_read_chunksize : int (default: in constants file)
            Specifies the chunksize to read CSVs. If we are reading very large CSVs this helps us to reduce risk of running
            out of memory

        remove_duplicates : bool (default: True)
            Should we remove consecutive duplicated market values (eg. if EURUSD is at 1.1652 and 20ms later it is also
            recorded at 1.1652, should we ignore the second point), which will make our calculations a lot faster
            - whilst in many TCA cases, we can ignore duplicated
            points, we cannot do this for situations where we might wish to use for example volume, to calculate VWAP

        Returns
        -------

        """

        logger = LoggerManager.getLogger(__name__)

        if isinstance(csv_file, str):
            csv_file = [csv_file]

        if isinstance(ticker, str):
            ticker = [ticker]

        for i in range(0, len(csv_file)):
            ticker_postfix = ticker[i] + self.postfix

            if '*' in csv_file[i]:
                # assume alphabetically
                mini_csv_file = glob.glob(csv_file[i])
            else:
                mini_csv_file = [csv_file[i]]

            # Assume files are alphabetically sorted in chronological order (eg. AUDUSD1.csv is BEFORE AUDUSD2.csv in time)
            mini_csv_file.sort()

            # Reset for each "CSV file"/ticker
            later_chunk = False

            if len(mini_csv_file) == 0:
                logger.warn('No files to read in ' + csv_file[i])

            # If the "CSV file" has a wildcard this could be a big loop
            for m in range(0, len(mini_csv_file)):

                logger.info("Parsing " + str(
                    mini_csv_file[m]) + " before tick database dump for ticker " + ticker_postfix)

                # can't process HDF5 fixed file in chunks
                if ".h5" in mini_csv_file[m]:
                    df = UtilFunc().read_dataframe_from_binary(mini_csv_file[m])

                    self._process_chunk(df, ticker[i], mini_csv_file[m], table_name, ticker_postfix,
                                        if_exists_ticker, market_trade_data, engine, store,
                                        later_chunk=later_chunk, date_format=date_format,
                                        remove_duplicates=remove_duplicates)

                    later_chunk = True
                else:
                    chunk_no = 0

                    # Load up CSV and then dump to Arctic

                    # If the CSV is sorted in reverse (ie. latest dates at top, and earlier at bottom)
                    if read_in_reverse:

                        for df_chunk in pd.read_csv(mini_csv_file[m], index_col=0, chunksize=csv_read_chunksize):

                            logger.debug("Writing chunk " + str(chunk_no) + " dumping to disk...")

                            path = os.path.join(constants.temp_data_folder, ticker[i] + '_' + str(chunk_no) + ".h5")

                            if os.path.exists(path):
                                os.remove(path)

                            self._util_func.write_dataframe_to_binary(df_chunk, path)

                            chunk_no = chunk_no + 1

                        for ch in range(chunk_no - 1, -1, -1):
                            logger.debug("Reading back chunk " + str(ch) + " from disk and then parsing...")

                            path = os.path.join(constants.temp_data_folder, ticker[i] + '_' + str(ch) + ".h5")

                            df_chunk = self._util_func.read_dataframe_from_binary(path)

                            self._process_chunk(df_chunk, ticker[i], mini_csv_file[m], table_name, ticker_postfix,
                                                if_exists_ticker, market_trade_data, engine, store,
                                                later_chunk=later_chunk, remove_duplicates=remove_duplicates)

                            # Delete temporary file
                            os.remove(path)

                            later_chunk = True

                    # If CSV is correctly sorted (ie. earliest dates first and latest dates at the end)
                    else:
                        for df_chunk in pd.read_csv(mini_csv_file[m], index_col=0, chunksize=csv_read_chunksize):
                            logger.debug("Processing chunk " + str(chunk_no) + " into tick database...")

                            self._process_chunk(df_chunk, ticker[i], mini_csv_file[m], table_name, ticker_postfix,
                                                if_exists_ticker, market_trade_data, engine, store,
                                                later_chunk=later_chunk, remove_duplicates=remove_duplicates,
                                                date_format=date_format)

                            if chunk_no > 2:
                                break

                            chunk_no = chunk_no + 1

                            later_chunk = True

    def _process_chunk(self, df, ticker, csv_file, table_name, ticker_postfix, if_exists_ticker, market_trade_data,
                       engine, store, later_chunk=False, date_format=None, remove_duplicates=True):

        df = self._tidy_market_data(df, ticker, csv_file, market_trade_data, date_format=date_format,
                                    remove_duplicates=remove_duplicates)

        # If we have later chunks in the CSV/HDF5 file DON'T overwrite the previous section we just wrote
        if later_chunk:
            if_exists_ticker = 'append'

        if df is not None:
            if not (df.empty):
                self._write_to_db(df, engine, store, table_name, ticker_postfix, if_exists_ticker)


########################################################################################################################

from arctic import Arctic
import pymongo

from arctic.date import DateRange
from arctic.exceptions import NoDataFoundException

import arctic
import threading


class DatabaseSourceArctic(DatabaseSourceTickData):
    """Implements DatabaseSource for Arctic/MongoDB database for both trade/order data and market data. It is
    recommended to use Arctic/MongoDB to store market data. It also allows us to store market data from multiple sources,
    by using the postfix notation (eg 'ncfx' or 'dukascopy').

    """

    # static variables
    # _engine = {}
    # _store = None
    # _username = None
    # _password = None
    # _host = None
    # _port = None

    # _arctic_lib_type = None

    # _arctic_lock = threading.Lock()

    socketTimeoutMS = constants.arctic_timeout_ms

    def __init__(self, postfix=None, host=constants.arctic_host, port=constants.arctic_port,
                 username=constants.arctic_username, password=constants.arctic_password,
                 arctic_lib_type=constants.arctic_lib_type):
        """Initialise the Arctic object with our selected market data type (Arctic library type)

        Parameters
        ----------
        postfix : str
            Postfix can be used to identify different market data sources (eg. 'ncfx' or 'dukascopy'), if we only use
            one market data source, this is not necessary

        host : str
            MongoDB server hostname/IP

        port : str
            Port of MongoDB server

        arctic_lib_type : str
            'TICK_STORE' : append only style database store specifically for tick data
            'VERSION_STORE' : more flexible data store

        """
        super(DatabaseSourceArctic, self).__init__(postfix=postfix)

        # # only want to instantiate this once (or if any parameters have changed!)
        # with DatabaseSourceArctic._arctic_lock:
        #     if self._engine is None or self._store is None or arctic_lib_type != self._arctic_lib_type or \
        #         self._host != host or self._port != port or self._username != username or self._password != password:

        ssl = constants.arctic_ssl
        ssl_cert_reqs = constants.arctic_ssl_cert_reqs

        self._engine = pymongo.MongoClient(host, port=port, connect=False,
                                           username=username,
                                           password=password,
                                           ssl=ssl, ssl_cert_reqs=ssl_cert_reqs)

        self._store = \
            Arctic(self._engine, socketTimeoutMS=self.socketTimeoutMS, serverSelectionTimeoutMS=self.socketTimeoutMS,
                   connectTimeoutMS=self.socketTimeoutMS)

        self._username = username
        self._password = password
        self._host = host
        self._password = password

        self._arctic_lib_type = arctic_lib_type

    def _get_database_engine(self, table_name=None):
        """Gets the database engine for Arctic and connection to MongoDB

        Parameters
        ----------
        table_name : str
            Table name

        Returns
        -------
        MongoClient, Arctic
        """
        logger = LoggerManager.getLogger(__name__)

        logger.info('Attempting to load Arctic/MongoDB library: ' + table_name)

        # with DatabaseSourceArctic._arctic_lock:
        engine = self._engine
        store = self._store

        table = None

        try:
            table = store[table_name]
        except:
            pass

        if table is None:
            # By default will create an Arctic VERSION_STORE (can be changed in Constants)
            store.initialize_library(table_name, audit=False, lib_type=self._get_arctic_lib_type(
                self._arctic_lib_type))  # , lib_type=arctic.TICK_STORE)

            logger.info("Created Arctic/MongoDB library: " + table_name)
        else:
            logger.info("Got Arctic/MongoDB library: " + table_name)

        return engine, store

    def _get_arctic_lib_type(self, arctic_lib_type):

        if arctic_lib_type is None:
            arctic_lib_type = constants.arctic_lib_type

        if arctic_lib_type == 'VERSION_STORE':
            return arctic.VERSION_STORE
        elif arctic_lib_type == 'TICK_STORE':
            return arctic.TICK_STORE

    def fetch_market_data(self, start_date=None, finish_date=None, ticker=None, table_name=None):
        """Fetches market data for a particular ticker

        Parameters
        ----------
        start_date : str
            Start date

        finish_date : str
            Finish date

        ticker : str
            Ticker to be downloaded

        Returns
        -------
        DataFrame
        """
        logger = LoggerManager.getLogger(__name__)

        if self._arctic_lib_type == 'VERSION_STORE':
            start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)
        elif self._arctic_lib_type == 'TICK_STORE':
            start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date, utc=True)

        ticker = ticker + self.postfix

        if table_name is None:
            table_name = constants.arctic_market_data_database_table

        engine, store = self._get_database_engine(table_name=table_name)

        library = store[table_name]

        # Strip away timezones for daterange (always assume we are using UTC data)
        item = library.read(ticker, date_range=DateRange(start_date, finish_date))

        logger.debug("Extracted Arctic/MongoDB library: " + str(table_name) + " for ticker " + str(ticker) +
                     " between " + str(start_date) + " - " + str(finish_date) + " from " + self._arctic_lib_type)

        if isinstance(item, pd.DataFrame):
            df = item
        else:
            df = item.data

        # TICK_STORE may return data in local timezone so should be converted
        # VERSION_STORE will return times without timezone

        # Downsample floats to reduce memory footprint
        return self._downsample_localize_utc(df, convert=True)

    def fetch_trade_order_data(self, start_date=None, finish_date=None, ticker=None, table_name=None):
        """Fetches trade/order data for a particular ticker from Arctic/MongoDB database. We assume that trades/orders
        are stored in different tables.

        Parameters
        ----------
        start_date : str
            Start date

        finish_date : str
            Finish date

        ticker : str
            Ticker

        table_name : str
            Table name which contains trade/order data

        Returns
        -------
        DataFrame
        """
        logger = LoggerManager.getLogger(__name__)

        logger.error("Arctic is not tested for storing trade data")

        return

        # start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)
        #
        # ticker = ticker + self.postfix
        #
        # # logger.warn("Only use Arctic/MongoDB database for extracting test trade data, does not contain actual trade data")
        #
        # engine, store = self._get_database_engine(table_name=table_name)
        #
        # library = store[table_name]
        #
        # # we store the trade data as one symbol
        # item = library.read(table_name, date_range=DateRange(start_date, finish_date))
        #
        # if isinstance(item, pd.DataFrame):
        #     df = item
        # else:
        #     df = item.data
        #
        # logger.debug("Extracted Arctic/MongoDB library: " + str(table_name) + " between " +
        #              " between " + str(start_date) + " - " + str(finish_date) + " from " + self._arctic_lib_type)
        #
        # if ticker is not None:
        #     return self._downsample_localize_utc(df[df['ticker'] == ticker], convert=True)

    def convert_csv_to_table(self, csv_file, ticker, table_name, database_name=None, if_exists_table='replace',
                             if_exists_ticker='replace', market_trade_data='market', date_format=None,
                             read_in_reverse=False,
                             csv_read_chunksize=constants.csv_read_chunksize, remove_duplicates=True):
        """Reads CSV from disk (or potentionally a list of CSVs for a list of different tickers) into a pandas DataFrame
        which is then dumped in Arctic/MongoDB.

        Parameters
        ----------
        csv_file : str (list)
            Path of CSV file - can also include wildcard characters (assume that files are ordered in time, eg. if
            we specify EURUSD*.csv, then EURUSD1.csv would be before EURUSD2.csv etc.

        ticker : str
            Ticker for the dataset (for market data eg. EURUSD, for trade data eg. trade_df

        table_name : str
            Table to store data in

        database_name : str
            Database name

        if_exists_table : str (default: 'replace')
            What to do if the database table already exists
            * 'replace' - replaces the database table (default)
            * 'append' - appends to the database table

        if_exists_ticker : str (default: 'replace')
            What to do if the ticker already exists in a table
            * 'replace' - replaces the ticker data already there (default)
            * 'append' - appends to the existing ticker data

        market_trade_date : str (default: 'market')
            'market' for market data
            'trade' for trade data

        date_format : str (default: None)
            Specify the format of the dates stored in CSV, specifying this speeds up CSV parsing considerably and
            is recommended

        csv_read_chunksize : int (default: in constants file)
            Specifies the chunksize to read CSVs. If we are reading very large CSVs this helps us to reduce risk of running
            out of memory

        remove_duplicates : bool (default: True)
            Should we remove consecutive duplicated market values (eg. if EURUSD is at 1.1652 and 20ms later it is also
            recorded at 1.1652, should we ignore the second point), which will make our calculations a lot faster
            - whilst in many TCA cases, we can ignore duplicated
            points, we cannot do this for situations where we might wish to use for example volume, to calculate VWAP

        Returns
        -------

        """
        logger = LoggerManager.getLogger(__name__)

        if market_trade_data == 'trade':
            logger.error("Arctic is not tested for storing trade data")

            return

        engine, store = self._get_database_engine(table_name=table_name)

        # Create Arctic table/collection (if needs to replace)
        if if_exists_table == 'replace':
            try:
                store.delete_library(table_name)
            except:
                pass

            try:
                store.initialize_library(table_name, audit=False,
                                         lib_type=self._get_arctic_lib_type(self._arctic_lib_type))
            except:
                pass

        store.set_quota(table_name, constants.arctic_quota_market_data_GB * constants.GB)

        # Read CSV files (possibly in chunks) and then dump to tick database
        self._stream_chunks(engine, store, csv_file, ticker, table_name,
                            if_exists_ticker=if_exists_ticker, market_trade_data=market_trade_data,
                            date_format=date_format,
                            read_in_reverse=read_in_reverse,
                            csv_read_chunksize=csv_read_chunksize, remove_duplicates=remove_duplicates)

        engine.close()

    def _write_to_db(self, df, engine, store, table_name, ticker, if_exists_ticker, existing_datacheck='ignore'):
        logger = LoggerManager.getLogger(__name__)

        # Get handle for the table library to write to
        library = store[table_name]

        if df is not None:
            if not(df.empty):
                # For pandas 0.25.0 (causes issues writing pytz.utc data otherwise)
                # we assume that we are using writing to Arctic in a non-timezone aware way for VERSION_STORE
                # (we'll reintroduce the timezone for TICK_STORE which *DOES* need it)
                df = df.tz_localize(None)

                # Append data or overwrite the whole ticker
                if if_exists_ticker == 'append':

                    # If we're appending check we have no overlapping data and make sure we are writing at the end!
                    # only allow people to append to the end (unless they set the ignore_existing_datacheck parameter)

                    if existing_datacheck == 'ignore':
                        item = None
                    else:
                        if self._arctic_lib_type == 'VERSION_STORE':
                            try:
                                item = library.read(ticker, date_range=DateRange(
                                    (df.index[0] + pd.Timedelta(np.timedelta64(1, 'ms'))).replace(tzinfo=None),
                                    pd.Timestamp(datetime.datetime.utcnow()).replace(tzinfo=None)))
                            except NoDataFoundException:
                                item = None
                        elif self._arctic_lib_type == 'TICK_STORE':
                            try:
                                item = library.read(ticker, date_range=DateRange(
                                    (df.index[0] + pd.Timedelta(np.timedelta64(1, 'ms'))).replace(tzinfo=pytz.utc),
                                    pd.Timestamp(datetime.datetime.utcnow()).replace(tzinfo=pytz.utc)))
                            except NoDataFoundException:
                                item = None

                    # df.index[-1].replace(tzinfo=None)))

                    # Note: when writing to TickStore you might get a warning - "NB treating all values as 'exists' - no longer sparse
                    # However this is benign https://github.com/manahl/arctic/issues/128
                    if item is not None:
                        if not (isinstance(item, pd.DataFrame)):
                            item = item.data

                        # From TICK_STORE might return in local timezone, convert to UTC if necessary
                        temp_df = self._downsample_localize_utc(item, convert=True)

                        if temp_df is not None:
                            if not (temp_df.empty):
                                engine.close()

                                err_msg = "In Arctic/MongoDB can't append overlapping data for " + ticker + \
                                          " in " + table_name + ". Has data between " + str(
                                    df.index[0]) + ' - ' + str(df.index[-1])

                                logger.error(err_msg)

                                raise ErrorWritingOverlapDataException(err_msg)

                    if self._arctic_lib_type == 'VERSION_STORE':

                        library.append(ticker, df)
                    elif self._arctic_lib_type == 'TICK_STORE':

                        # For tick store need a timezone (causes problems with version_store)
                        if self._arctic_lib_type == 'TICK_STORE':
                            df = df.tz_localize(pytz.utc)

                        library.write(ticker, df)

                elif if_exists_ticker == 'replace':
                    library.delete(ticker)

                    # For tick store need a timezone (causes problems with version_store)
                    if self._arctic_lib_type == 'TICK_STORE':
                        df = df.tz_localize(pytz.utc)

                    library.write(ticker, df)
                else:
                    logger.info('Nothing written in ' + self._arctic_lib_type)

    def append_market_data(self, market_df, ticker, table_name=constants.arctic_market_data_database_table,
                           if_exists_table='append', if_exists_ticker='append', remove_duplicates=True,
                           existing_datacheck='yes'):
        """Append market data to Arctic/MongoDB. It is expected that market data has an index of DateTimeIndex, and fields
        such as "mid", "bid", "ask" etc. Do not write data which overlaps, otherwise it messes up the internal store.

        Parameters
        ----------
        market_df : DataFrame
            Market data to dumped

        ticker : str
            Ticker for the dataset (for market data eg. EURUSD, for trade data eg. trade_df

        table_name : str
            Table to store data in

        database_name : str
            Database name

        if_exists_table : str
            What to do if the database table already exists
            * 'replace' - replaces the database table (default)
            * 'append' - appends to the database table

        if_exists_ticker : str
            What to do if the ticker already exists in a table
            * 'replace' - replaces the ticker data already there (default)
            * 'append' - appends to the existing ticker data

        remove_duplicates : bool
            Should we remove consecutive duplicates in market data, mainly to reduce file size on disk
            * True (default) - removes duplicates (ie. every bit of market data other than time the same)
            * False - leave data as it is

        existing_datacheck : str (default: 'yes')
            If set to 'ignore', we shall throw an error if there's data already on the disk for this period

        Returns
        -------

        """
        logger = LoggerManager.getLogger(__name__)

        old_ticker = ticker
        ticker = ticker + self.postfix

        engine, store = self._get_database_engine(table_name=table_name)

        if if_exists_table == 'replace':
            try:
                store.delete_library(table_name)
            except:
                pass

        # If neccessary initialise library/table
        try:
            store.initialize_library(table_name, audit=False, lib_type=self._get_arctic_lib_type(self._arctic_lib_type))
        except:
            pass

        store.set_quota(table_name, constants.arctic_quota_market_data_GB * constants.GB)

        try:
            # Assume market data is stored in UTC (as with ALL data for tcapy)
            market_df.index = market_df.index.tz_localize(pytz.utc)
        except:
            pass

        logger.info("Now doing Arctic/MongoDB database dump for ticker " + ticker + " in table " + table_name + " for " +
                    self._arctic_lib_type)

        market_df = self._tidy_market_data(market_df, old_ticker, 'dataframe', 'market',
                                           remove_duplicates=remove_duplicates)

        self._write_to_db(market_df, engine, store, table_name, ticker, if_exists_ticker,
                          existing_datacheck=existing_datacheck)

        engine.close()

    def delete_market_data(self, ticker, start_date=None, finish_date=None, table_name=None):

        # Can't manipulate Arctic time series on disk, so we need to load it all up into memory first, edit in pandas
        # and then write back down on to disk once finished
        market_df = self.fetch_market_data(ticker=ticker, start_date='01 Jan 1999',
                                           finish_date=pd.Timestamp(datetime.datetime.utcnow()), table_name=table_name)

        # Make start/finish dates into timestamp (UTC) - all data on disk stored in UTC
        start_date = pd.Timestamp(start_date, tzinfo=pytz.utc)
        finish_date = pd.Timestamp(finish_date, tzinfo=pytz.utc)

        market_df = self._time_series_ops.remove_between_dates(market_df, start_date, finish_date)

        # Write back into the database
        self.append_market_data(market_df, ticker, table_name=table_name, if_exists_table='append',
                                if_exists_ticker='replace', remove_duplicates=False)

    def insert_market_data(self, market_df, ticker, table_name=None, remove_duplicates=True):

        # Can't manipulate Arctic time series on disk, so we need to load it all up into memory first, edit in pandas
        # and then write back down on to disk once finished (with the insertion)
        market_old_df = self.fetch_market_data(ticker=ticker, start_date='01 Jan 1999',
                                               finish_date=pd.Timestamp(datetime.datetime.utcnow()),
                                               table_name=table_name)

        # make start/finish dates into timestamp (UTC) - all data on disk stored in UTC
        start_date = pd.Timestamp(market_df.index[0], tzinfo=pytz.utc)
        finish_date = pd.Timestamp(market_df.index[-1], tzinfo=pytz.utc)

        market_old_df = self._time_series_ops.remove_between_dates(market_old_df, start_date, finish_date)

        market_df = market_old_df.append(market_df).sort_index()

        # Write back into the database
        self.append_market_data(market_df, ticker, table_name=table_name, if_exists_table='append',
                                if_exists_ticker='replace', remove_duplicates=remove_duplicates)


########################################################################################################################

from influxdb import DataFrameClient


class DatabaseSourceInfluxDB(DatabaseSourceTickData):
    """Wrapper for InfluxDB to access market data for tcapy

    Installation instructions for InfluxDB at https://docs.influxdata.com/influxdb/v1.7/introduction/installation/

    """

    # static variable
    # engine = None
    # store = None

    def __init__(self, host=constants.influxdb_host, port=constants.influxdb_port, username=constants.influxdb_username,
                 password=constants.influxdb_password, postfix=None):
        super(DatabaseSourceInfluxDB, self).__init__(postfix=postfix)

        self._host = host
        self._port = port
        self._username = username
        self._password = password

    def _get_database_engine(self, table_name=None):

        engine = DataFrameClient(self._host, self._port, self._username, self._password, table_name)

        db_stored = engine.query("show databases")

        databases = [x['name'] for x in list(db_stored.get_points(measurement='databases'))]

        if table_name not in databases:
            engine.query("create database %s" % (table_name))

        return engine, None

    def convert_csv_to_table(self, csv_file, ticker, table_name, database_name=None, if_exists_table='replace',
                             if_exists_ticker='replace', market_trade_data='market', date_format=None,
                             read_in_reverse=False,
                             csv_read_chunksize=constants.csv_read_chunksize, remove_duplicates=True):
        """Reads CSV from disk (or potentionally a list of CSVs for a list of different tickers) into a pandas DataFrame
        which is then dumped in InfluxDB

        Parameters
        ----------
        csv_file : str (list)
            Path of CSV file - can also include wildcard characters (assume that files are ordered in time, eg. if
            we specify EURUSD*.csv, then EURUSD1.csv would be before EURUSD2.csv etc.

        ticker : str
            Ticker for the dataset (for market data eg. EURUSD, for trade data eg. trade_df) - InfluxDB measurement

        table_name : str
            Table to store data in (InfluxDB database)

        database_name : str
            Database name (not used in InfluxDB)

        if_exists_table : str (default: 'replace')
            What to do if the database table already exists
            * 'replace' - replaces the database table (default)
            * 'append' - appends to the database table

        if_exists_ticker : str (default: 'replace')
            What to do if the ticker already exists in a table
            * 'replace' - replaces the ticker data already there (default)
            * 'append' - appends to the existing ticker data

        market_trade_date : str (default: 'market')
            'market' for market data
            'trade' for trade data

        date_format : str (default: None)
            Specify the format of the dates stored in CSV, specifying this speeds up CSV parsing considerably and
            is recommended

        csv_read_chunksize : int (default: in constants file)
            Specifies the chunksize to read CSVs. If we are reading very large CSVs this helps us to reduce risk of running
            out of memory

        remove_duplicates : bool (default: True)
            Should we remove consecutive duplicated market values (eg. if EURUSD is at 1.1652 and 20ms later it is also
            recorded at 1.1652, should we ignore the second point), which will make our calculations a lot faster
            - whilst in many TCA cases, we can ignore duplicated
            points, we cannot do this for situations where we might wish to use for example volume, to calculate VWAP

        Returns
        -------

        """
        engine, store = self._get_database_engine(table_name=table_name)

        # Delete InfluxDB measurement (if we specified to replace whole table ie. delete all tickers)
        # can't delete directly with 'hdel', need to empty the folder recursively
        if if_exists_table == 'replace':
            self._delete_table(engine, table_name)
            engine.query("create database %s" % (table_name))

        # read CSV/H5 files (possibly in chunks) and then dump to tick database
        self._stream_chunks(engine, store, csv_file, ticker, table_name,
                            if_exists_ticker=if_exists_ticker, market_trade_data=market_trade_data,
                            date_format=date_format,
                            read_in_reverse=read_in_reverse,
                            csv_read_chunksize=csv_read_chunksize, remove_duplicates=remove_duplicates)

    def _delete_table(self, engine, table_name):
        engine.query("drop database %s" % (table_name))

    def _write_to_db(self, df, engine, store, table_name, ticker, if_exists_ticker, existing_datacheck='yes'):
        logger = LoggerManager.getLogger(__name__)

        # Get handle for the table library to write to

        if df is not None:
            if not (df.empty):

                # Can't have "-" in KDB names
                # ticker = ticker.replace('-', '_')

                # Append data or overwrite the whole ticker
                if if_exists_ticker == 'append':

                    start_date = (df.index[0] + pd.Timedelta(np.timedelta64(1, 'ms'))).replace(tzinfo=None)

                    try:
                        engine.open()
                    except:
                        pass

                    if existing_datacheck == 'ignore':
                        temp_df = None
                    else:
                        # Is there any data already there on disk in this date range
                        try:
                            start_date_str = self._convert_influxdb_date_string(start_date)

                            # now select the Dates which could overlap
                            temp_df = engine.sync("select from %s where Date >= %s" % (ticker, start_date_str))

                        except:
                            temp_df = None

                    # If there was data in KDB during this time period, don't attempt to write
                    if temp_df is not None:
                        temp_df = self._downsample_localize_utc(temp_df)

                        if temp_df is not None:
                            if not (temp_df.empty):
                                err_msg = "tcapy doesn't allow influxdb to append overlapping data for " + ticker + " in " + table_name + ". " \
                                                                                                                                          "Has data between " + str(
                                    df.index[0]) + ' - ' + str(df.index[-1])

                                logger.error(err_msg)

                                raise ErrorWritingOverlapDataException(err_msg)

                elif if_exists_ticker == 'replace':

                    # Delete the data for a ticker
                    try:
                        engine.query("drop measurement %s"(ticker))
                    except Exception as e:
                        logger.warn("Didn't delete anything from table %s for ticker %s" % (table_name, ticker))

                chunk_size = constants.influxdb_chunksize

                chunk_no = 0

                df = self._time_series_ops.split_array_chunks(df, chunk_size=chunk_size)

                no_of_chunks = len(df)

                for start in range(0, len(df)):
                    logger.debug("Writing chunk %s of %s in %s for ticker %s..." % (
                    str(chunk_no), str(no_of_chunks), table_name, ticker))

                    engine.write_points(df[start], ticker, protocol=constants.influxdb_protocol)

                    chunk_no = chunk_no + 1

    def append_market_data(self, market_df, ticker, table_name=constants.kdb_market_data_database_table,
                           if_exists_table='append', if_exists_ticker='append', remove_duplicates=True,
                           existing_datacheck='yes'):
        """Append market data to InfluxDB It is expected that market data has an index of DateTimeIndex, and fields
        such as "mid", "bid", "ask" etc. Will throw an error if we try to write overlapping data.

        Parameters
        ----------
        market_df : DataFrame
            Market data to dumped

        ticker : str
            Ticker for the dataset (for market data eg. EURUSD, for trade data eg. trade_df (InfluxDB measurement)

        table_name : str
            Table to store data in (InfluxDB database)

        database_name : str
            Database name (not used in InfluxDB)

        if_exists_table : str
            What to do if the database table already exists
            * 'replace' - replaces the database table (default)
            * 'append' - appends to the database table

        if_exists_ticker : str
            What to do if the ticker already exists in a table
            * 'replace' - replaces the ticker data already there (default)
            * 'append' - appends to the existing ticker data

        remove_duplicates : bool (default: True)
            Should we remove consecutive duplicated market values (eg. if EURUSD is at 1.1652 and 20ms later it is also
            recorded at 1.1652, should we ignore the second point), which will make our calculations a lot faster
            - whilst in many TCA cases, we can ignore duplicated
            points, we cannot do this for situations where we might wish to use for example volume, to calculate VWAP

        existing_datacheck : str (default: 'yes')
            If set to 'ignore', we shall throw an error if there's data already on the disk for this period

        Returns
        -------

        """
        logger = LoggerManager.getLogger(__name__)

        old_ticker = ticker
        ticker = ticker + self.postfix

        engine, store = self._get_database_engine(table_name=table_name)

        if if_exists_table == 'replace':
            self._delete_table(engine, table_name)

        try:
            # Assume market data is stored in UTC (as with ALL data for tcapy)
            market_df.index = market_df.index.tz_localize(pytz.utc)
        except:
            pass

        logger.info("Now doing InfluxDB dump for ticker " + ticker + " in table " + table_name)

        # Tidy up time series/remove duplicates etc.
        market_df = self._tidy_market_data(market_df, old_ticker, 'dataframe', 'market',
                                           remove_duplicates=remove_duplicates)

        # Write to disk
        self._write_to_db(market_df, engine, store, table_name, ticker, if_exists_ticker,
                          existing_datacheck=existing_datacheck)

    def _convert_influxdb_date_string(self, date):
        if date is None:
            return ''

        return date.strftime('%Y-%m-%dT%H:%M:%S.%f')

    def fetch_market_data(self, start_date=None, finish_date=None, ticker=None, table_name=None):
        logger = LoggerManager.getLogger(__name__)

        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)

        ticker = ticker + self.postfix

        if table_name is None:
            table_name = constants.influxdb_market_data_database_table

        engine, store = self._get_database_engine(table_name=table_name)

        # Format like 2005.01.03D04:00:00
        start_date_str = self._convert_influxdb_date_string(start_date)
        finish_date_str = self._convert_influxdb_date_string(finish_date)

        # ticker = ticker.replace('-', '_')

        df = engine.query(
            "select * from %s where Date >= '%s' and Date <= '%s'" % (ticker, start_date_str, finish_date_str))

        df = df[ticker].describe()

        logger.debug("Extracted InfluxDB library: " + str(table_name) + " for ticker " + str(ticker) +
                     " between " + str(start_date) + " - " + str(finish_date))

        # downsample floats to reduce memory footprint
        return self._downsample_localize_utc(df)

    def fetch_trade_order_data(self, start_date=None, finish_date=None, ticker=None):
        raise Exception("InfluxDB wrapper not implemented")

    def delete_market_data(self, ticker, start_date=None, finish_date=None, table_name=None):
        logger = LoggerManager.getLogger(__name__)

        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)

        ticker = ticker + self.postfix

        if table_name is None:
            table_name = constants.influxdb_market_data_database_table

        engine, store = self._get_database_engine(table_name=table_name)

        # Can't have "-" in KDB names
        # ticker = ticker.replace('-', '_')

        # format like 2005.01.03T04:00:00
        start_date_str = self._convert_influxdb_date_string(start_date)
        finish_date_str = self._convert_influxdb_date_string(finish_date)

        try:
            engine.query(
                "delete from %s where Date >= '%s' and Date <= '%s'" % (ticker, start_date_str, finish_date_str))
        except:
            logger.warn("Error deleting data between " + start_date_str + " " + finish_date_str)


########################################################################################################################

import qpython.qconnection as qconnection
from qpython import MetaData as MetaDataQ
from qpython.qtype import QKEYED_TABLE, QDATETIME_LIST


class DatabaseSourceKDB(DatabaseSourceTickData):
    """Wrapper for KDB access for tick data. Note, that minimal computations are done inside KDB, this mostly reads/writes
    tick data with KDB. Assumes that we store data on disk for each ticker in a single file under the folder for table_name,
    ie. table_name/ticker. Uses qPython for interacting with KDB database and return_cache_handles are the conversions back and forth
    between pandas data types and KDB data types.

    The newer PyQ library is also available for interacting with KDB, but is trickier to install and is not supported
    yet by tcapy.

    KDB supports many different schemes for data storage, including splaying columns in different files, or for very
    high frequency data, we can store the data in different files for each day. If you choose a more complicated disk storage
    method for your tick data, you will likely need to edit some of the KDB code).

    Install KDB using instructions from https://code.kx.com/q/tutorials/install/linux/

    """

    # static variable
    # engine = None
    # store = None

    def __init__(self, host=constants.kdb_host, port=constants.kdb_port, username=constants.kdb_username,
                 password=constants.kdb_password, postfix=None):
        super(DatabaseSourceKDB, self).__init__(postfix=postfix)

        self._host = host
        self._port = port
        self._username = username
        self._password = password

    def _get_database_engine(self):

        # Note table_name is not needed to make a connection with KDB
        engine = qconnection.QConnection(host=self._host, port=self._port,
                                         username=self._username, password=self._password, pandas=True)

        return engine, None

    def convert_csv_to_table(self, csv_file, ticker, table_name, database_name=None, if_exists_table='replace',
                             if_exists_ticker='replace', market_trade_data='market', date_format=None,
                             read_in_reverse=False,
                             csv_read_chunksize=constants.csv_read_chunksize, remove_duplicates=True):
        """Reads CSV from disk (or potentionally a list of CSVs for a list of different tickers) into a pandas DataFrame
        which is then dumped in KDB

        Parameters
        ----------
        csv_file : str (list)
            Path of CSV file - can also include wildcard characters (assume that files are ordered in time, eg. if
            we specify EURUSD*.csv, then EURUSD1.csv would be before EURUSD2.csv etc.

        ticker : str
            Ticker for the dataset (for market data eg. EURUSD, for trade data eg. trade_df

        table_name : str
            Table to store data in

        database_name : str
            Database name (not used in KDB)

        if_exists_table : str (default: 'replace')
            What to do if the database table already exists
            * 'replace' - replaces the database table (default)
            * 'append' - appends to the database table

        if_exists_ticker : str (default: 'replace')
            What to do if the ticker already exists in a table
            * 'replace' - replaces the ticker data already there (default)
            * 'append' - appends to the existing ticker data

        market_trade_date : str (default: 'market')
            'market' for market data
            'trade' for trade data

        date_format : str (default: None)
            Specify the format of the dates stored in CSV, specifying this speeds up CSV parsing considerably and
            is recommended

        csv_read_chunksize : int (default: in constants file)
            Specifies the chunksize to read CSVs. If we are reading very large CSVs this helps us to reduce risk of running
            out of memory

        remove_duplicates : bool (default: True)
            Should we remove consecutive duplicated market values (eg. if EURUSD is at 1.1652 and 20ms later it is also
            recorded at 1.1652, should we ignore the second point), which will make our calculations a lot faster
            - whilst in many TCA cases, we can ignore duplicated
            points, we cannot do this for situations where we might wish to use for example volume, to calculate VWAP

        Returns
        -------

        """
        engine, store = self._get_database_engine()

        # Delete KDB directory/collection (if we specified to replace whole table ie. delete all tickers)
        # can't delete directly with 'hdel', need to empty the folder recursively
        if if_exists_table == 'replace':
            engine.open()
            self._delete_table(engine, table_name)

        # read CSV/H5 files (possibly in chunks) and then dump to tick database
        self._stream_chunks(engine, store, csv_file, ticker, table_name,
                            if_exists_ticker=if_exists_ticker, market_trade_data=market_trade_data,
                            date_format=date_format,
                            read_in_reverse=read_in_reverse,
                            csv_read_chunksize=csv_read_chunksize, remove_duplicates=remove_duplicates)

        engine.close()

    def _delete_table(self, engine, table_name):
        engine.sync("diR:{$[11h=type d:key x;raze x,.z.s each` sv/:x,/:d;d]}")
        engine.sync("nuke:hdel each desc diR@")
        engine.sync("nuke`:%s" % (table_name))

    def _write_to_db(self, df, engine, store, table_name, ticker, if_exists_ticker, existing_datacheck='yes'):
        logger = LoggerManager.getLogger(__name__)

        # Get handle for the table library to write to

        if df is not None:
            if not (df.empty):

                # Can't have "-" in KDB names
                ticker = ticker.replace('-', '_')

                # Append data or overwrite the whole ticker
                if if_exists_ticker == 'append':

                    start_date = (df.index[0] + pd.Timedelta(np.timedelta64(1, 'ms'))).replace(tzinfo=None)

                    try:
                        engine.open()
                    except:
                        pass

                    if existing_datacheck == 'ignore':
                        temp_df = None
                    else:
                        # Is there any data already there on disk in this date range
                        try:
                            start_date_str = self._convert_kdb_date_string(start_date)

                            # Load table from disk
                            engine.sync('%s: get `:%s/%s' % (ticker, table_name, ticker))

                            # Now select the Dates which could overlapp
                            temp_df = engine.sync("select from %s where Date >= %s" % (ticker, start_date_str))

                        except:
                            temp_df = None

                    # If there was data in KDB during this time period, don't attempt to write
                    if temp_df is not None:
                        temp_df = self._downsample_localize_utc(temp_df)

                        if temp_df is not None:
                            if not (temp_df.empty):
                                err_msg = "tcapy doesn't allow KDB to append overlapping data for " + ticker + " in " + table_name + ". " \
                                                                                                                                     "Has data between " + str(
                                    df.index[0]) + ' - ' + str(df.index[-1])

                                logger.error(err_msg)

                                raise ErrorWritingOverlapDataException(err_msg)

                    # Otherwise, create a new temporary table with the data to append
                    df.index = np.array(df.index, dtype=np.datetime64)
                    df.index.name = 'Date'

                    df.meta = MetaDataQ(Date=QDATETIME_LIST, qtype=QKEYED_TABLE)
                    engine('set', np.string_(ticker + '_temp'), df)

                    # Do a join of the new data with existing ticker data
                    engine.sync('`%s upsert %s' % (ticker, ticker + '_temp'))

                    # Store the combined ticker to a given path of folder
                    engine.sync('`:%s/%s set %s' % (table_name, ticker, ticker))

                elif if_exists_ticker == 'replace':
                    engine.open()

                    # Delete the data for a ticker
                    try:
                        # Load ticker from disk and delete
                        engine.sync('%s: get `:%s/%s' % (ticker, table_name, ticker))
                        engine.sync("hdel `%s" % (ticker))
                    except Exception as e:
                        logger.warn("Didn't delete anything from table %s for ticker %s" % (table_name, ticker))

                    df.index = np.array(df.index, dtype=np.datetime64)
                    df.index.name = 'Date'

                    df.meta = MetaDataQ(Date=QDATETIME_LIST, qtype=QKEYED_TABLE)

                    engine('set', np.string_(ticker), df)

                    # Persist deleted changes to disk
                    engine.sync('`:%s/%s set %s' % (table_name, ticker, ticker))
                else:
                    logger.info('Nothing written')

    def append_market_data(self, market_df, ticker, table_name=constants.kdb_market_data_database_table,
                           if_exists_table='append', if_exists_ticker='append', remove_duplicates=True,
                           existing_datacheck='yes'):
        """Append market data to KDB It is expected that market data has an index of DateTimeIndex, and fields
        such as "mid", "bid", "ask" etc. Will throw an error if we try to write overlapping data.

        Parameters
        ----------
        market_df : DataFrame
            Market data to dumped

        ticker : str
            Ticker for the dataset (for market data eg. EURUSD, for trade data eg. trade_df

        table_name : str
            Table to store data in

        database_name : str
            Database name

        if_exists_table : str
            What to do if the database table already exists
            * 'replace' - replaces the database table (default)
            * 'append' - appends to the database table

        if_exists_ticker : str
            What to do if the ticker already exists in a table
            * 'replace' - replaces the ticker data already there (default)
            * 'append' - appends to the existing ticker data

        remove_duplicates : bool (default: True)
            Should we remove consecutive duplicated market values (eg. if EURUSD is at 1.1652 and 20ms later it is also
            recorded at 1.1652, should we ignore the second point), which will make our calculations a lot faster
            - whilst in many TCA cases, we can ignore duplicated
            points, we cannot do this for situations where we might wish to use for example volume, to calculate VWAP

        existing_datacheck : str (default: 'yes')
            If set to 'ignore', we shall throw an error if there's data already on the disk for this period

        Returns
        -------

        """
        logger = LoggerManager.getLogger(__name__)

        old_ticker = ticker
        ticker = ticker + self.postfix

        engine, store = self._get_database_engine()

        if if_exists_table == 'replace':
            engine.open()
            self._delete_table(engine, table_name)

        try:
            # Assume market data is stored in UTC (as with ALL data for tcapy)
            market_df.index = market_df.index.tz_localize(pytz.utc)
        except:
            pass

        logger.info("Now doing KDB dump for ticker " + ticker + " in table " + table_name)

        # Tidy up time series/remove duplicates etc.
        market_df = self._tidy_market_data(market_df, old_ticker, 'dataframe', 'market',
                                           remove_duplicates=remove_duplicates)

        # Write to disk
        self._write_to_db(market_df, engine, store, table_name, ticker, if_exists_ticker,
                          existing_datacheck=existing_datacheck)

        engine.close()

    def _convert_kdb_date_string(self, date):
        if date is None:
            return ''

        return date.strftime('%Y.%m.%dD%H:%M:%S.%f')

    def fetch_market_data(self, start_date=None, finish_date=None, ticker=None,
                          table_name=constants.kdb_market_data_database_table):
        logger = LoggerManager.getLogger(__name__)

        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)

        ticker = ticker + self.postfix

        if table_name is None:
            table_name = constants.kdb_market_data_database_table

        engine, store = self._get_database_engine()

        try:
            engine.open()
        except:
            pass

        # Format like 2005.01.03D04:00:00
        start_date_str = self._convert_kdb_date_string(start_date)
        finish_date_str = self._convert_kdb_date_string(finish_date)

        ticker = ticker.replace('-', '_')

        engine.sync('%s: get `:%s/%s' % (ticker, table_name, ticker))
        df = engine.sync("select from %s where Date within %s %s" % (
        ticker, start_date_str, finish_date_str))  # , str(start_date), (finish_date)))  where Date >= %s and Date <= %s

        try:
            engine.close()
        except:
            pass

        logger.debug("Extracted KDB library: " + str(table_name) + " for ticker " + str(ticker) +
                     " between " + str(start_date) + " - " + str(finish_date))

        # Downsample floats to reduce memory footprint
        return self._downsample_localize_utc(df)

    def fetch_trade_order_data(self, start_date=None, finish_date=None, ticker=None):
        raise Exception("KDB wrapper not implemented")

    def delete_market_data(self, ticker, start_date=None, finish_date=None, table_name=None):
        logger = LoggerManager.getLogger(__name__)

        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)

        ticker = ticker + self.postfix

        if table_name is None:
            table_name = constants.kdb_market_data_database_table

        engine, store = self._get_database_engine()

        # Can't have "-" in KDB names, so replace with '_' character
        ticker = ticker.replace('-', '_')

        try:
            engine.open()
        except:
            pass

        # Format like 2005.01.03D04:00:00
        start_date_str = self._convert_kdb_date_string(start_date)
        finish_date_str = self._convert_kdb_date_string(finish_date)

        try:
            # Get the data for the ticker
            engine.sync('%s: get `:%s/%s' % (ticker, table_name, ticker))

            # Delete the section between the Dates
            engine.sync("delete from `%s where Date within %s %s" % (ticker, start_date_str, finish_date_str))

            # Persist deleted changes to disk
            engine.sync('`:%s/%s set %s' % (table_name, ticker, ticker))
        except:
            logger.warn("Error deleting data between " + start_date_str + " " + finish_date_str)

        engine.close()


########################################################################################################################

class DatabaseSourceDataFrame(DatabaseSource):
    """Implements DatabaseSource for Pandas DataFrames, both for market and trade/order data.

    """

    def __init__(self, market_df=None, trade_df=None):

        super(DatabaseSourceDataFrame, self).__init__(postfix=None)

        self.market_df = market_df
        self.trade_df = trade_df

    def fetch_market_data(self, start_date=None, finish_date=None, ticker=None, table_name=None, date_format=None):
        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date, utc=True)

        if table_name is None:
            table_name = self.market_df

        return self._filter_dataframe(start_date, finish_date, ticker,
                                      self._fetch_table(table_name, date_format=date_format))

    def fetch_trade_order_data(self, start_date=None, finish_date=None, ticker=None, table_name=None, date_format=None):
        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date, utc=True)

        if table_name is None:
            table_name = self.trade_df

        return self._filter_dataframe(start_date, finish_date, ticker,
                                      self._fetch_table(table_name, date_format=date_format))

    def _fetch_table(self, df, date_format=None):

        df.index = pd.to_datetime(df.index, format=date_format)

        for c in constants.date_columns:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], format=date_format)

        return df

    def _filter_dataframe(self, start_date, finish_date, ticker, df):
        # Convert start/finish dates have the same timezone as the DataFrame (otherwise can cause problems when filtering)
        start_date, finish_date = self.mirror_data_timezone(df, start_date, finish_date)

        # Filter the DataFrame by start/finish dates and the ticker
        if start_date != None:
            try:
                df = df[df.index >= start_date]
            except:
                return None

        if finish_date != None:
            try:
                df = df[df.index <= finish_date]
            except:
                return None

        if ticker != None:
            try:
                df = df[df['ticker'] == ticker]
            except:
                return None

        return self._downsample_localize_utc(df)

########################################################################################################################
# External data sources
########################################################################################################################

from datetime import timedelta
import time

import pytz
import requests

try:
    import urllib2 as urllib_gen
except:
    import urllib.request as urllib_gen

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

class DatabaseSourceNCFX(DatabaseSource):
    """Implements DatabaseSource for calling New Change FX's external server, via their Rest/API. It is recommended to use
    this to download data from NCFX and then cache data locally in an Arctic/MongoDB database for reuse. Repeated calling
    of this unnecessarily will cause latency issues.

    """

    def __init__(self):
        super(DatabaseSourceNCFX, self).__init__()

        self._util_func = UtilFunc()

    def fetch_market_data(self, start_date=None, finish_date=None, ticker=None, web_proxies=constants.web_proxies, table_name=None):
        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)

        logger = LoggerManager.getLogger(__name__)

        # Only allow at most related to the minute chunk size
        timed = finish_date - start_date

        logger.debug("Downloading " + str(start_date) + " - " + str(finish_date) + " for " + str(ticker))

        if timed > timedelta(minutes=constants.ncfx_chunk_min_size):
            logger.error("Cannot download data from NCFX for more than " + str(constants.ncfx_chunk_min_size) + " minutes.")

            return None

        start_date = self._convert_date_to_string(start_date)
        finish_date = self._convert_date_to_string(finish_date)

        url = constants.ncfx_url + start_date + ";" + finish_date + ";" + ticker + ";,CSV," \
              + constants.ncfx_username + "," + constants.ncfx_password

        # Much faster to parse date/time by position in this way (if somewhat messy!) than using strptime
        # constructor year, month, day, hour, minutes, seconds, microseconds
        # need to multiply by 1000, because one of the arguments is microseconds
        dateparse = lambda x: datetime.datetime(int(x[6:10]), int(x[3:5]), int(x[0:2]),
                                                int(x[11:13]), int(x[14:16]), int(x[17:19]), int(x[20:23]) * 1000)

        # download via http from NCFX
        df = self._download(url, dateparse, web_proxies)

        if df is None:
            logger.warn("Failed to download from NCFX for " + ticker + " from " + str(start_date) + " - " +
                        str(finish_date) + ". Check if network problem?")

            return None
        elif df.empty:
            logger.warn("Download from NCFX for " + ticker + " from " + str(start_date) + " - " +
                        str(finish_date) + " is empty. Data doesn't exist for that time.")

            return None

        df.index.name = "Date"
        df.drop(['PriceType'], axis=1, inplace=True)
        df = df.rename(columns={'Price': 'mid', 'CurrencyPair': 'ticker'})
        df['ticker'] = ticker

        df.index = df.index.tz_localize(pytz.utc)

        return df

    def fetch_trade_order_data(self, start_date=None, finish_date=None, ticker=None):
        raise Exception("NCFX does not provide trade data")

    def _convert_date_to_string(self, date):

        return date.strftime("%d%m%Y%H%M%S")

    def append_market_data(self, market_df, postfix=None):
        raise Exception("You cannot upload market data to NCFX")

    def _download(self, url, dateparse, web_proxies):
        logger = LoggerManager.getLogger(__name__)

        def download(url):

            https_proxy = web_proxies['https']

            # return pd.read_csv(url, index_col='DateTimeStamp', date_parser=dateparse)

            if https_proxy is None:
                response = requests.get(url)

                status_code = response.status_code
                s = response.text
            else:
                # Note requests has issues with HTTPS proxy, so need to use urllib2 (or urllib.requests in Python 3)

                # response = requests.get(url, proxies={"https": https_proxy}).text
                # status_code = response.status_code
                # s = response.text

                # use urllib2 library
                opener = urllib_gen.build_opener(
                    urllib_gen.HTTPHandler(),
                    urllib_gen.HTTPSHandler(),
                    urllib_gen.ProxyHandler({'https': https_proxy})
                )

                urllib_gen.install_opener(opener)

                response = opener.open(url)
                status_code = response.get_code()

                s = response.read()

            if status_code == 200:
                raw_string = StringIO(s)

                if 'DateTimeStamp' in s:
                    df = pd.read_csv(raw_string, index_col='DateTimeStamp', date_parser=dateparse)

                    # for pandas 0.25.0
                    df = df.tz_localize(None)

                    return df
                else:
                    logger.warn("Problem downloading data from NCFX: " + str(s))

            return None

            # create a thread to download data - this way we can control the timeout better

        swim = Swim(parallel_library=constants.database_source_threading_library)

        df = None

        for i in range(0, constants.ncfx_retry_times):
            pool = swim.create_pool(thread_no=1)

            # open the market data downloads in their own threads and return the results
            result = pool.map_async(download, (url,))

            try:
                df = result.get(timeout=constants.ncfx_sleep_seconds)

                break
            except Exception as e:
                logger.warn("Retrying... for " + str(i) + " time " + str(e.message) + " from URL " + url)

                time.sleep(constants.ncfx_sleep_seconds)

        if df is None:
            pass
        elif df != []:
            df = df[0]

        return df

########################################################################################################################

from findatapy.market import Market, MarketDataGenerator, MarketDataRequest

class DatabaseSourceExternalDownloader(DatabaseSource):
    """Implements DatabaseSource for calling an external source. It is recommended to use this to download data from external source
    and then cache data locally in an Arctic/MongoDB database (or similar) for reuse. Repeated calling
    of this unnecessarily will cause latency issues.

    """

    def __init__(self):
        super(DatabaseSourceExternalDownloader, self).__init__()

    def fetch_market_data(self, start_date=None, finish_date=None, ticker=None, web_proxies=None, table_name=None):
        start_date, finish_date = self._parse_start_finish_dates(start_date, finish_date)

        logger = LoggerManager.getLogger(__name__)

        timed = finish_date - start_date

        if self._max_chunk_size() is not None:
            if timed > self._max_chunk_size():
                logger.error(
                    "Cannot download data from " + self._data_provider() + " for more than " +
                    str(self._max_chunk_size()) + " minutes.")

                return None

        logger.debug("Downloading " + str(start_date) + " - " + str(finish_date) + " for " + str(ticker))

        df = self._download(start_date, finish_date, ticker)

        if df is None:
            if start_date.dayofweek != 5 and finish_date.dayofweek != 6:
                logger.warn("Failed to download from " + self._data_provider() + " for " + ticker + " from " + str(start_date) + " - " +
                            str(finish_date) + ". Check if network problem?")
            else:
                logger.info("Didn't get data from " + self._data_provider() + " for " + ticker + " from " + str(start_date) + " - " +
                        str(finish_date) + ", likely because weekend")

            return None

        elif df.empty:
            logger.warn("Download from" + self._data_provider() + " for " + ticker + " from " + str(start_date) + " - " +
                        str(finish_date) + " is empty. Data doesn't exist for that time.")

            return None

        df.index.name = "Date"
        df['ticker'] = ticker

        df.index = df.index.tz_localize(pytz.utc)

        return df

    def fetch_trade_order_data(self, start_date=None, finish_date=None, ticker=None):
        raise Exception(str(self._data_provider) + " does not provide trade data")

    def append_market_data(self, market_df, postfix=None):
        raise Exception("You cannot upload market data to " + str(self._data_provider()))

    def _data_provider(self):
        return None

    def _download(self, start_date, finish_date, ticker):
        return None

    def _max_chunk_size(self):
        return None

class DatabaseSourceDukascopy(DatabaseSourceExternalDownloader):
    """Implements DatabaseSource for calling Dukascopy. It is recommend edto use this to download data from external source
    and then cache data locally in an Arctic/MongoDB database (or similar) for reuse. Repeated calling
    of this unnecessarily will cause latency issues.

    """

    def __init__(self):
        super(DatabaseSourceDukascopy, self).__init__()

    def _data_provider(self):
        return 'dukascopy'

    def _download(self, start_date, finish_date, ticker):
        # Use findatapy to download (supports several providers including Dukascopy)
        market = Market(market_data_generator=MarketDataGenerator())

        md_request = MarketDataRequest(start_date=start_date, finish_date=finish_date, data_source=self._data_provider(),
                                       category='fx',
                                       freq='tick',
                                       tickers=ticker, vendor_tickers=constants.dukascopy_tickers[ticker],
                                       fields=['bid', 'ask'], vendor_fields=['bid', 'ask'])

        df = market.fetch_market(md_request)

        if df is not None:
            # tcapy has different column format
            df.columns = [x.replace(ticker + ".", '') for x in df.columns]

            # add mid point, which is often used in TCA
            df['mid'] = (df['bid'].values + df['ask'].values) / 2.0

        return df
