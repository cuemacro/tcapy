from __future__ import unicode_literals

__author__ = 'saeedamen' # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta
import pytz

import pandas as pd
from pandas.tseries.offsets import *

import re
import copy
import os
import glob

from collections import OrderedDict

import pickle as pkl
import json

from tcapy.util.loggermanager import LoggerManager
from tcapy.conf.constants import Constants

constants = Constants()

pkl.DEFAULT_PROTOCOL = 2    # for backward compatability with Python 2
pkl.HIGHEST_PROTOCOL = 2    # for backward compatability with Python 2

from plotly.utils import PlotlyJSONEncoder

class UtilFunc(object):
    """Contains utility methods which are commonly used throughout the project to

    - manipulate lists of strings
    - parse dates
    - split dates into lists
    - read/write DataFrames to disk in HDF5 format

    """

    ###### helper functions for lists

    def remove_list_duplicates(self, lst):
        return list(OrderedDict.fromkeys(lst))

    def flatten_list_of_lists(self, list_of_lists):
        """Flattens lists of obj, into a single list of strings (rather than characters, which is default behavior).

        Parameters
        ----------
        list_of_lists : obj (list)
            List to be flattened

        Returns
        -------
        str (list)
        """

        if isinstance(list_of_lists, list):
            rt = []
            for i in list_of_lists:
                if isinstance(i, list):
                    rt.extend(self.flatten_list_of_lists(i))
                else:
                    rt.append(i)

            return rt

        return list_of_lists

        # import itertools
        # return list(itertools.chain.from_iterable(itertools.repeat(x, 1) if isinstance(x, str) else x for x in list_of_lists))

    def pretty_str_list(self, list_str, binder=' & '):
        """Makes a string list into pretty human readable list separated by commas and the last element is separated
        by an "and"

        Parameters
        ----------
        list_str : list(str)
            List of strings

        Returns
        -------
        str
        """

        if isinstance(list_str, str):
            return list_str

        # remove duplicated str in the list
        list_str = self.remove_duplicated_str(list_str)

        str_pretty = list_str[0]

        if len(list_str) == 1:
            return str_pretty

        if len(list_str) == 2:
            return str_pretty + binder  + list_str[1]

        for i in range(1, len(list_str) - 1):
            str_pretty = str_pretty + ', ' + list_str[i]

        return str_pretty + binder + list_str[-1]

    def pretty_str_within_list(self, list_str):
        """Goes through a str list and makes it pretty removing things like underscores to make it more readable

        Parameters
        ----------
        list_str : str(list)
            List to be prettified

        Returns
        -------
        str(list)
        """

        list_str = list_str.copy()
        list_str = [ele.replace('_', ' ') for ele in list_str]

        return list_str

    def pretty_columns(self, df):

        df.columns = self.pretty_str_within_list(df.columns)

        return df


    def remove_empty_str_list(self, list_str):
        """Removes the empty elements from a str list

        Parameters
        ----------
        list_str : str (list)
            List to be filtered

        Returns
        -------
        str (list)
        """

        lst = []

        for k in list_str:
            if k is not None:
                lst.append(k)

        return lst

    def remove_duplicated_str(self, list_str):
        """Removes duplicated strings in a list, returning a list with unique elements, whilst preserving the order.

        Parameters
        ----------
        list_str : str (list)
            List to be examined

        Returns
        -------
        str (list)
        """
        result = []

        if isinstance(list_str, str):
            return list_str

        for item in list_str:
            if item not in result:
                result.append(item)

        return result

    def populate_field(self, field_val, available_dictionary, exception_fields=[]):
        """Expands a specified field (eg. 'All' becomes a list of all the tickers we have)
        according to a pre-defined dictionary.

        Parameters
        ----------
        field_val : str (array)
            Current value of field

        available_dictionary : dict
            Dictionary of values for that field

        exception_fields : str (list)
            Fields not to expand

        Returns
        -------
        str (array)
        """

        if not(isinstance(field_val, list)):
            field_val = [field_val]

        if not(isinstance(exception_fields, list)):
            exception_fields = [exception_fields]

        field_val_list = []

        # expand out ticker list
        for fld in field_val:

            if fld not in exception_fields:
                if fld in self.dict_key_list(available_dictionary.keys()):
                    fld = available_dictionary[fld]

            field_val_list.append([fld])

        # flatten ticker list and remove any duplicates
        field_val = self.flatten_list_of_lists(field_val_list)
        field_val = self.remove_duplicated_str(field_val)

        return field_val

    def remove_keymatch_dict(self, dictionary, keymatch):
        for k in self.dict_key_list(dictionary.keys()):
            if keymatch in k:
                dictionary.pop(k, None)

        return dictionary

    def read_dataframe_from_binary(self, fname, format=constants.binary_default_dump_format):
        """Reads a DataFrame which is in HDF5/Parquet file format which was previously written by tcapy

        Parameters
        ----------
        fname : str
            Path of binary file

        format : str (default: 'hdf5')
            What is the binary format? ('hdf5' and 'parquet' are supported)

        Returns
        -------
        pd.DataFrame
        """
        logger = LoggerManager.getLogger(__name__)

        # parquet is not fully implemented at this stage in tcapy
        if format == 'parquet':
            data_frame = None

            try:
                if not (os.path.exists(fname)):
                    logger.error("Path doesn't exist for " + fname)

                    return data_frame

                return pd.read_parquet(fname, engine=constants.parquet_engine)
            except Exception as e:
                logger.error("No valid data for " + fname + ': ' + str(e))

                return data_frame

        elif format == 'hdf5':
            data_frame = None
            store = None

            try:
                if not (os.path.exists(fname)):
                    logger.error("Path doesn't exist for " + fname)

                    return data_frame

                store = pd.HDFStore(fname)
                data_frame = store.select("data")
            except Exception as e:
                logger.error("No valid data for " + fname + ': ' + str(e))

                return data_frame

            finally:
                try:
                    if store is not None:
                        store.close()
                except:
                    pass

            return data_frame
        else:
            logger.warning("Cannot read file " + fname + ", invalid format specified")

            return None


    def write_dataframe_to_binary(self, data_frame, fname, format=constants.binary_default_dump_format):
        """Writes a DataFrame to disk in Parquet, HDF5, CSV or CSV.GZ format

        Parameters
        ----------
        data_frame : DateFrame
            Time series to be written

        fname : str
            Path of file to be written

        format : str (default: 'parquet')
            What is the format? ('parquet', 'hdf5', 'csv' or 'csv.gz' are supported)

        Returns
        -------

        """

        # Try to delete the old copy
        try:
            os.remove(fname)
        except:
            pass

        # Parquet is preferred
        if format == 'parquet':
            if data_frame is not None:
                if not (data_frame.empty):
                    data_frame.to_parquet(fname, compression=constants.parquet_compression, engine=constants.parquet_engine)

        elif format == 'hdf5':
            if data_frame is not None:
                if not(data_frame.empty):
                    store = pd.HDFStore(fname)

                    store.put(key='data', value=data_frame, format='fixed')
                    store.close()

        elif format == 'csv':
            if data_frame is not None:
                if not (data_frame.empty):
                    data_frame.to_csv(fname)

        elif format == 'csv.gz':
            if data_frame is not None:
                if not (data_frame.empty):
                    data_frame.to_csv(fname, compression='gzip')

    ####################################################################################################################

    # helper methods for compatibility with Python 3 (Python 2) when dealing with filtering lists and dealing with 'keys'
    # of a dictionary (given Python 3, returns an iterator for dictionary keys, whereas Python 2 doesn't)
    def remove_none_list(self, filt):

        if isinstance(filt, list):
            lst = []

            for f in filt:
                if f is not None:
                    lst.append(f)

            return lst

        return filt

    def filter_list(self, filt):

        if isinstance(filt, list):
            return filt

        lst = []

        for f in filt:
            lst.append(f)

        return lst

    def dict_key_list(self, keys):

        # keys = di.keys()

        if isinstance(keys, list):
            return keys

        lst = []

        for k in keys:
            lst.append(k)

        return lst

    ####################################################################################################################

    def find_sub_string_between(self, actual_str, start, end):
        return re.search('%s(.*)%s' % (start, end), actual_str).group(1)

    def keep_numbers_list(self, lst):
        if not(isinstance(lst, list)):
            lst = [lst]

        def keep_numbers_aux(obj):
            return int(re.sub('[^0-9]', '', obj))

        lst = [keep_numbers_aux(k) for k in lst]

        return lst


    ####################################################################################################################

    def forcibly_create_empty_folder(self, path):
        """For a given path create a folder (and if it already exists, delete any files contained in it)

        Parameters
        ----------
        path : str
            File path

        Returns
        -------

        """
        # prepare the CSV folder first
        try:
            os.makedirs(path)
        except:
            pass

        # clear folder
        try:
            files = glob.glob(path + "/*")

            for f in files:
                os.remove(f)
        except:
            pass

    ###### helpful functions for date times
    def parse_datetime(self, datetime_str):
        """Parses date/time strings into Python datetime objects. Supports date/time strings with milliseconds, seconds and
        minute fields.

        Parameters
        ----------
        datetime_str : str
            Date/time to be parsed

        Returns
        -------
        datetime
        """

        dates = ['%Y-%m-%d', '%d %b %Y', '%d %b %y', '%d %B %Y', '%d %B %y']
        times = ['%H:%M:%S.%f', '%H:%M:%S', '%H:%M', None]

        if not(isinstance(datetime_str, str)):
            return datetime_str

        for d in dates:
            for t in times:

                if t is None:
                    parse_format = d
                else:
                    parse_format = d + ' ' + t

                try:
                    return datetime.datetime.strptime(datetime_str, parse_format)
                except:
                    pass

        return 'Unable to parse date'

    def replace_datetime_zone_to_utc(self, date):

        try:
            date = date.replace(tzinfo=pytz.utc)
        except:
            pass

        return date

    def period_bounds(self, date, period='month'):
        """Gets the very first and very last ticks of the month/week/day period for a particular date

        Parameters
        ----------
        date : pd.Timestamp
            Date to be used

        period : str
            'month' (default)
            'week'
            'day'

        Returns
        -------
        datetime, datetime
        """

        if not(isinstance(date, pd.Timestamp)):
            date = pd.Timestamp(self.parse_datetime(date))

        return self._start_period(date, period=period), self._last_tick_current_period(date, period=period)

    def _start_period(self, date, period='month'):
        """For a particular date, return the date/time on the very first day at time 0

        Parameters
        ----------
        date : pd.Timestamp
            Date to be used

        period : str
            'month' (default)
            'week'
            'day'

        Returns
        -------
        datetime
        """

        if period == 'month':
            date = copy.deepcopy(date)
            date = date.replace(day=1)

        elif period == 'week':
            date = copy.deepcopy(date)
            date = date - relativedelta(days=date.dayofweek)

        elif period == 'day':
            date = copy.deepcopy(date)

        date = date.replace(hour=0)
        date = date.replace(minute=0)
        date = date.replace(second=0)
        date = date.replace(microsecond=0)

        return date

    def _last_tick_current_period(self, date, period='month'):
        """Gets the very last tick of the current month/week period, ie. on the very last day of the month/week at 23:59.999999

        Parameters
        ----------
        date : pd.Timestamp
            Date to be used

        period : str
            Period defined
            "month" - define monthly period
            "week" - define weekly period
            "day" - define daily period

        Returns
        -------
        datetime
        """

        next_start_period = self._next_start_period(date, period=period)

        # Reset the time to 00:00:00
        next_start_period = next_start_period.replace(hour=0)
        next_start_period = next_start_period.replace(minute=0)
        next_start_period = next_start_period.replace(second=0)
        next_start_period = next_start_period.replace(microsecond=0)

        # Go back a microsecond eg. to get 31 Jan 23:59:59.999999
        next_start_period = next_start_period - timedelta(microseconds=1)

        return next_start_period

    def _next_start_period(self, date, period='month'):
        date = copy.deepcopy(date)

        if period == 'month':
            date = date.replace(day=1)
            date = date + relativedelta(months=1)
        elif period == 'week':
            date = date - relativedelta(days=date.dayofweek)
            date = date + relativedelta(weeks=1)
        elif period == 'day':
            date = date + relativedelta(days=1)

        else:
            Exception("Invalid period selected")

        return date
    
    def floor_tick_of_date(self, date, add_day=False):
        """For a particular date, floor the time to 0

        Parameters
        ----------
        date : datetime
            Date to be amended

        Returns
        -------
        datetime
        """
        date = copy.copy(date)
        date = date.replace(hour=0)
        date = date.replace(minute=0)
        date = date.replace(second=0)
        date = date.replace(microsecond=0)

        if add_day:
            date = date + timedelta(days=1)
    
        return date
    
    def split_into_daily(self, start_date, finish_date):
        return self.split_into_arb(start_date, finish_date, BDay())

    def split_into_monthly(self, start_date, finish_date):
        return self.split_into_arb(start_date, finish_date, BMonthBegin())

    def split_into_yearly(self, start_date, finish_date):
        return self.split_into_arb(start_date, finish_date, BYearBegin())

    def split_into_arb(self, start_date, finish_date, freq):
        date_range = pd.date_range(start_date, finish_date, freq=freq)

        date_range_end = [self.floor_tick_of_date(d) for d in date_range]

        return date_range, date_range_end

    def split_date_single_list(self, start_date, finish_date, split_size ='yearly', add_partial_period_start_finish_dates=False):
        """From a start and finish date/time, create a range of dates at an annual, monthly or daily frequency. Also
        has the option of adding the start/finish dates, even if they are not strictly aligned to a frequency boundary.

        Parameters
        ----------
        start_date : Timestamp
            Start date/time

        finish_date : Timestamp
            Finish date/time

        split_size : str
            'yearly' - split into annual chunks
            'daily' - split into daily chunks
            'monthly' - split into monthly chunks

        add_partial_period_start_finish_dates : bool (default: False)
            Add the start and finish dates we originally specified even if they are not perfectly aligned to the periods

        Returns
        -------
        Timestamp (list)
        """
        from tcapy.util.timeseries import TimeSeriesOps

        start_date = TimeSeriesOps().date_parse(start_date)
        finish_date = TimeSeriesOps().date_parse(finish_date)

        if split_size == 'monthly':
            split_dates_freq = 'MS'

            dates = pd.date_range(start=start_date, end=finish_date, freq=split_dates_freq).tolist()

        elif split_size == 'daily':
            split_dates_freq = 'D'

            dates = pd.date_range(start=start_date, end=finish_date, freq=split_dates_freq).tolist()
        elif split_size == 'yearly':
            split_dates_freq = 'Y'

            dates = pd.date_range(start=start_date, end=finish_date, freq=split_dates_freq).tolist()
        else:
            dates = pd.date_range(start=start_date, end=finish_date, freq=split_size).tolist()
        #else:
        #    dates = [start_date, finish_date]

        #if len(dates) == 1:
        if add_partial_period_start_finish_dates:

            if len(dates) > 0:
                if start_date < dates[0]:
                    dates = self.flatten_list_of_lists([start_date, dates])

                if finish_date > dates[-1]:
                    dates = self.flatten_list_of_lists([dates, finish_date])
            else:
                dates = [start_date, finish_date]

        return dates

    def split_into_freq(self, start_date, finish_date, freq='5min', microseconds_offset=0, chunk_int_min=None):
        """Between a defined start/finish date/time, split it up into chunks of arbitary size (by default, 5 minutes) and
        return two lists which define the starting and ending points respectively. If the size is less than the miniumum chunksize
        then return start/finish dates

        Parameters
        ----------
        start_date : Timestamp
            Start date/time of period

        finish_date : Timestamp
            Finish date/time of period

        freq : str
            Chunksize of period (default: '5min')

        chunk_size_min : int (optional)
            Minimum chunk size in minutes

        microseconds_offset : int
            How many microseconds to offset the end point (default: 0)

        Returns
        -------
        Timestamp (list), Timestamp (list)
        """

        if chunk_int_min is not None:
            if finish_date - start_date < pd.Timedelta(minutes=chunk_int_min):
                return [start_date], [finish_date]

        date_range = pd.date_range(start_date, finish_date, freq=freq)

        date_range_end = []

        for i in range(0, len(date_range) - 1):
            end = copy.copy(date_range[i+1]) - timedelta(microseconds=microseconds_offset)
            date_range_end.append(end)

        date_range_end[-1] = date_range[-1]

        date_range = date_range[:-1]

        return date_range, date_range_end

    def remove_weekend_points(self, start_date_hist, finish_date_hist, friday_close_utc_hour=constants.friday_close_utc_hour,
                              sunday_open_utc_hour=constants.sunday_open_utc_hour, timezone='utc'):
        """Removes those periods where either the start or endpoint are in the weekend. So for example if we have periods which
        start or end on a Saturday these will be removed

        Parameters
        ----------
        start_date_hist : Timestamp (list)
            List of dates which are the starts of the periods

        finish_date_hist : Timestamp (list)
            List of dates which are the endpoints

        friday_close_utc_hour : int
            Closing hour for markets on Friday night in UTC

        sunday_open_utc_hour : int
            Opening hour for markets on Sunday night in UTC


        Returns
        -------
        Timestamp (list)
        """

        if len(start_date_hist) != len(finish_date_hist):
            raise Exception("The number of start dates, is not equal to the number of finish dates.")

        filtered_start_date_hist = []; filtered_finish_date_hist = []

        for i in range(0, len(start_date_hist)):

            # Ignore weekends/create a cutoff at Sunday open/Friday close
            if self.is_weekday_point(start_date_hist[i], finish_date_hist[i], friday_close_nyc_hour=friday_close_utc_hour,
                                     sunday_open_utc_hour=sunday_open_utc_hour, timezone=timezone):

                filtered_start_date_hist.append(start_date_hist[i])
                filtered_finish_date_hist.append(finish_date_hist[i])

        return filtered_start_date_hist, filtered_finish_date_hist

    def is_weekday_point(self, start_date, finish_date, friday_close_nyc_hour=constants.friday_close_nyc_hour,
                         sunday_open_utc_hour=constants.sunday_open_utc_hour, timezone='UTC'):
        """Is the time duration partially between the start and finish date in normal FX working time? (ie. not on a Saturday, also
        after Sunday 2200 GMT (this arbitrary, some data providers could provide data before this) and on Friday before 2200 GMT.
        We assume timezone is UTC.

        Parameters
        ----------
        start_date : pd.Timestamp
            Start date/time of the duration

        finish_date : pd.Timestamp
            Finish date/time of the duration

        friday_close_nyc_hour : int
            Closing hour for markets on Friday night in UTC

        sunday_open_utc_hour : int
            Opening hour for markets on Sunday night in UTC

        Returns
        -------
        bool
        """

        # If we request more than 48 hours (or slightly less), it will cover more than the weekend!
        if (finish_date - start_date).seconds >= constants.weekend_period_seconds:
            return True
        else:
            return self.date_within_market_hours(start_date, friday_close_nyc_hour=friday_close_nyc_hour, \
                                                 sunday_open_utc_hour=sunday_open_utc_hour, timezone=timezone) \
                or self.date_within_market_hours(finish_date, friday_close_nyc_hour=friday_close_nyc_hour, \
                                                 sunday_open_utc_hour=sunday_open_utc_hour, timezone=timezone)

    def date_within_market_hours(self, date, friday_close_nyc_hour=constants.friday_close_nyc_hour,
                                 sunday_open_utc_hour=constants.sunday_open_utc_hour, timezone='UTC'):

        try:
            ny_date = pd.Timestamp(copy.copy(date)).tz_convert('US/Eastern')
        except:
            ny_date = pd.Timestamp(copy.copy(date)).tz_localize(timezone).tz_convert('US/Eastern')

        if (date.dayofweek >= 0 and date.dayofweek <= 3):
            return True
        elif date.dayofweek == 4 and ny_date.hour < friday_close_nyc_hour:
            return True
        elif date.dayofweek == 6 and date.hour >= sunday_open_utc_hour:
            return True

        return False

    def check_data_frame_points_in_every_hour(self, df, start_date, finish_date, timezone='UTC'):
        """Checks if there are data points in every hour window between the start and finish date/time.

        We ignore any time on Sunday (as no strict starting point for FX markets on Sunday). Also any point after 1700 NYC
        on a Friday ignored

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to check for integrity

        start_date : str/pandas.Timestamp
            Start date/time of duration

        finish_date : str/pandas.Timestamp
            Finish date/time of duration

        Returns
        -------
        bool
        """
        start_date_t = pd.Timestamp(start_date).replace(tzinfo=df.index.tz)
        finish_date_t = pd.Timestamp(finish_date).replace(tzinfo=df.index.tz)

        date_list = pd.date_range(start=start_date_t, end=finish_date_t, freq='h').tolist()

        for i in range(0, len(date_list) - 1):
            st = date_list[i]
            fi = date_list[i + 1]

            # only check weekday points
            if self.is_weekday_point(st, fi):
                df_mini = df.truncate(before=st, after=fi)

                try:
                    fi_nyc = pd.Timestamp(copy.copy(fi)).tz_convert('US/Eastern')
                except:
                    fi_nyc = pd.Timestamp(copy.copy(fi)).tz_localize(timezone).tz_convert('US/Eastern')

                # Special case for Friday close/Sunday open (ignore)
                if df_mini.empty and not(fi.dayofweek == 4 and fi_nyc.hour >= constants.friday_close_nyc_hour) and not(st.dayofweek == 0) and not(st.dayofweek == 6):
                    return False

        return True

    def replace_text_in_cols(self, df, replace_text):
        """Replaces text in the columns of a DataFrame

        Parameters
        ----------
        df : DataFrame
            Time series

        replace_text : dict
            Dictionary of phases to be replaced in columns

        Returns
        -------
        DataFrame
        """

        if replace_text is not None:
            for r in replace_text.keys():
                df.columns = [x.replace(r, replace_text[r]) for x in df.columns]

        return df

    def convert_dict_of_dataframe_to_json(self, dict_of_df):
        for k in dict_of_df.keys():
            if 'df' in k:
                dict_of_df[k] = dict_of_df[k].reset_index().to_json()
            elif 'fig' in k:
                dict_of_df[k] = self._plotly_fig_2_json(dict_of_df[k])

        return dict_of_df

    def _plotly_fig_2_json(self, fig):
        """Serialize a plotly figure object to JSON so it can be persisted to disk.
        Figure's persisted as JSON can be rebuilt using the plotly JSON chart API:

        http://help.plot.ly/json-chart-schema/

        If `fpath` is provided, JSON is written to file.

        Modified from https://github.com/nteract/nteract/issues/1229
        """

        return json.dumps({'data': json.loads(json.dumps(fig.data, cls=PlotlyJSONEncoder)),
                           'layout': json.loads(json.dumps(fig.layout, cls=PlotlyJSONEncoder))})

    # # generic list style functions
    # def check_emp(self, obj):
    #     if obj is None:
    #         return False
    #
    #     if isinstance(obj, str):
    #         if obj == '':
    #             return False
    #
    #     return True
    #
    # def split_dataframe_to_list(self, data_frame):
    #     data_frame_list = []
    #
    #     for col in data_frame.columns:
    #         data_frame_list.append(
    #             pd.DataFrame(index=data_frame.index, columns=[col], data=data_frame[col]))
    #
    #     return data_frame_list
    #
    # def flatten_list(self, lst):
    #     return list(numpy.array(lst).flat)
    #
    #
        
        
        


