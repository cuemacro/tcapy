from __future__ import division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import numpy as np
import pandas as pd
import pytz

from datetime import timedelta

from bisect import bisect  # operate as sorted container

import copy

from random import randint

from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager

constants = Constants()

from tcapy.util.customexceptions import *

class TimeSeriesOps(object):
    """TimeSeriesOps provides generalised time series operations on DataFrame objects, which are used throughout the
    library. These include filtering a DataFrame by start/finish dates, joining DataFrames, doing VLOOKUP style operations,
    calculating open/high/low/close prices for defined resampling periods etc.

    """

    def __init__(self):
        pass
        # self.logger = LoggerManager().getLogger(__name__)

    def concat_dataframe_list(self, df_list, sort=True):
        """Concatenates a list of DataFrames into a single DataFrame and sorts them. Removes any empty or None elements
        from the list and optionally sorts them)

        Parameters
        ----------
        df_list : DataFrame (list)
            DataFrames to be concatenated

        sort : bool (default: True)
            Sorts final concatenated DataFrame by index

        Returns
        -------
        DataFrame
        """
        if df_list is not None:

            # Remove and empty dataframes from the list
            if isinstance(df_list, list):
                df_list = [x for x in df_list if x is not None]
                df_list = [x for x in df_list if not x.empty]
            else:
                return df_list

            # Only concatenate if any non-empty dataframes are left
            if len(df_list) > 0:
                # Careful: concatenating DataFrames can change the order, so insist on arranging by old cols
                old_cols = df_list[0].columns

                if len(df_list) == 1:
                    df_list = df_list[0]

                    if sort:
                        df_list = df_list.sort_index()

                else:
                    df_list = pd.concat(df_list, sort=sort)

                return df_list[old_cols]

        return None

    def nanify_array_based_on_other(self, array_to_match, matching_value, array_to_filter):
        """Make elements of an array NaN, depending on matches in another (same-sized) array.

        Parameters
        ----------
        array_to_match : np.array
            Array to match one

        matching_value : double
            What matching array should filter to

        array_to_filter : np.array
            Array to be NaNified where matches

        Returns
        -------
        np.array
        """

        return np.where(array_to_match == matching_value, np.nan, array_to_filter)  # ie. put NaN for sells

    def downsample_time_series_floats(self, df, do_downsample):
        """Downsamples numerical values in a DataFrame to float32

        Parameters
        ----------
        df : DataFrame
            Data to be downsample

        do_downsample : bool
            Flag to activate function

        Returns
        -------
        DataFrame
        """

        if do_downsample:
            for i, j in zip(df.columns, df.dtypes):
                if str(j) == 'float64':
                    df[i] = df[i].astype(np.float32)

        return df

    def downsample_time_series_usable(self, df, start_date=None, finish_date=None, field='mid'):
        """Creates a downsampled version of data ready for plotting.

        Parameters
        ----------
        df : DataFrame
            Time series data, typically containing the mid price for a ticker

        start_date : str
            Start date of plot

        finish_date : str
            Finish date of plot

        Returns
        -------
        pd.DataFrame,
        """

        # Curtail the time series we plot to a specific date range
        if start_date is not None and finish_date is not None:
            df = self.filter_between_dates(df, start_date, finish_date)

        # Get the resampling rate which will fit the maximum number of chart data points
        seconds = self.calculate_resample_period(df)

        # Resample mid into open/high/low/close and everything else (ie. bid/ask/mid) into mean
        downsampled_df = pd.concat(
            [self.resample_time_series(df[field], resample_amount=seconds, how='ohlc',
                                                        unit='seconds'),
            self.resample_time_series(df, resample_amount=seconds, how='mean',
                                                        unit='seconds')],
            axis=1
        )

        return downsampled_df

    def filter_between_dates(self, df, start, finish):
        """Filters a DataFrame between two specific dates (on an inclusive basis)

        Parameters
        ----------
        df : DataFrame
            DataFrame to be filtered
        start : str
            Start date
        finish : str
            Finish date

        Returns
        -------
        DataFrame
        """

        return df.loc[(start <= df.index) & (df.index <= finish)]

    def remove_between_dates(self, df, start=None, finish=None):
        """Removes the entries between a defined start/finish date/time (on an inclusive basis)

        Parameters
        ----------
        df : DataFrame
            Data to be filtered

        start : Timestamp
            Date to start deletion

        finish : Timestamp
            Date to finish deletion

        Returns
        -------
        DataFrame
        """
        if start is not None and finish is not None:

            if isinstance(df.index, pd.DatetimeIndex):
                start_df = df.loc[df.index[0]:start]
                finish_df =  df.loc[finish:df.index[len(df.index)-1]]

                df = pd.concat([start_df, finish_df])
            else:
                df = df.loc[(df.index <= start) | df.index >= finish]

        elif start is None and finish is not None:
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.loc[finish:df.index[len(df.index) - 1]]
            else:
                df = df.loc[df.index >= finish]
        elif start is not None and finish is None:
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.loc[df.index[0]:start]
            else:
                df = df.loc[df.index <= start]

        return df

    def filter_time_series_by_included_keyword(self, keyword, data_frame):
        """Filter time series to include columns which contain keyword

        Parameters
        ----------
        keyword : str
            columns to be included with this keyword
        data_frame : DataFrame
            data frame to be filtered

        Returns
        -------
        DataFrame
        """
        columns = [elem for elem in data_frame.columns if keyword in elem]

        if len(columns) > 0:
            return data_frame[columns]

        return None

    def filter_time_series_by_matching_columns(self, data_frame, columns):
        """Filters a DataFrame so it only has the matching columns

        Parameters
        ----------
        data_frame : DataFrame
            DataFrame to be filtered

        columns : str (list)
            Which columns to be filtered

        Returns
        -------
        DataFrame
        """

        columns_matched = [x for x in columns if x in data_frame.columns]

        if len(columns_matched) > 0:
            return data_frame[columns_matched]

        return None

    def filter_time_series_by_time_of_day(self, data_frame, start_time=None, finish_time=None):
        """Filter a DataFrame so it only has points in between specified times of day

        Parameters
        ----------
        data_frame : DateFrame
            Time series

        start_time : str (default: None)
            Start time of filter

        finish_time : str (default: None)
            Finish time of filter

        Returns
        -------
        DataFrame
        """

        if start_time is not None:
            data_frame = data_frame[data_frame.index.time >= start_time]

        if finish_time is not None:
            data_frame = data_frame[data_frame.index.time <= finish_time]

        return data_frame

    def filter_start_finish_dataframe(self, data_frame, start_date, finish_date):
        """Strips down the data frame to the dates which have been requested in the initial TCA request

        Parameters
        ----------
        data_frame : DataFrame
            Data to be stripped down

        start_date : datetime
            Start date of the computation

        finish_date : datetime
            Finish date of the computation

        Returns
        -------
        DataFrame
        """

        if data_frame is not None:
            try:
                if start_date is not None and finish_date is not None:
                    return data_frame.loc[(data_frame.index <= finish_date) & (data_frame.index >= start_date)]
                elif start_date is not None:
                    return data_frame.loc[(data_frame.index >= start_date)]
                elif finish_date is not None:
                    return data_frame.loc[(data_frame.index <= finish_date)]
            except:
                return None

    def vlookup_style_data_frame(self, dt, data_frame, search_field, timedelta_amount=None, just_before_point=True):
        """Does a VLOOKUP style search in a DataFrame given a set of times for a particular field. We assume both
        our DataFrame and dates to lookup are sorted (oldest first).

        Parameters
        ----------
        dt : DateTimeIndex list
            Dates to be looked up

        data_frame : DataFrame
            The DataFrame where we wish to do our lookup

        search_field : str
            Which field do we want to output

        timedelta_amount : TimeDelta (default: None)
            How much we wish to perturb our search times

        just_before_point : bool (default: True)
            Should we fetch the point just before (in the case of not matching), which would be necessary for slippage
            calculations, by contrast for market impact we would likely want to set this to False (ie. for points just after)

        Returns
        -------
        Series, DateTimeIndex
        """
        logger = LoggerManager.getLogger(__name__)

        # logger.debug("Applying VLOOKUP in timezone " + str(dt.tz) + " with " + str(data_frame.index.tz))

        if dt is None:
            return None, None

        if len(dt) == 0:
            return None, None

        # Check that our input times are within the bounds of our data frame
        if dt[0] <= data_frame.index[0] or dt[-1] >= data_frame.index[-1]:
            err_msg = "Lookup data (eg. trade) does not fully overlap with the main search space of data (eg. market)"

            logger.error(err_msg)

            raise ValidationException(err_msg)

            # return None, None

        indices = self.search_series(data_frame, dt, timedelta_amount=timedelta_amount, just_before_point=just_before_point)

        search_series = data_frame[search_field].iloc[indices]
        actual_dt = search_series.index
        search_series.index = dt

        # Return our VLOOKUPed values and alongside it, the time stamps of those observations
        return search_series, actual_dt

    def search_series(self, data_frame, dt, timedelta_amount = None, just_before_point=True):
        dt_shifted = dt

        if timedelta_amount is not None:
            dt_shifted = dt + timedelta_amount

        indices = data_frame.index.searchsorted(dt_shifted)

        # Make sure that the indices are smaller (because search sorted can indicate and index after

        # TODO do additional check on this
        if just_before_point:
            # for exact matches we don't wish to move the point, so search for inexact matches ONLY
            # careful, we don't select indices OUTSIDE of our search space (by default take the last point)
            is_inexact_match = dt_shifted != data_frame.index[np.minimum(indices, len(data_frame.index) - 1)]

            # for inexact matches get the point just after
            indices[is_inexact_match] = indices[is_inexact_match] - 1

            # if indices[0] < 0: indices[0] = 1

        # If any points are past the end of the series assume it's the last point
        # Conversely any points before start are assumed to be start of series
        indices[indices >= len(data_frame.index)] = len(data_frame.index) - 1
        indices[indices < 0] = 0

        return indices

    def outer_join(self, df_list):
        """Does an outer join of a list of DataFrames, into a single DataFrame

        Parameters
        ----------
        df_list : DataFrame (list)
            Data sets to be joined

        Returns
        -------
        DataFrames
        """
        if df_list is None: return None

        # Remove any None elements (which can't be joined!)
        df_list = [i for i in df_list if i is not None]

        if len(df_list) == 0:
            return None
        elif len(df_list) == 1:
            return df_list[0]

        # df_list = [dd.from_pd(df) for df in df_list]

        return df_list[0].join(df_list[1:], how="outer")

    def merge(self, df_list, on=None):
        return reduce(lambda x, y: pd.merge(x, y, on=on), df_list)

    ### TODO Add to specification
    # def reweight_observations(self, df, field, weighting_field, newly_weighted_field = None):
    #
    #     total = df[weighting_field].sum()
    #
    #     if newly_weighted_field is None:
    #         newly_weighted_field = field
    #
    #     df[newly_weighted_field] = (df[field].values * df[weighting_field].values) / total
    #
    #     return df

    def calculate_resample_period(self, df, chart_max_time_points=constants.chart_max_time_points):
        """For a given DataFrame, what resampling period should we use such that it does not go over our max points
        parameter. This is useful for downsampling charts so they do not take too long to display.

        Parameters
        ----------
        df : DataFrame
            Data we wish to plot

        chart_max_time_points : int
            Maximum number of time periods

        Returns
        -------
        int
        """

        # Resample for plotting (careful we don't exceed max number of points - Plotly will get VERY SLOW with many
        # when trying to plot many points and will noticable be laggy for GUI users)
        number_of_points = (df.index[-1] - df.index[0]).total_seconds()

        max_points = float(chart_max_time_points)

        # try these resampling values (ie. 600 is 600 seconds)
        points = [1, 2, 5, 10, 15, 30, 60, 120, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000, 3300, 3600, 3900,
                  4200, 4500, 4800, 5100, 5400, 5700, 6000, 6300, 6600, 6900, 7200, 7500, 7800, 8100, 8400, 8700, 9000]

        # default resampling
        seconds = points[-1]

        for i in range(0, len(points)):
            if (float(number_of_points) / float(points[i])) < float(max_points):
                seconds = points[i]
                break

        return seconds

    def get_time_unit(self, unit):
        """Converts full English word of time unit into the letter representation used by Pandas (eg. for resampling)

        Parameters
        ----------
        unit : str
            'milliseconds' for MS
            'seconds' for S
            'minutes' for m
            'hours' for h

        Returns
        -------
        str
        """
        unit_dict = {'milliseconds': "L",
                     'seconds': "S",
                     'minutes': "T",
                     'hours': 'H'}

        unit = unit.lower()

        if unit in unit_dict.keys():
            unit_r = unit_dict[unit]
        else:
            unit_r = unit

        return unit_r

    def resample_time_series(self, df, resample_amount=1, how='mean', unit='milliseconds', field=None):
        """Resamples a DataFrame to a particular unit (eg. 'milliseconds'). We can specify resampling by for the example
        the first or last in each unit. Uses underlying resampling calls in pd.

        Parameters
        ----------
        df : DataFrame
            Data to be resampled

        resample_amount : int
            How many units?

        how : str
            'mean' - mean of each unit
            'last' - last point of each unit
            'first' - first point of each unit
            'sum' - sum of all the point in a unit
            'high' - high of the unit
            'low' - low of the unit
            'ohlc' - open/high/low/close

        unit : str
            Which unit to use ('milliseconds', 'seconds', 'minutes' or 'hours')

        field

        Returns
        -------
        DataFrame
        """

        # Note that the UNITS are not obvious for pd! Full list is below
        #
        # B       business day frequency
        # C       custom business day frequency (experimental)
        # D       calendar day frequency
        # W       weekly frequency
        # M       month end frequency
        # SM      semi-month end frequency (15th and end of month)
        # BM      business month end frequency
        # CBM     custom business month end frequency
        # MS      month start frequency
        # SMS     semi-month start frequency (1st and 15th)
        # BMS     business month start frequency
        # CBMS    custom business month start frequency
        # Q       quarter end frequency
        # BQ      business quarter endfrequency
        # QS      quarter start frequency
        # BQS     business quarter start frequency
        # A       year end frequency
        # BA      business year end frequency
        # AS      year start frequency
        # BAS     business year start frequency
        # BH      business hour frequency
        # H       hourly frequency
        # T       minutely frequency
        # S       secondly frequency
        # L       milliseonds
        # U       microseconds
        # N       nanoseconds
        #

        unit_r = self.get_time_unit(unit)

        if field is not None:
            df = df[field]

        resample_func = df.resample(str(resample_amount) + unit_r)

        if how == 'mean':
            return resample_func.mean()
        elif how == 'last':
            return resample_func.last()
        elif how == 'first':
            return resample_func.first()
        elif how == 'sum':
            return resample_func.sum()
        elif how == 'high':
            return resample_func.high()
        elif how == 'low':
            return resample_func.low()
        elif how == 'ohlc':
            return resample_func.ohlc()
            # return resample_func.agg({'open': 'first',
            #                     'high': 'max',
            #                     'low': 'min',
            #                     'close': 'last'})

    def localize_as_UTC(self, df, convert=False):
        """Localizes the the times in a DataFrame to UTC format (overwrites the previous timezone whatever it is)

        Parameters
        ----------
        df : DataFrame
            Data to be used

        Returns
        -------
        DataFrame
        """

        # Assume UTC time, if no timezone specified (don't want to mix UTC and non-UTC in database!)
        # If timezone is already in the DataFrame, then convert to UTC
        try:
            if convert:
                if df.index.tz is not None:
                    df = df.tz_convert(pytz.utc)
                else:
                    df = df.tz_localize(None)
                    df = df.tz_localize(pytz.utc)
            else:
                df = df.tz_localize(None)
                df = df.tz_localize(pytz.utc)
        except:
            pass

        return df

    def localize_cols_as_UTC(self, df, columns, index=False, convert=False):
        """Localizes selected columns/and or index in the DataFrame for UTC time

        Parameters
        ----------
        df : DataFrame
            Data to be used

        columns : str (list)
            Columns to be localized

        index : boolean (False)
            Should the index be localized?

        Returns
        -------
        DataFrame
        """

        if not(isinstance(columns, list)): columns = [columns]

        if index:
            df = self.localize_as_UTC(df, convert=convert)

        for d in columns:
            if d in df.columns:
                if convert:
                    if df[d].dt.tz is not None:
                        df[d] = df[d].tz_convert(pytz.utc)
                    else:
                        try:
                            df[d] = df[d].dt.tz_localize(pytz.utc)
                        except:
                            pass
                else:
                    try:
                        df[d] = df[d].dt.tz_localize(pytz.utc)
                    except:
                        pass

        return df

    def drop_consecutive_duplicates(self, df, field):
        """Removes rows where there are consecutive duplicates (for numerical fields only).

        Typically this can be used with tick data, to remove repeated values. Repeated NaNs are preserved.

        Note, this is different the typical pd drop duplicates where ANY duplicate is removed, regardless of location.

        Parameters
        ----------
        df : DataFrame
            Data to be analysed

        field : str (list of str)
            Field to check for consecutive duplicates

        Returns
        -------
        DataFrame
            With rows removed where the records are consecutive duplicates
        """
        if isinstance(field, list):
            diff = df[field].diff().abs().fillna(1).sum(axis=1)

            return df.loc[diff != 0]
        else:
            return df.loc[df[field].diff() != 0]

    def date_parse(self, date, assume_utc=True):
        """Parse a date into a datetime object

        Parameters
        ----------
        date : str
            Date to be parsed

        assume_utc : bool (default: True)
            Assume time is in UTC format

        Returns
        -------
        datetime
        """
        if isinstance(date, str):
            date1 = None

            if date is 'midnight':
                date1 = datetime.datetime.utcnow()

                date1 = datetime.datetime(date1.year, date1.month, date1.day, 0, 0, 0)
            elif date is 'decade':
                date1 = datetime.datetime.utcnow()

                date1 = date1 - timedelta(days=365 * 10)
            elif date is 'year':
                date1 = datetime.datetime.utcnow()

                date1 = date1 - timedelta(days=365)
            elif date is 'month':
                date1 = datetime.datetime.utcnow()

                date1 = date1 - timedelta(days=30)
            elif date is 'week':
                date1 = datetime.datetime.utcnow()

                date1 = date1 - timedelta(days=7)
            elif date is 'day':
                date1 = datetime.datetime.utcnow()

                date1 = date1 - timedelta(days=1)
            elif date is 'hour':
                date1 = datetime.datetime.utcnow()

                date1 = date1 - timedelta(hours=1)
            else:
                # format expected 'Jun 1 2005 01:33', '%b %d %Y %H:%M'
                try:
                    date1 = datetime.datetime.strptime(date, '%b %d %Y %H:%M')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                # format expected '1 Jun 2005 01:33', '%d %b %Y %H:%M'
                try:
                    date1 = datetime.datetime.strptime(date, '%d %b %Y %H:%M')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%b %d %Y')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%d %b %Y')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

            if date1 is None:
                err_msg = "Couldn't parse date, please check the format: " + str(date)

                LoggerManager().getLogger(__name__).error(err_msg)

                raise DateException(err_msg)

            date = date1
        else:
            if not(isinstance(date, pd.Timestamp)):
                date = pd.Timestamp(date)

        # for consistency we generally assume all times are in UTC
        if assume_utc:
            date = date.replace(tzinfo=pytz.utc)

        return date

    def closest_date_index(self, df, date):
        """Gets the closest index location in a DataFrame to a particular date. It is used, when users specify markout
        points of trades, in market data (which is unlikely to align exactly to the trade point).

        Parameters
        ----------
        df : DataFrame
            Data to be searched

        date : datetime
            Date/time to be found

        Returns
        -------
        int
        """

        np_dt64 = np.datetime64(pd.Timestamp(date))

        timestamps = np.array(df.index)

        upper_index = bisect(timestamps, np_dt64,
                             hi=len(timestamps) - 1)  # find the upper index of the closest time stamp

        return df.index.get_loc(min(timestamps[upper_index], timestamps[upper_index - 1], key=lambda x: abs(
            x - np_dt64)))  # find the closest between upper and lower timestamp

    def weighted_average_by_agg(self, df, data_col, weight_col, by_col_agg, unweighted_data_col=None,
                                unweighted_agg='sum'):
        """Calculates a weighted average for a column, aggregating the result by another column. We can also
        specify other columns, not to include in the weighting (these can be aggregated using an unweighted average or
        summed).

        One typical usage is calculating the average execution price for a number of trades, weighting our average
        by the executed notional for each trade.

        Parameters
        ----------
        df : pd.DataFrame
            Dataset to be averaged

        data_col : str (list)
            Column to be (weighted) average

        weight_col : str
            Column to use for weighting of the average

        by_col_agg : str
            Aggregate our weighted average column by this

        unweighted_data_col : str (list)
            These columns will be unweighted

        unweighted_agg : str
            'mean' - the unweighted column will be aggregated using an unweighted average
            'sum' (default) - the unweighted column will be aggregated using a sum

        Returns
        -------
        pd.DataFrame
        """

        unweighted_data_col = copy.copy(unweighted_data_col)

        # If we want certain columns NOT to be weighted in our average
        if unweighted_data_col is not None:

            if isinstance(unweighted_data_col, list):
                for u in unweighted_data_col:
                    if u not in df.columns:
                        unweighted_data_col.remove(u)

            if isinstance(unweighted_data_col, list) and len(unweighted_data_col) > 0:
                unweighted_data_col = unweighted_data_col[0]

            df = pd.DataFrame(index=df.index,
                                  data={'_data_times_weight': df[data_col].values * df[weight_col].values,
                                        '_weight_where_notnull': df[weight_col].values * pd.notnull(
                                            df[data_col].values),
                                        by_col_agg: df[by_col_agg].values,
                                        unweighted_data_col: df[unweighted_data_col].values})

        # All columns to be weighted when taking the average
        else:
            df = pd.DataFrame(index=df.index,
                                  data={'_data_times_weight': df[data_col].values * df[weight_col].values,
                                        '_weight_where_notnull': df[weight_col].values * np.isfinite(
                                            df[data_col].values),
                                        by_col_agg: df[by_col_agg].values})
        g = df.groupby(by_col_agg)

        result = g['_data_times_weight'].sum() / g['_weight_where_notnull'].sum()
        result.name = by_col_agg

        if unweighted_data_col is not None:
            if unweighted_agg == 'sum':
                unweighted_data_col_agg = g[unweighted_data_col].sum()
            elif unweighted_agg == 'mean':
                unweighted_data_col_agg = g[unweighted_data_col].mean()

            return pd.DataFrame(index=result.index,
                                    data={data_col: result, unweighted_data_col: unweighted_data_col_agg})

        result.name = data_col

        return result

    def weighted_average_lambda(self, group, avg_name, weighting_field):
        """ http://stackoverflow.com/questions/10951341/pd-dataframe-aggregate-function-using-multiple-columns
        In rare instance, we may not have weights, so just return the mean. Customize this if your business case
        should return otherwise.
        """

        d = group[avg_name]

        if weighting_field is None: return d.mean()

        w = group[weighting_field]

        try:
            return (d * w).sum() / w.sum()
        except ZeroDivisionError:
            return d.mean()

    def weighted_average_of_each_column(self, df, weighting_col=None, append=False, exclude_fields_from_avg=[]):
        """Calculates the weighted average of each column of a dataframe. Can optionally append the average to the dataframe
        or return the average on its own.

        We can for example use this to calculate the notional weighted average markout of price action.

        Parameters
        ----------
        df : pd.DataFrame
            Data to be averaged

        weighting_col : str (None)
            Column to use as a weighting, if it left as None, we assume that the average is unweighted

        append : bool (False)
            Should we append the average to the last row?

        Returns
        -------
        pd.DataFrame
        """

        if weighting_col is not None:
            weights = df[weighting_col]
            w_sum = weights.sum()

            non_numeric = []
            index_name = df.index.name

            if append:
                df_copy = df.copy()
            else:
                df_copy = df

            for a in df_copy.columns:
                if (df_copy[a].dtype.kind in 'iufc'):
                    df_copy[a] = (df_copy[a] * weights) / w_sum
                    non_numeric.append(False)
                else:
                    df_copy[a] = np.nan
                    non_numeric.append(True)

            df_copy = df_copy.sum(axis=0)
            df_copy[non_numeric] = np.nan
        else:
            df_copy = df.mean(axis=0)

        df_copy = pd.DataFrame(df_copy).transpose()
        df_copy.index = ['Avg']
        df_copy.index.name = index_name

        for f in exclude_fields_from_avg:
            if f in df_copy.columns:
                df_copy[f] = np.nan

        if append:
            old_cols = df.columns
            df = df.append(df_copy)
            df = df[old_cols]

            return df

        return df_copy

    def get_binary_diff(self, df, column, type='int'):
        """Check if there are any changes in a column and mark this. Works with either integer or string base columns.

        Parameters
        ----------
        df : DataFrame
            Data to be checked

        column : str
            Column to be checked

        type : str
            What is the type of column, we want to check?

        Returns
        -------
        DataFrame
        """

        if type == 'str':
            dt = (df[column].ne(df[column].shift())).astype(int)
            dt[0] = 0
        elif type in ['int', 'float', 'double']:

            x = df[column].copy()
            x = x.fillna(0)

            dt = x.diff()

        dt[dt != 0] = 1

        return dt

    def round_dataframe(self, df, round_figures_by, columns_to_keep=None):
        """Rounds a DataFrame by a number of decimal places. Users can also (optionally) specify which columns to keep of
        the DataFrame. It is also customisable to round different columns by different numbers of decimal places.

        Parameters
        ----------
        df : DataFrame
            Data to be rounded

        round_figures_by : int or dict
            Number of decimal places to round by (int) or we can specify the decimal places by column name (dict)

        columns_to_keep : str (list) opt
            Columns to keep in our final output

        Returns
        -------
        DataFrame
        """
        if columns_to_keep is None: columns_to_keep = df.columns

        if round_figures_by is not None:

            # If we want to round figures all by the same amount
            if isinstance(round_figures_by, int):

                for col in columns_to_keep:

                    # For cases when the type is numerical
                    if df[col].dtype.kind in 'iufc':
                        df[col] = df[col].round(round_figures_by)
            elif isinstance(round_figures_by, dict):
                # Otherwise, we specify the rounding differently according to column

                exclude = []

                # Should we exclude any columns from our rounding?
                if 'exclude' in round_figures_by.keys():
                    exclude = round_figures_by['exclude']

                    if not (isinstance(exclude, list)): exclude = [exclude]

                # Special case: when rounding all
                if 'all' in round_figures_by.keys():
                    mult = round_figures_by['all']

                    # round_figures_by = {}
                    for x in df.columns:
                        if x not in round_figures_by.keys():
                            round_figures_by[x] = mult

                # Only round those columns which are numerical and not excluded (careful: can round different
                # columns by a different number of decimal places)
                for col in round_figures_by.keys():
                    # for cases when the type is numerical
                    if col in df.columns and col not in exclude and df[col].dtype.kind in 'iufc':
                        df[col] = df[col].round(round_figures_by[col])

        return df

    def multiply_scalar_dataframe(self, df, scalar=None):
        """Multiplies a DataFrame by a scalar. Users can also choose to mulitply different columns by a different scalar.
        For example, we may wish to mulitply only the slippage by 10000 to convert it into basis points, but not other
        numerical values, such as notional.

        Parameters
        ----------
        df : DataFrame
            Data to be multiplied by scalar

        scalar : float or dict
            Scalar to multiply dataframe by (float) or  or we can specify scalar by column name (dict)

        Returns
        -------
        DataFrame
        """
        if scalar is not None:

            # For instances when we specify when different columns are multiplied by different scalars
            if isinstance(scalar, dict):

                exclude = []

                # Exclude these columns from our multiplication
                if 'exclude' in scalar.keys():
                    exclude = scalar['exclude']

                    if not (isinstance(exclude, list)): exclude = [exclude]

                # For cases when we have specified 'all' columns to be multiplied
                if 'all' in scalar.keys():
                    mult = scalar['all']

                    for x in df.columns:
                        if x not in scalar.keys():
                            scalar[x] = mult

                for s in scalar.keys():
                    # For cases when the type is numerical and are not excluded, multiply them
                    if s in df.columns and s not in exclude and df[s].dtype.kind in 'iufc':
                        df[s] = df[s] * np.float(scalar[s])
            else:
                # Otherwise multiply every column by the same scalar (if it's numerical)
                df = df.apply(lambda x: x * np.float(scalar) if x.dtype.kind in 'iufc' else x)

        return df

    def aggregate_dict_to_dataframe(self, dictionary, index_col, drop_cols=None):
        """Aggregates a dictionary into a DataFrame.

        Parameters
        ----------
        dictionary : dict
            Dictionary of fields/values

        index_col : str
            Which column to use as index?

        drop_cols : str (list) - opt
            Which columns to drop?

        Returns
        -------
        DataFrame
        """
        df = pd.DataFrame.from_dict(dictionary)

        if drop_cols is not None:
            if not (isinstance(drop_cols, list)):
                drop_cols = [drop_cols]

            df = df.drop(drop_cols, axis=1)

        df = df.set_index(index_col)

        return df

    def outer_join(self, df_list):
        """Outer joins a list of DataFrames into a single large DataFrame.

        Parameters
        ----------
        df_list : DataFrame (list)
            DataFrame to be joined

        Returns
        -------
        DataFrame
        """
        if df_list is None: return None

        # Remove any None elements (which can't be joined!)
        df_list = [i for i in df_list if i is not None]

        if len(df_list) == 0:
            return None
        elif len(df_list) == 1:
            return df_list[0]

        # df_list = [dd.from_pd(df) for df in df_list]

        return df_list[0].join(df_list[1:], how="outer")

    def split_array_chunks(self, array, chunks=None, chunk_size=None):
        """Splits an array or DataFrame into a list of equally sized chunks

        Parameters
        ----------
        array : NumPy array/pd DataFrame
            array to be split into chunks

        chunks : int (optional)
            number of chunks

        chunk_size : int (optional)
            size of each chunk (in rows)

        Returns
        -------
        list of arrays or DataFrames
        """

        if chunk_size is None and chunks is None:
            return array

        if chunk_size is None:
            chunk_size = int(array.shape[0] / chunks)

        if chunks is None:
            chunks = int(array.shape[0] / chunk_size)

        # alternative split array method (untested)

        # if isinstance(array, pd.DataFrame):
        #     array = array.copy()
        #     array_list = []
        #
        #     for start in range(0, array.shape[0], chunk_size):
        #         array_list.append(array.iloc[start:start + chunk_size])
        #
        #     return array_list

        if chunks > 0:
            # if isinstance(array, pd.DataFrame):
            #    array = [array[i:i + chunk_size] for i in range(0, array.shape[0], chunk_size)]

            return np.array_split(array, chunks)

        return array

    def assign_column(self, df, column, mask, val):
        """Assigns a column of a DataFrame with a value over which there's a mask

        Parameters
        ----------
        df : DataFrame
            With data

        column : str
            Column to  be filled

        mask : DataFrame
            Mask to be applied to column

        val : DataFrame
            Value for replacing (subject to mask)

        Returns
        -------
        DataFrame
        """
        # if there is only one value, pandas can't assign so replicate (these won't be assigned where False is the mask
        # anyway
        if val.size == 1:
            val = np.repeat(val[0], len(mask.index))

        df[column][mask.values] = val

        return df

import datetime
import random

from dateutil.rrule import rrule, DAILY

class RandomiseTimeSeries(object):
    """Operations to randomise time series. Typically used when creating test trades/orders for development purposes.

    These should not be used when running tcapy in a production environment.

    """

    def randomly_fill(self, df, proportion, column=None, fill=np.nan):
        """Fills a DataFrame with values randomly (by default NaN), according to a given percentage proportion. If
        columns are not specified then we assume we should randomly fill all columns.

        Parameters
        ----------
        df : DataFrame
            DataFrame to be randomly filled
        proportion : float (list)
            The proportion of the DataFrame to be randomly filled (eg. 0.1 is 10%)
        column : str (list)
            Columns to be randomly filled with value (or randoms)
        fill : object (list)
            Fill value to be used

        Returns
        -------
        DataFrame
        """

        df_old = df

        # If columns are not specified, assume we should randomly fill all of them
        if column is None:
            column = df.columns

        if isinstance(column, str):
            column = [column]

        if not (isinstance(proportion, list)):
            proportion = [proportion]

        if not (isinstance(fill, list)):
            fill = [fill]

        if column is not None:
            df = df[column]

            if df is None:
                return None

        # Get number of rows/columns
        if isinstance(df, pd.DataFrame):
            M = len(df.index)
            N = len(df.columns)
        elif isinstance(df, pd.Series):
            df = pd.DataFrame(df)
            M = len(df.index)
            N = 1
        elif isinstance(df, np.ndarray):
            M = df.shape[0]
            N = df.shape[1]

        # How many to fill in NaN
        for i in range(0, len(proportion)):
            c = int(proportion[i] * M)

            # Create a randomised mask
            mask = np.zeros(M * N, dtype=bool)
            mask[:c] = True
            np.random.shuffle(mask)
            mask = mask.reshape(M, N)

            # Fill randomized mask with NaN (or other fill)
            df[mask] = fill[i]

        if column is not None:

            for col in column:
                df_old[col] = df[col]

            return df_old

        return df

    def randomly_perturb_time(self, df, time_field='milliseconds', max_amount=1000, area_of_perturbation='before'):

        # Create a randomised integer between 0 and max_amount
        perturb = np.random.rand(len(df.index), 1) * float(max_amount)

        # Should we add to the time (after), substract (before) or do both (before_and_after)
        if area_of_perturbation == 'before':
            perturb = perturb * -1
        elif area_of_perturbation == 'before_and_after':
            perturb = perturb - float(max_amount) / float(2)
        elif area_of_perturbation == 'after':
            pass

        # Round to nearest integer
        perturb = np.rint(perturb)

        from datetime import timedelta

        if 'millisecond' in time_field:
            df.index = [df.index[i] + timedelta(milliseconds=int(perturb[i])) for i in range(0, len(df.index))]

        return df

    def randomly_perturb_column(self, df, column=None, magnitude=1000.0, skew=-0.25, scale=0.5):
        """Perturb columns of a DataFrame by a randomised amount. Useful for creating trade data from market data (typically
        by perturbing bids for "sell" trades and ask quotes for "buy" trades.

        Parameters
        ----------
        df : DataFrame
            DataFrame to be perturbed
        column : str or list(str)
            Columns to be perturbed
        magnitude : float
            Order of magnitude divisor
            eg. 1000 would perturb by a randomised amount -0.5/1000 to +0.5/1000 (where skew is -0.5 and scale is 1)
            eg. 1 would perturb by a randomised amount -0.5/1 to +0.5/1 (where skew is -0.5 and scale is 1)
            eg. 1000 would perturb by a randomised amount -1.0/1000 to +1.0 (where skew is -1.0 and scale is 2)
        skew : float
            Size of the skew (should be half of the scale to ensure equal distribution above and below)
        scale : float
            Max size of the perturbation from the lower skew area

        Returns
        -------
        DataFrame
        """

        if column is None:
            column = df.columns

        if isinstance(column, str):
            column = [column]

        for col in column:
            # TODO check if column is numerical (cannot perturb string based columns)

            # Come up with a randomised vector to multply for our pertubation
            rand = scale * np.random.rand(len(df.index), 1)
            perturb = 1.0 + ((rand + skew) / float(magnitude))

            df[col] = np.multiply(df[col].values, perturb.T).T

        return df

    def randomly_remove_rows(self, df, remove_perc_proportion=0.75):

        return df.drop(df.sample(int(remove_perc_proportion * float(len(df.index)))).index)

    def randomly_truncate_data_frame_within_bounds(self, df, start_perc=0.25, finish_perc=0.75):
        length = len(df.index)

        start_date = df.index[randint(0, int(start_perc * float(length)))]
        finish_date = df.index[randint(int(finish_perc * float(length)), length - 1)]

        df = df[df.index >= start_date]
        df = df[df.index <= finish_date]

        return df

    def randomly_split_number_into_n_parts_totalling(self, total, n, min_factor=0):

        # Generate random sequence
        a = np.random.random(n)

        # Scale so they add to 1
        a /= a.sum()

        # Scale by proportion
        a *= (float(total) * (1.0 - min_factor))

        # Create a floor (eg. half of what would have been equal division)
        a += np.ones(n) * ((float(total) * min_factor) / float(n))

        return a

    def create_random_time_series(self, max_points=-1, freq='daily', start='01 Jan 2010', end=datetime.datetime.utcnow().date(), cols=['price']):

        if freq is 'daily':
            dates = pd.date_range(start=start, end=end, freq='B')
        else:
            dates = pd.date_range(start=start, end=end, freq='1min')

        dates = dates[dates.dayofweek<5]

        if max_points > 0 and max_points <= len(dates) - 1:
            dates = dates[0:max_points]

        return pd.DataFrame(columns=cols, index=dates, data=np.random.randn(len(dates), len(cols)))
