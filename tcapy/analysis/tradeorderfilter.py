from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc
import pytz

import pandas as pd

from tcapy.conf.constants import Constants
from tcapy.util.timeseries import TimeSeriesOps

from tcapy.util.utilfunc import UtilFunc

constants = Constants()

# compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

class TradeOrderFilter(ABC):
    def __init__(self, **kwargs):
        self._time_series_ops = TimeSeriesOps()
        self._util_func = UtilFunc()

    def set_trade_order_params(self, **kwargs):
        """Sets the parameters for our trade/order filtering

        Parameters
        ----------
        kwargs : dict
            Sets the parameters for filtering our trades/orders

        Returns
        -------

        """
        pass

    def filter_trade_order_dict(self, trade_order_df_dict=None):
        """Filters a dictionary of trade/orders according to various criteria.

        Parameters
        ----------
        trade_order_df_dict : dict
            Trade/orders dictionary

        Returns
        -------
        dict
        """

        for k in trade_order_df_dict.keys():
            trade_order_df_dict[k] = self.filter_trade_order(trade_order_df=trade_order_df_dict[k])

        return trade_order_df_dict

    @abc.abstractmethod
    def filter_trade_order(self, trade_order_df=None):
        """Filters a specific DataFrame of trade/orders

        Parameters
        ----------
        trade_order_df : DataFrame
            Trade/orders

        Returns
        -------
        DataFrame
        """
        pass


class TradeOrderFilterTag(TradeOrderFilter):
    def __init__(self, tca_request=None, tag_value_combinations={}):
        """Initialise with the TCA parameters of our analysis and which field/value combinations we wish to filter for.

        Parameters
        ----------
        tca_request : TCARequest
            TCA parameters for our analysis


        tag_value_combinations : dict
            User defined fields and their value to be filtered
        """
        self._util_func = UtilFunc()

        self.set_trade_order_params(tca_request=tca_request, tag_value_combinations=tag_value_combinations)

    def set_trade_order_params(self, tca_request=None, tag_value_combinations={}):
        """Sets the parameters for filtering of trade/orders according to the values of tags

        Parameters
        ----------
        tca_request : TCARequest

        tag_value_combinations : dict
            Filter for a combination of tag/values

        Returns
        -------

        """
        self.tca_request = tca_request
        self.tag_value_combinations = tag_value_combinations
        self._util_func = UtilFunc()

        if tag_value_combinations != {}:
            self.tag = self._util_func.dict_key_list(tag_value_combinations.keys())

    def filter_trade_order(self, trade_order_df=None, tag_value_combinations={}):
        """Filters a trade/order DataFrame for user defined tag/value combinations (field values).

        Parameters
        ----------
        trade_order_df : DataFrame
            Trades/orders

        tag_value_combinations : dict
            User defined values for fields to be filtered

        Returns
        -------
        DataFrame
        """
        tag = None

        if tag_value_combinations != {}:
            tag_value = tag_value_combinations

            tag = self._util_func.dict_key_list(tag_value_combinations.keys())
        elif self.tag_value_combinations != {}:
            tag_value = self.tag_value_combinations
            tag = self._util_func.dict_key_list(tag_value.keys())
        # else:
        #    tag_value = self.tca_request.__dict__

        if tag is not None:
            for t in tag:
                trade_order_df = self._filter_by_tag(trade_order_df=trade_order_df, tag=t, tag_value=tag_value[t])

        return trade_order_df

    def _filter_by_tag(self, trade_order_df=None, tag=None, tag_value=None):
        """Filters a DataFrame of trade/orders by a certain field/tag for a certain value.

        Parameters
        ----------
        trade_order_df : DataFrame
            Trades/orders

        tag : str
            Field to be filtered

        tag_value : str
            Value of field to keep

        Returns
        -------
        DataFrame
        """

        try:
            # check to ensure tag actually exists as a field
            # special case for the word 'All", which means we filter for everything
            if tag in trade_order_df.columns:
                if isinstance(tag_value, list):
                    if tag_value is not None:
                        if 'All' not in tag_value:
                            return trade_order_df[trade_order_df[tag].isin(tag_value)]
                else:
                    if tag_value is not None:
                        if tag_value != 'All':
                            return trade_order_df[trade_order_df[tag] == tag_value]
        except Exception as e:
            print(str(e))

        return trade_order_df


class TradeOrderFilterTimeOfDayWeekMonth(TradeOrderFilter):
    """Filters trades/orders by user specified times of day, days of the week and months of the year.

    """

    day_of_week_ordinals = {'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3, 'fri': 4, 'sat': 5, 'sun': 6}

    month_of_year_ordinals = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                              'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}

    def __init__(self, tca_request=None, time_of_day={'start_time': '07:00:00', 'finish_time': '17:00:00'},
                 day_of_week=None, month_of_year=None, specific_dates=None, time_zone='utc'):
        """Initialise our filter, by the times of day, days of the week and months we wish to filter our trade/filters by.
        Note that it is optional which period to filter by (eg. we can filter just by time of day if we want to).

        Parameters
        ----------
        tca_request : TCARequest
            TCA parameters for our analysis

        time_of_day : dict
            Describing the start and finish time of our filter

        day_of_week : str
            Which day of the week to filter by?

        month_of_year : str
            Which month of the of the year to filter by?

        time_zone : str
            Time zone to use (eg. 'utc')
        """
        self.set_trade_order_params(tca_request=tca_request, time_of_day=time_of_day,
                                    day_of_week=day_of_week, month_of_year=month_of_year, specific_dates=specific_dates,
                                    time_zone=time_zone)

    def set_trade_order_params(self, tca_request=None, time_of_day=None,
                               day_of_week=None, month_of_year=None, specific_dates=None,
                               time_zone='utc'):
        """Initialise our filter, by the times of day, days of the week and months we wish to filter our trade/filters by.
        Note that it is optional which period to filter by (eg. we can filter just by time of day if we want to).

        Parameters
        ----------
        tca_request : TCARequest
            TCA parameters for our analysis

        time_of_day : dict
            Describing the start and finish time of our filter

        day_of_week : str
            Which day of the week to filter by?

        month_of_year : str
            Which month of the of the year to filter by?

        specific_dates : str / str (list)
            Which dates to filter by

        time_zone : str
            Time zone to use (eg. 'utc')
        """

        self.tca_request = tca_request
        self.time_of_day = time_of_day
        self.day_of_week = day_of_week
        self.month_of_year = month_of_year
        self.specific_dates = specific_dates
        self.time_zone = time_zone

        self._util_func = UtilFunc()

    def filter_trade_order(self, trade_order_df=None, time_of_day=None, day_of_week=None, month_of_year=None,
                           specific_dates=None,
                           time_zone=None):
        """

        Parameters
        ----------
        trade_order_df : DataFrame
            Trades/orders to be filed

        time_of_day : dict
            Filter trades/orders between certain times of day
            Eg. {'start_time' : '10:00', 'finish_time' : '12:00'}

        day_of_week : str
            Filter trades/orders by certain day of week
            Eg. Mon, Tue etc.

        month_of_year : str
            Filter trades/orders by certain month of year
            Eg. Jan, Feb etc.

        specific_dates : str / list(str)
            Filter trades by specific user defined dates

        time_zone : str
            Timezone for the time of day

        Returns
        -------

        """

        if time_of_day is None: time_of_day = self.time_of_day
        if day_of_week is None: day_of_week = self.day_of_week
        if month_of_year is None: month_of_year = self.month_of_year
        if specific_dates is None: specific_dates = self.specific_dates
        if time_zone is None: time_zone = self.time_zone

        # Filter by time of day
        if time_of_day is not None and trade_order_df is not None:
            start_time = time_of_day['start_time']
            finish_time = time_of_day['finish_time']

            if start_time == finish_time: return trade_order_df

            # Convert timezone which we have defined our time in
            original_time_zone = trade_order_df.index.tz
            trade_order_df = trade_order_df.tz_convert(pytz.timezone(time_zone))

            # Filter by that time of day
            # trade_order_df = trade_order_df[(trade_order_df.index.time >= start_time) & (trade_order_df.index.time <= finish_time)]
            trade_order_df = trade_order_df.between_time(start_time, finish_time)

            # Convert back to original timezone
            trade_order_df = trade_order_df.tz_convert(original_time_zone)

        # Filter by day of week
        if day_of_week is not None and trade_order_df is not None:
            if not(isinstance(day_of_week, list)): day_of_week = [day_of_week]

            day_of_week = [self.day_of_week_ordinals[x.lower()[0:3]] for x in day_of_week]

            trade_order_df = trade_order_df[trade_order_df.index.dayofweek.isin(day_of_week)]

        # Filter by month of year
        if month_of_year is not None and trade_order_df is not None:
            if not(isinstance(month_of_year, list)): month_of_year = [month_of_year]

            month_of_year = [self.month_of_year_ordinals[x.lower()[0:3]] for x in month_of_year]

            trade_order_df = trade_order_df[trade_order_df.index.month.isin(month_of_year)]

        # Filter by specific dates
        if specific_dates is not None and trade_order_df is not None:
            if not(isinstance(specific_dates, list)): specific_dates = [specific_dates]

            trade_order_df_list = []

            for d in specific_dates:
                # If the user has specified the start date/time & finish date/time as a list
                # eg. ["01 Jan 2017", "10 Jan 2017"]
                if isinstance(d, list):
                    start = self._util_func.parse_datetime(d[0])
                    finish = self._util_func.parse_datetime(d[1])
                else:
                    start, finish = self._util_func.period_bounds(self._util_func.parse_datetime(d), period='day')

                if start.tzinfo is None:
                    start = start.replace(tzinfo=pytz.timezone(time_zone))
                if finish.tzinfo is None:
                    finish = finish.replace(tzinfo=pytz.timezone(time_zone))

                trade_order_df_list.append(trade_order_df[start:finish])

            trade_order_df = pd.concat(trade_order_df_list)

        return trade_order_df
