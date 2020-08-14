from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import numpy as np
import pandas as pd
from datetime import timedelta
import abc

# from tcapy.util.loggermanager import LoggerManager
from tcapy.util.mediator import Mediator

# from tcapy.util.loggermanager import LoggerManager
from tcapy.util.timeseries import TimeSeriesOps

from tcapy.util.loggermanager import LoggerManager
from tcapy.util.customexceptions import *

from tcapy.conf.constants import Constants

# compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

constants = Constants()

class Benchmark(ABC):
    """Abstract class for calculation of benchmark prices given an input of market data, which will typically be made up
    of bid/ask quotes together with volume data. The supplied trade data will then be populated with
    that benchmark (typically will be calculated by trade). BenchmarkMarket is designed purely for market analysis, whilst
    BenchmarkTrade classes generally need to have both market and trade data.

    """

########################################################################################################################

class BenchmarkMarket(Benchmark):
    """These benchmarks can be calculated directly on market data (without the need for trade data). For example,
    we might want to calculate the mid price, in market data, we only have the bid/ask data. Alternatively, we might wish
    to calculate the spread to mid of bid/ask market data.

    """

    def __init__(self,  **kwargs):
        # self.logger = LoggerManager().getLogger(__name__)
        self._time_series_ops = Mediator.get_time_series_ops()

    @abc.abstractmethod
    def calculate_benchmark(self, market_df=None, field=None):
        pass

    def _check_empty_benchmark_market_data(self, market_df):
        if market_df is None: return True
        if market_df.empty: return True

        return False


class BenchmarkMarketMid(BenchmarkMarket):
    """Calculates the mid-point for every time point of market data from the underlying bid/ask prices.

    """
    def __init__(self, field='mid', bid='bid', ask='ask'):

        super(BenchmarkMarketMid, self).__init__()

        self._field = field
        self._bid = bid
        self._ask = ask


    def calculate_benchmark(self, market_df=None, field=None,
                            bid=None, ask=None):
        if self._check_empty_benchmark_market_data(market_df): return market_df

        if field is None: field = self._field
        if bid is None: bid = self._bid
        if ask is None: ask = self._ask

        # If the 'mid' price does not already exist in the market data, calculate it from the underlying bid/ask prices
        if field not in market_df.columns:
            # if field == 'bid/ask':
            market_df[field] = (market_df[bid].values + market_df[ask].values) / 2.0
        else:
            LoggerManager().getLogger(__name__).warning(field + " not in market data")

        return market_df

class BenchmarkMarketFilter(BenchmarkMarket):
    """Filters market data by dates etc.

    """
    def __init__(self, time_of_day=None, day_of_week=None, month_of_year=None, year=None,
                 specific_dates=None, time_zone=None):

        super(BenchmarkMarketFilter, self).__init__()

        self._time_of_day = time_of_day
        self._day_of_week = day_of_week
        self._month_of_year = month_of_year
        self._year = year
        self._specific_dates = specific_dates
        self._time_zone = time_zone


    def calculate_benchmark(self, market_df=None, time_of_day=None,
                 day_of_week=None, month_of_year=None, year=None,
                 specific_dates=None, time_zone=None):

        if self._check_empty_benchmark_market_data(market_df): return market_df

        if time_of_day is None: time_of_day = self._time_of_day
        if day_of_week is None: day_of_week = self._day_of_week
        if month_of_year is None: month_of_year = self._month_of_year
        if year is None: year = self._year
        if specific_dates is None: specific_dates = self._specific_dates
        if time_zone is None: time_zone = self._time_zone

        market_df = TimeSeriesOps().filter_time_series_by_multiple_time_parameters(market_df, time_of_day=time_of_day,
                 day_of_week=day_of_week, month_of_year=month_of_year, year=year,
                 specific_dates=specific_dates, time_zone=time_zone)

        return market_df


class BenchmarkMarketSpreadToMid(BenchmarkMarket):
    """Calculates the spread for each mid point

    """
    def __init__(self, mid='mid', bid='bid', ask='ask', bid_mid_bp=1, ask_mid_bp=1,
                 overwrite_bid_ask=False):

        super(BenchmarkMarketSpreadToMid, self).__init__()

        self._mid = mid
        self._bid = bid
        self._ask = ask

        self._bid_mid_bp = bid_mid_bp
        self._ask_mid_bp = ask_mid_bp
        self._overwrite_bid_ask = overwrite_bid_ask

    def calculate_benchmark(self, market_df=None, mid=None, bid=None,
                            ask=None,
                            bid_mid_bp=None, ask_mid_bp=None, overwrite_bid_ask=None):

        if self._check_empty_benchmark_market_data(market_df): return market_df

        if mid is None: mid = self._mid
        if bid is None: bid = self._bid
        if ask is None: ask = self._ask
        if bid_mid_bp is None: bid_mid_bp = self._bid_mid_bp
        if ask_mid_bp is None: ask_mid_bp = self._ask_mid_bp
        if overwrite_bid_ask is None: overwrite_bid_ask = self._overwrite_bid_ask

        bid_mid_bp = float(bid_mid_bp);
        ask_mid_bp = float(ask_mid_bp)

        # market_df_list = [market_df]

        if mid not in market_df.columns:
            market_df[mid] = (market_df[bid].values + market_df[ask].values)/2.0

        # Calculate the bid-mid and ask-mid spreads from market data
        if bid in market_df.columns and ask in market_df.columns and not (overwrite_bid_ask):
            # market_df[bid + '_' + mid + '_spread'] = (market_df[bid].values / market_df[mid].values) - 1.0
            # market_df[ask + '_' + mid + '_spread'] = (market_df[mid].values / market_df[ask].values) - 1.0
            market_df[bid + '_' + mid + '_spread'] = pd.eval('(market_df.bid / market_df.mid) - 1.0')
            market_df[ask + '_' + mid + '_spread'] = pd.eval('(market_df.mid / market_df.ask) - 1.0')

        # If we have been asked to overwrite bid/ask columns with an artificial proxy
        elif bid in market_df.columns and ask in market_df.columns and overwrite_bid_ask:
            # otherwise if we don't have sufficient bid/ask data (and only mid data), or if we want to forecibly overwrite it,
            # create a synthetic bid/ask and use the user specified spread
            market_df[bid + '_' + mid + '_spread'] = -bid_mid_bp / 10000.0
            market_df[ask + '_' + mid + '_spread'] = -ask_mid_bp / 10000.0
            market_df[bid] = (market_df[mid].values) * (1.0 - bid_mid_bp / 10000.0)
            market_df[ask] = (market_df[mid].values) / (1.0 - ask_mid_bp / 10000.0)
            # market_df[bid + '_' + mid + '_spread'] = pd.eval('-bid_mid_bp / 10000.0')
            # market_df[ask + '_' + mid + '_spread'] = pd.eval('-ask_mid_bp / 10000.0')
            # market_df[bid] = pd.eval('(market_df.mid) * (1.0 - bid_mid_bp / 10000.0)')
            # market_df[ask] = pd.eval('(market_df.mid) / (1.0 - ask_mid_bp / 10000.0)')


        # If we only have the mid column
        elif mid in market_df.columns and bid not in market_df.columns and ask not in market_df.columns:
            market_df[bid + '_' + mid + '_spread'] = -bid_mid_bp / 10000.0
            market_df[ask + '_' + mid + '_spread'] = -ask_mid_bp / 10000.0
            market_df[bid] = (market_df[mid].values) * (1.0 - bid_mid_bp / 10000.0)
            market_df[ask] = (market_df[mid].values) / (1.0 - ask_mid_bp / 10000.0)
            # market_df[bid + '_' + mid + '_spread'] = pd.eval('-bid_mid_bp / 10000.0')
            # market_df[ask + '_' + mid + '_spread'] = pd.eval('-ask_mid_bp / 10000.0')
            # market_df[bid] = pd.eval('(market_df.mid) * (1.0 - bid_mid_bp / 10000.0)')
            # market_df[ask] = pd.eval('(market_df.mid) / (1.0 - ask_mid_bp / 10000.0)')
        else:
            LoggerManager().getLogger(__name__).warning("Couldn't calculate spread from mid, check market data has appropriate fields.")

        return market_df

class BenchmarkMarketResampleOffset(BenchmarkMarket):
    """Resamples market data to the chosen frequency and/or offsets the time by a specified amount. Can be used to
    reduce data size or fix issues associated with misalignment of venue clocks.

    """
    def __init__(self, market_resample_freq=None, market_resample_unit='s', market_offset_ms=None, resample_how='last',
                 price_field=None, volume_field=None, dropna=False):

        super(BenchmarkMarketResampleOffset, self).__init__()

        self._market_resample_freq = market_resample_freq
        self._market_resample_unit = market_resample_unit
        self._market_offset_ms = market_offset_ms
        self._resample_how = resample_how

        self._price_field = price_field
        self._volume_field = volume_field

        self._dropna = dropna


    def calculate_benchmark(self, market_df=None, market_resample_freq=None,
                            market_resample_unit=None, market_offset_ms=None, resample_how=None,
                            price_field=None, volume_field=None, dropna=None):

        if self._check_empty_benchmark_market_data(market_df): return market_df

        if market_resample_freq is None: market_resample_freq = self._market_resample_freq
        if market_resample_unit is None: market_resample_unit = self._market_resample_unit
        if market_offset_ms is None: market_offset_ms = self._market_offset_ms
        if resample_how is None: resample_how = self._resample_how
        if price_field is None: price_field = self._price_field
        if volume_field is None: volume_field = self._volume_field
        if dropna is None: dropna = self._dropna

        if market_offset_ms is not None:
            market_df.index = market_df.index + timedelta(milliseconds=market_offset_ms)

        if market_resample_freq is not None and market_resample_unit is not None:
            if not(isinstance(resample_how, list)):
                resample_how = [resample_how]

            market_df_list = []

            for how in resample_how:
                market_df_list.append(self._time_series_ops.resample_time_series(market_df,
                    resample_amount=market_resample_freq, how=how, unit=market_resample_unit,
                    price_field=price_field, volume_field=volume_field))

            market_df = self._time_series_ops.outer_join(market_df_list)

        if dropna:
            market_df = market_df.dropna()

        return market_df

########################################################################################################################

class BenchmarkTrade(Benchmark):
    """These benchmarks are calculates for particular trades/orders, and cannot be simply computed on market data. Take
    for example TWAP. To compute this we need to know an order's particular start and finish time, as well as market data
    between those points. For an arrival price, we need to know the time of a trade.

    """
    def __init__(self, trade_order_list=None, **kwargs):
        # self.logger = LoggerManager().getLogger(__name__)
        self._trade_order_list = trade_order_list
        self._time_series_ops = Mediator.get_time_series_ops()

    @abc.abstractmethod
    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, field=None):
        pass

    def _add_benchmark_to_trade(self, trade_order_df, market_df, field=None):
        pass

    def _check_empty_benchmark_market_trade_data(self, trade_order_name, trade_order_df, market_df):
        """Should we calculate a benchmark for trade/orders (eg. sometimes we might only wish to calculate benchmarks
        for orders but not for trades).

        Parameters
        ----------
        trade_order_name : str (list)
            Which trades/orders should we compute these benchmarks for

        trade_order_df : DataFrame
            trade/order data

        market_df : DataFrame
            market data

        Returns
        -------
        bool
        """
        if trade_order_df is None: return True
        if trade_order_df.empty: return True
        if market_df is None: return True
        if market_df.empty: return True

        if trade_order_name is not None and self._trade_order_list is not None:
            if trade_order_name not in self._trade_order_list:
                return True

        return False


########################################################################################################################

class BenchmarkArrival(BenchmarkTrade):
    """For each trade/order DataFrame, finds the associated price associated with each trade arrival time in a market dataframe.
    Adds as an 'arrival' column in the trade/order DataFrame

    """

    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid', benchmark_post_fix='',
                 start_time_before_offset=None,
                 overwrite_time_of_day=None, overwrite_timezone=None):
        super(BenchmarkArrival, self).__init__(trade_order_list=trade_order_list)

        self._bid_benchmark = bid_benchmark
        self._ask_benchmark = ask_benchmark
        self._time_series_ops = TimeSeriesOps()
        self._benchmark_name = 'arrival' + benchmark_post_fix
        self._start_time_before_offset = start_time_before_offset
        self._overwrite_time_of_day = overwrite_time_of_day
        self._overwrite_timezone = overwrite_timezone

    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, bid_benchmark=None,
                            ask_benchmark=None, start_time_before_offset=None, overwrite_time_of_day=None, overtime_zone=None):
        if self._check_empty_benchmark_market_trade_data(trade_order_name, trade_order_df, market_df):
            return trade_order_df, market_df

        if bid_benchmark is None: bid_benchmark = self._bid_benchmark
        if ask_benchmark is None: ask_benchmark = self._ask_benchmark
        if start_time_before_offset is None: start_time_before_offset = self._start_time_before_offset
        if overwrite_time_of_day is None: overwrite_time_of_day = self._overwrite_time_of_day
        if overtime_zone is None: overtime_zone = self._overwrite_time_of_day

        if bid_benchmark in market_df.columns and ask_benchmark in market_df.columns:
            trade_order_df[self._benchmark_name] = np.nan

            # Deal with all the buy trades (ie. buying at the ask!)
            is_side = trade_order_df['side'] == 1
            side_dt = trade_order_df.index[is_side]

            if start_time_before_offset is not None:
                side_dt = side_dt - self._time_series_ops.get_time_delta(start_time_before_offset)

            if overwrite_time_of_day is not None:
                side_dt = self._time_series_ops.overwrite_time_of_day_in_datetimeindex(side_dt, overwrite_time_of_day,
                            old_tz=trade_order_df.index.tz, overwrite_timezone=overtime_zone)

            # TODO work on actual rather than copy
            benchmark, actual_dt = self._time_series_ops.vlookup_style_data_frame(side_dt, market_df, ask_benchmark)
            trade_order_df[self._benchmark_name][is_side] = benchmark

            # Now, do all the sell trades (ie. selling at the bid!)
            is_side = trade_order_df['side'] == -1
            side_dt = trade_order_df.index[is_side]

            # Offset time and then overwrite if specified by user
            if start_time_before_offset is not None:
                side_dt = side_dt - self._time_series_ops.get_time_delta(start_time_before_offset)

            if overwrite_time_of_day is not None:
                side_dt = self._time_series_ops.overwrite_time_of_day_in_datetimeindex(side_dt, overwrite_time_of_day,
                            old_tz=trade_order_df.index.tz, overwrite_timezone=overtime_zone)

            benchmark, actual_dt = self._time_series_ops.vlookup_style_data_frame(side_dt, market_df, bid_benchmark)
            trade_order_df[self._benchmark_name][is_side] = benchmark

            # # find the nearest price as arrival
            # series, dt = self._time_series_ops.vlookup_style_data_frame(market_trade_order_df.index, market_df, field)
            #
            # market_trade_order_df['arrival'] = series

        return trade_order_df, market_df

########################################################################################################################

class BenchmarkWeighted(BenchmarkTrade):
    """Calculates weighted benchmark price (based on any field) for individual orders or (trades over a window)

    """

    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid', weighting_field='weighted',
                 benchmark_date_start_field='benchmark_date_start',
                 benchmark_date_end_field='benchmark_date_end', benchmark_post_fix='',
                 start_time_before_offset=None, finish_time_after_offset=None,
                 overwrite_time_of_day=None, overwrite_timezone=None):

        super(BenchmarkWeighted, self).__init__(trade_order_list=trade_order_list)

        self._bid_benchmark = bid_benchmark
        self._ask_benchmark = ask_benchmark
        self._weighting_field = weighting_field
        self._benchmark_date_start_field = benchmark_date_start_field
        self._benchmark_date_end_field = benchmark_date_end_field
        self._benchmark_name = 'weighted' + benchmark_post_fix

        self._start_time_before_offset = start_time_before_offset
        self._finish_time_after_offset = finish_time_after_offset

        self._overwrite_time_of_day = overwrite_time_of_day
        self._overwrite_timezone = overwrite_timezone

    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, bid_benchmark=None,
                            ask_benchmark=None,
                            weighting_field=None,
                            benchmark_date_start_field=None,
                            benchmark_date_end_field=None, start_time_before_offset=None, finish_time_after_offset=None,
                            overwrite_time_of_day=None, overwrite_timezone=None):

        if self._check_empty_benchmark_market_trade_data(trade_order_name, trade_order_df, market_df):
            return trade_order_df, market_df

        # If fields have not been specified, then take them from the field variables
        if bid_benchmark is None: bid_benchmark = self._bid_benchmark
        if ask_benchmark is None: ask_benchmark = self._ask_benchmark
        if weighting_field is None: weighting_field = self._weighting_field
        if benchmark_date_start_field is None: benchmark_date_start_field = self._benchmark_date_start_field
        if benchmark_date_end_field is None: benchmark_date_end_field = self._benchmark_date_end_field
        if start_time_before_offset is None: start_time_before_offset = self._start_time_before_offset
        if finish_time_after_offset is None: finish_time_after_offset = self._finish_time_after_offset
        if overwrite_time_of_day is None: overwrite_time_of_day = self._overwrite_time_of_day
        if overwrite_timezone is None: overwrite_timezone = self._overwrite_timezone

        if weighting_field is not None:
            weighting_field_condition = True
        else:
            weighting_field_condition = weighting_field in market_df.columns

        if bid_benchmark in market_df.columns and ask_benchmark in market_df.columns and weighting_field_condition:
            trade_order_df[self._benchmark_name] = np.nan

            if benchmark_date_start_field is not None and benchmark_date_end_field is not None and \
                    benchmark_date_start_field in trade_order_df.columns and benchmark_date_end_field in trade_order_df.columns:
                date_start = trade_order_df[benchmark_date_start_field].values
                date_end = trade_order_df[benchmark_date_end_field].values
            else:
                date_start = trade_order_df.index.values
                date_end = trade_order_df.index.values

            # Overwrite every trade/order start/end time by a specific time of day if this has been specified
            if overwrite_time_of_day is not None and overwrite_timezone is not None:
                date_start = self._time_series_ops.overwrite_time_of_day_in_datetimeindex(date_start,
                                overwrite_time_of_day,
                                old_tz=trade_order_df.index.tz,
                                overwrite_timezone=overwrite_timezone)
                date_end = self._time_series_ops.overwrite_time_of_day_in_datetimeindex(date_end,
                                overwrite_time_of_day,
                                old_tz=trade_order_df.index.tz,
                                overwrite_timezone=overwrite_timezone)

            # Subtract a user defined time from the start time of the order (or point in time for a trade) if specifed
            if start_time_before_offset is not None:
                date_start = date_start - self._time_series_ops.get_time_delta(start_time_before_offset)

            # Add a user defined time from the finish time of the order (or point in time for a trade) if specified
            if finish_time_after_offset is not None:
                date_end = date_end + self._time_series_ops.get_time_delta(finish_time_after_offset)

            date_start = np.searchsorted(market_df.index, date_start)
            date_end = np.searchsorted(market_df.index, date_end)
            bid_price = market_df[bid_benchmark].values
            ask_price = market_df[ask_benchmark].values

            try:
                trade_order_df[self._benchmark_name] = \
                    self._benchmark_calculation(trade_order_df, bid_price, ask_price, date_start, date_end,
                                                weights=self._generate_weights(market_df, weighting_field=weighting_field))
            except:
                LoggerManager.getLogger(__name__).warning(
                    self._benchmark_name + " not calculated (check if has correct input fields)")

        else:
            LoggerManager.getLogger(__name__).warning(
                bid_benchmark + ", " + ask_benchmark + " " + weighting_field + " may not be in market data")

        return trade_order_df, market_df

    def _generate_weights(self, market_df, weighting_field=None):
        if weighting_field is not None and weighting_field in market_df.columns:
            return market_df[weighting_field].values

        return None

    def _benchmark_calculation(self, trade_order_df, bid_price, ask_price, date_start, date_end, weights=None):

        benchmark = []

        for i in range(0, len(trade_order_df.index)):
            # If the trade is a buy
            if trade_order_df['side'][i] == 1:
                price = ask_price

            # If the trade is a sell
            elif trade_order_df['side'][i] == -1:
                price = bid_price

            if date_start[i] == date_end[i]:
                benchmark.append(price[date_start[i]])
            else:
                try:
                    benchmark.append(
                        np.average(price[date_start[i]:date_end[i]], weights=weights[date_start[i]:date_end[i]]))
                except Exception as e:
                    err_msg = self._benchmark_name + " cannot be calculated, given market data does not fully overlap with trade data: " \
                              + str(e)

                    LoggerManager.getLogger(__name__).error(err_msg)

                    raise TradeMarketNonOverlapException(err_msg)

        return benchmark

########################################################################################################################

class BenchmarkVWAP(BenchmarkWeighted):
    """Calculates VWAP price for individual orders or (trades over a window)
    """

    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid', weighting_field='volume',
                 benchmark_date_start_field='benchmark_date_start',
                 benchmark_date_end_field='benchmark_date_end', benchmark_post_fix='',
                 start_time_before_offset=None, finish_time_after_offset=None,
                 overwrite_time_of_day=None, overwrite_timezone=None):

        super(BenchmarkVWAP, self).__init__(trade_order_list=trade_order_list,
                                            bid_benchmark=bid_benchmark, ask_benchmark=ask_benchmark,
                                            weighting_field=weighting_field,
                                            benchmark_date_start_field=benchmark_date_start_field,
                                            benchmark_date_end_field=benchmark_date_end_field,
                                            benchmark_post_fix=benchmark_post_fix,
                                            start_time_before_offset=start_time_before_offset, finish_time_after_offset=finish_time_after_offset,
                                            overwrite_time_of_day=overwrite_time_of_day, overwrite_timezone=overwrite_timezone)

        self._benchmark_name = 'vwap' + benchmark_post_fix

########################################################################################################################

class BenchmarkTWAP(BenchmarkWeighted):
    """Calculates TWAP price for individual orders or (trades over a window)
    """

    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid',
                 benchmark_date_start_field='benchmark_date_start',
                 benchmark_date_end_field='benchmark_date_end', benchmark_post_fix='',
                 start_time_before_offset=None, finish_time_after_offset=None,
                 overwrite_time_of_day=None, overwrite_timezone=None):

        super(BenchmarkTWAP, self).__init__(trade_order_list=trade_order_list,
                                            bid_benchmark=bid_benchmark, ask_benchmark=ask_benchmark,
                                            benchmark_date_start_field=benchmark_date_start_field,
                                            benchmark_date_end_field=benchmark_date_end_field,
                                            benchmark_post_fix=benchmark_post_fix,
                                            start_time_before_offset=start_time_before_offset, finish_time_after_offset=finish_time_after_offset,
                                            overwrite_time_of_day=overwrite_time_of_day, overwrite_timezone=overwrite_timezone)

        self._benchmark_name = 'twap' + benchmark_post_fix

    def _generate_weights(self, market_df, weighting_field=None):
        weights = market_df.index.tz_convert(None).to_series().diff().values / np.timedelta64(1, 's')

        if len(weights) == 0:
            weights = np.array(1)
        else:
            weights[0] = 0  # first point should be weighted zero (since don't know how long it's been there)

        return weights

########################################################################################################################

class BenchmarkMedian(BenchmarkWeighted):
    """Calculates median price for individual orders or (trades over a window)
    """
    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid',
                 benchmark_date_start_field='benchmark_date_start',
                 benchmark_date_end_field='benchmark_date_end', benchmark_post_fix='',
                 start_time_before_offset=None, finish_time_after_offset=None,
                 overwrite_time_of_day=None, overwrite_timezone=None):

        super(BenchmarkMedian, self).__init__(trade_order_list=trade_order_list,
                                            bid_benchmark=bid_benchmark, ask_benchmark=ask_benchmark,
                                            benchmark_date_start_field=benchmark_date_start_field,
                                            benchmark_date_end_field=benchmark_date_end_field,
                                            benchmark_post_fix=benchmark_post_fix,
                                            start_time_before_offset=start_time_before_offset, finish_time_after_offset=finish_time_after_offset,
                                            overwrite_time_of_day=overwrite_time_of_day, overwrite_timezone=overwrite_timezone)

        self._benchmark_name = 'median' + benchmark_post_fix

    def _benchmark_calculation(self, trade_order_df, bid_price, ask_price, date_start, date_end, weights=None):

        benchmark = []

        for i in range(0, len(trade_order_df.index)):
            # If the trade is a buy
            if trade_order_df['side'][i] == 1:
                price = ask_price

            # If the trade is a sell
            elif trade_order_df['side'][i] == -1:
                price = bid_price

            if date_start[i] == date_end[i]:
                benchmark.append(price[date_start[i]])
            else:
                try:
                    benchmark.append(self._get_price(price[date_start[i]:date_end[i]], side=trade_order_df['side'][i]))
                except Exception as e:
                    err_msg = self._benchmark_name + " cannot be calculated, given market data does not fully overlap with trade data: " \
                              + str(e)

                    LoggerManager.getLogger(__name__).error(err_msg)

                    raise TradeMarketNonOverlapException(err_msg)

        return benchmark

    def _get_price(self, price, side=None):
        return np.median(price)

    def _generate_weights(self, market_df, weighting_field=None):
        return None

########################################################################################################################

class BenchmarkBest(BenchmarkMedian):
    """Calculates the benchmark as the best price during an order (or within a window), ie. for a buy trade, it'll take
    the lowest price during the window, for a sell trade it'll take the highest price during the window
    """

    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid',
                 benchmark_date_start_field='benchmark_date_start',
                 benchmark_date_end_field='benchmark_date_end', benchmark_post_fix='',
                 start_time_before_offset=None, finish_time_after_offset=None,
                 overwrite_time_of_day=None, overwrite_timezone=None):

        super(BenchmarkBest, self).__init__(trade_order_list=trade_order_list,
                                            bid_benchmark=bid_benchmark, ask_benchmark=ask_benchmark,
                                            benchmark_date_start_field=benchmark_date_start_field,
                                            benchmark_date_end_field=benchmark_date_end_field,
                                            benchmark_post_fix=benchmark_post_fix,
                                            start_time_before_offset=start_time_before_offset, finish_time_after_offset=finish_time_after_offset,
                                            overwrite_time_of_day=overwrite_time_of_day, overwrite_timezone=overwrite_timezone)

        self._benchmark_name = 'best' + benchmark_post_fix

    def _get_price(self, price, side=None):
        if side == 1:
            return np.min(price)
        elif side == -1:
            return np.max(price)


########################################################################################################################

class BenchmarkWorst(BenchmarkMedian):
    """Calculates the benchmark as the worst price during an order (or within a window), ie. for a buy trade, it'll take
    the highest price during the window, for a sell trade it'll take the lowest price during the window
    """

    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid',
                 benchmark_date_start_field='benchmark_date_start',
                 benchmark_date_end_field='benchmark_date_end', benchmark_post_fix='',
                 start_time_before_offset=None, finish_time_after_offset=None,
                 overwrite_time_of_day=None, overwrite_timezone=None):

        super(BenchmarkWorst, self).__init__(trade_order_list=trade_order_list,
                                            bid_benchmark=bid_benchmark, ask_benchmark=ask_benchmark,
                                            benchmark_date_start_field=benchmark_date_start_field,
                                            benchmark_date_end_field=benchmark_date_end_field,
                                            benchmark_post_fix=benchmark_post_fix,
                                            start_time_before_offset=start_time_before_offset, finish_time_after_offset=finish_time_after_offset,
                                            overwrite_time_of_day=overwrite_time_of_day, overwrite_timezone=overwrite_timezone)

        self._benchmark_name = 'best' + benchmark_post_fix

    def _get_price(self, price, side=None):
        if side == 1:
            return np.max(price)
        elif side == -1:
            return np.min(price)

########################################################################################################################

class BenchmarkTradeOffset(BenchmarkTrade):
    """Offsets the time of trades by a certain number of milliseconds

    """
    def __init__(self, trade_order_list=None, trade_offset_ms=None, date_columns=constants.date_columns):

        super(BenchmarkTradeOffset, self).__init__(trade_order_list=trade_order_list)

        self._trade_offset_ms = trade_offset_ms
        self._date_columns = date_columns

    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, trade_offset_ms=None, date_columns=None):

        if trade_offset_ms is None: trade_offset_ms = self._trade_offset_ms
        if date_columns is None: date_columns = self._date_columns

        trade_order_df.index = trade_order_df.index + timedelta(milliseconds=trade_offset_ms)

        for d in date_columns:
            if d in trade_order_df.columns:
                trade_order_df[d] = trade_order_df[d] + timedelta(milliseconds=trade_offset_ms)

        return trade_order_df, market_df






