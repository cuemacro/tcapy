from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import numpy as np
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
    that benchmark (typically will be calculated by trade)

    """

    def __init__(self, trade_order_list=None, **kwargs):
        # self.logger = LoggerManager().getLogger(__name__)
        self._trade_order_list = trade_order_list
        self._time_series_ops = Mediator.get_time_series_ops()

    def _check_calculate_benchmark(self, trade_order_name):
        """Should we calculate a benchmark for trade/orders (eg. sometimes we might only wish to calculate benchmarks
        for orders but not for trades).

        Parameters
        ----------
        trade_order_name : str (list)
            Which trades/orders should we compute these benchmarks for

        Returns
        -------
        bool
        """
        if trade_order_name is not None and self._trade_order_list is not None:
            if trade_order_name not in self._trade_order_list:
                return False

        return True

    @abc.abstractmethod
    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, field=None):
        pass

    def _add_benchmark_to_trade(self, trade_order_df, market_df, field=None):
        pass

########################################################################################################################

class BenchmarkMarket(Benchmark):
    """These benchmarks can be calculated directly on market data (without the need for trade data). For example,
    we might want to calculate the mid price, in market data, we only have the bid/ask data. Alternatively, we might wish
    to calculate the spread to mid of bid/ask market data.

    """
    pass


class BenchmarkMid(BenchmarkMarket):
    """Calculates the mid-point for every time point of market data from the underlying bid/ask prices.

    """
    def __init__(self, trade_order_list=None, field='mid', bid='bid', ask='ask'):

        super(BenchmarkMid, self).__init__(trade_order_list=trade_order_list)

        self._field = field
        self._bid = bid
        self._ask = ask


    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, field=None, bid=None,
                            ask=None):
        # if not (self._check_calculate_benchmark(trade_order_name=trade_order_name)): return trade_order_df, market_df

        if field is None: field = self._field
        if bid is None: bid = self._bid
        if ask is None: ask = self._ask

        # If the 'mid' price does not already exist in the market data, calculate it from the underlying bid/ask prices
        if field not in market_df.columns:
            # if field == 'bid/ask':
            market_df[field] = (market_df[bid].values + market_df[ask].values) / 2.0
        else:
            LoggerManager().getLogger(__name__).warn(field + " not in market data")

        return trade_order_df, market_df

class BenchmarkSpreadToMid(BenchmarkMarket):
    """Calculates the spread for each mid point

    """
    def __init__(self, trade_order_list=None, mid='mid', bid='bid', ask='ask', bid_mid_bp=1, ask_mid_bp=1,
                 overwrite_bid_ask=False):

        super(BenchmarkSpreadToMid, self).__init__(trade_order_list=trade_order_list)

        self._mid = mid
        self._bid = bid
        self._ask = ask

        self._bid_mid_bp = bid_mid_bp
        self._ask_mid_bp = ask_mid_bp
        self._overwrite_bid_ask = overwrite_bid_ask

    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, mid=None, bid=None,
                            ask=None,
                            bid_mid_bp=None, ask_mid_bp=None, overwrite_bid_ask=None):
        # if not (self._check_calculate_benchmark(trade_order_name=trade_order_name)): return trade_order_df

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
            market_df[bid + '_' + mid + '_spread'] = (market_df[bid].values / market_df[mid].values) - 1.0
            market_df[ask + '_' + mid + '_spread'] = (market_df[mid].values / market_df[ask].values) - 1.0

        # If we have been asked to overwrite bid/ask columns with an artificial proxy
        elif bid in market_df.columns and ask in market_df.columns and overwrite_bid_ask:
            # otherwise if we don't have sufficient bid/ask data (and only mid data), or if we want to forecibly overwrite it,
            # create a synthetic bid/ask and use the user specified spread
            market_df[bid + '_' + mid + '_spread'] = -bid_mid_bp / 10000.0
            market_df[ask + '_' + mid + '_spread'] = -ask_mid_bp / 10000.0
            market_df[bid] = (market_df[mid].values) * (1.0 - bid_mid_bp / 10000.0)
            market_df[ask] = (market_df[mid].values) / (1.0 - ask_mid_bp / 10000.0)

        # If we only have the mid column
        elif mid in market_df.columns and bid not in market_df.columns and ask not in market_df.columns:
            market_df[bid + '_' + mid + '_spread'] = -bid_mid_bp / 10000.0
            market_df[ask + '_' + mid + '_spread'] = -ask_mid_bp / 10000.0
            market_df[bid] = (market_df[mid].values) * (1.0 - bid_mid_bp / 10000.0)
            market_df[ask] = (market_df[mid].values) / (1.0 - ask_mid_bp / 10000.0)
        else:
            LoggerManager().getLogger(__name__).warn("Couldn't calculate spread from mid, check market data has appropriate fields.")

        return trade_order_df, market_df

class BenchmarkResampleOffset(BenchmarkMarket):
    """Resamples market data to the chosen frequency and/or offsets the time by a specified amount. Can be used to
    reduce data size or fix issues associated with misalignment of venue clocks.

    """
    def __init__(self, trade_order_list=None, market_resample_freq=None, market_resample_unit='s', market_offset_ms=None, resample_how='last'):

        super(BenchmarkMid, self).__init__(trade_order_list=trade_order_list)

        self._market_resample_freq = market_resample_freq
        self._market_resample_unit = market_resample_unit
        self._market_offset_ms = market_offset_ms
        self._resample_how = resample_how


    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, market_resample_freq=None,
                            market_resample_unit=None, market_offset_ms=None, resample_how=None):

        # if not (self._check_calculate_benchmark(trade_order_name=trade_order_name)): return trade_order_df, market_df

        if market_resample_freq is None: market_resample_freq = self._market_resample_freq
        if market_resample_unit is None: market_resample_unit = self._market_resample_unit
        if market_offset_ms is None: market_offset_ms = self._market_offset_ms
        if resample_how is None: resample_how = self._resample_how

        if market_offset_ms is not None:
            market_df.index = market_df.index + timedelta(milliseconds=market_offset_ms)

        if market_resample_freq is not None and market_resample_unit is not None:
            market_df = Mediator.get_time_series_ops().resample_time_series(market_df,
                resample_amount=market_resample_freq, how=resample_how, unit=market_resample_unit)

        return trade_order_df, market_df

########################################################################################################################

class BenchmarkTrade(Benchmark):
    """These benchmarks are calculates for particular trades/orders, and cannot be simply computed on market data. Take
    for example TWAP. To compute this we need to know an order's particular start and finish time, as well as market data
    between those points. For an arrival price, we need to know the time of a trade.

    """
    pass

########################################################################################################################

class BenchmarkArrival(BenchmarkTrade):
    """For each trade dataframe, find the associated price associated with each trade arrival time in a market dataframe.
    Add as an 'arrival' column in the trade dataframe

    """

    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid', benchmark_post_fix=''):
        super(BenchmarkArrival, self).__init__(trade_order_list=trade_order_list)

        self._bid_benchmark = bid_benchmark
        self._ask_benchmark = ask_benchmark
        self._time_series_ops = TimeSeriesOps()
        self._benchmark_name = 'arrival' + benchmark_post_fix

    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, bid_benchmark=None,
                            ask_benchmark=None):
        if not (self._check_calculate_benchmark(trade_order_name=trade_order_name)): return trade_order_df, market_df

        if bid_benchmark is None: bid_benchmark = self._bid_benchmark
        if ask_benchmark is None: ask_benchmark = self._ask_benchmark

        if bid_benchmark in market_df.columns and ask_benchmark in market_df.columns:
            trade_order_df[self._benchmark_name] = np.nan

            # Deal with all the buy trades (ie. buying at the ask!)
            is_side = trade_order_df['side'] == 1
            side_dt = trade_order_df.index[is_side]

            # TODO work on actual rather than copy
            benchmark, actual_dt = self._time_series_ops.vlookup_style_data_frame(side_dt, market_df, ask_benchmark)
            trade_order_df[self._benchmark_name][is_side] = benchmark

            # Now, do all the sell trades (ie. selling at the bid!)
            is_side = trade_order_df['side'] == -1
            side_dt = trade_order_df.index[is_side]

            benchmark, actual_dt = self._time_series_ops.vlookup_style_data_frame(side_dt, market_df, bid_benchmark)
            trade_order_df[self._benchmark_name][is_side] = benchmark

            # # find the nearest price as arrival
            # series, dt = self.time_series_ops.vlookup_style_data_frame(trade_order_df.index, market_df, field)
            #
            # trade_order_df['arrival'] = series

        return trade_order_df, market_df

########################################################################################################################

class BenchmarkVWAP(BenchmarkTrade):
    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid', volume_field='volume',
                 benchmark_date_start_field='benchmark_date_start',
                 benchmark_date_end_field='benchmark_date_end', benchmark_post_fix=''):

        super(BenchmarkVWAP, self).__init__(trade_order_list=trade_order_list)

        self._bid_benchmark = bid_benchmark
        self._ask_benchmark = ask_benchmark
        self._volume_field = volume_field
        self._benchmark_date_start_field = benchmark_date_start_field
        self._benchmark_date_end_field = benchmark_date_end_field
        self._benchmark_name = 'vwap' + benchmark_post_fix

    """Calculates VWAP (volume weighted average price) for individual orders.

    """

    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, bid_benchmark=None,
                            ask_benchmark=None,
                            volume_field=None,
                            benchmark_date_start_field=None,
                            benchmark_date_end_field=None):

        if not (self._check_calculate_benchmark(trade_order_name=trade_order_name)): return trade_order_df, market_df

        # if fields have not been specified, then take them from the field variables
        if bid_benchmark is None: bid_benchmark = self._bid_benchmark
        if ask_benchmark is None: ask_benchmark = self._ask_benchmark
        if volume_field is None: volume_field = self._volume_field
        if benchmark_date_start_field is None: benchmark_date_start_field = self._benchmark_date_start_field
        if benchmark_date_end_field is None: benchmark_date_end_field = self._benchmark_date_end_field

        if bid_benchmark in market_df.columns and ask_benchmark in market_df.columns and volume_field in market_df.columns:
            trade_order_df[self._benchmark_name] = np.nan

            date_start = trade_order_df[benchmark_date_start_field].values
            date_end = trade_order_df[benchmark_date_end_field].values

            date_start = np.searchsorted(market_df.index, date_start)
            date_end = np.searchsorted(market_df.index, date_end)
            bid_price = market_df[bid_benchmark].values
            ask_price = market_df[ask_benchmark].values
            volume = market_df[volume_field].values

            vwap = []

            for i in range(0, len(trade_order_df.index)):
                if trade_order_df['side'][i] == 1:
                    price = ask_price
                elif trade_order_df['side'][i] == -1:
                    price = bid_price

                if date_start[i] == date_end[i]:
                    vwap.append(price[date_start[i]])
                else:
                    try:
                        vwap.append(
                            np.average(price[date_start[i]:date_end[i]], weights=volume[date_start[i]:date_end[i]]))
                    except Exception as e:
                        err_msg = "VWAP cannot be calculated, given market data does not fully overlap with trade data: " \
                                  + str(e)

                        LoggerManager.getLogger(__name__).error(err_msg)

                        raise TradeMarketNonOverlapException(err_msg)

            trade_order_df[self._benchmark_name] = vwap
        else:
            LoggerManager.getLogger(__name__).warn(
                bid_benchmark + ", " + ask_benchmark + " " + volume_field + " may not be in market data")

        return trade_order_df, market_df

########################################################################################################################

class BenchmarkTWAP(BenchmarkTrade):
    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid',
                 benchmark_date_start_field='benchmark_date_start',
                 benchmark_date_end_field='benchmark_date_end', benchmark_post_fix=''):

        super(BenchmarkTWAP, self).__init__(trade_order_list=trade_order_list)

        self._bid_benchmark = bid_benchmark
        self._ask_benchmark = ask_benchmark
        self._benchmark_date_start_field = benchmark_date_start_field
        self._benchmark_date_end_field = benchmark_date_end_field
        self._benchmark_name = 'twap' + benchmark_post_fix

    """Calculates TWAP (time weighted average price) for individual orders.

    """

    def calculate_benchmark(self, trade_order_df=None, market_df=None, trade_order_name=None, bid_benchmark=None,
                            ask_benchmark=None,
                            benchmark_date_start_field=None,
                            benchmark_date_end_field=None):
        if not (self._check_calculate_benchmark(trade_order_name=trade_order_name)): return trade_order_df, market_df

        # for the specified field (usually 'mid' field) calculate the time weighted average price, which is the simple
        # average
        if bid_benchmark is None: bid_benchmark = self._bid_benchmark
        if ask_benchmark is None: ask_benchmark = self._ask_benchmark
        if benchmark_date_start_field is None: benchmark_date_start_field = self._benchmark_date_start_field
        if benchmark_date_end_field is None: benchmark_date_end_field = self._benchmark_date_end_field

        if bid_benchmark in market_df.columns and ask_benchmark in market_df:
            trade_order_df[self._benchmark_name] = np.nan

            date_start = trade_order_df[benchmark_date_start_field].values
            date_end = trade_order_df[benchmark_date_end_field].values

            date_start = np.searchsorted(market_df.index, date_start)
            date_end = np.searchsorted(market_df.index, date_end)
            bid_price = market_df[bid_benchmark].values
            ask_price = market_df[ask_benchmark].values
            dt = market_df.index.to_series().diff().values / np.timedelta64(1, 's')
            dt[0] = 0  # first point should be weighted zero (since don't know how long it's been there)

            twap = []

            for i in range(0, len(trade_order_df.index)):

                if trade_order_df['side'][i] == 1:
                    price = ask_price
                elif trade_order_df['side'][i] == -1:
                    price = bid_price

                try:
                    if date_start[i] == date_end[i]:
                        twap.append(price[date_start[i]])
                    else:
                        twap_val = np.average(price[date_start[i]:date_end[i]], weights=dt[date_start[i]:date_end[i]])

                        twap.append(twap_val)
                except Exception as e:
                    err_msg = "TWAP cannot be calculated, given market data does not fully overlap with trade data: " \
                              + str(e)

                    LoggerManager.getLogger(__name__).error(err_msg)

                    raise TradeMarketNonOverlapException(err_msg)

            trade_order_df[self._benchmark_name] = twap
        else:
            LoggerManager.getLogger(__name__).warn(bid_benchmark + " and " + ask_benchmark + " may not be in market data.")

        return trade_order_df, market_df

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





