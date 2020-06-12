from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc
from datetime import timedelta

import numpy as np
import pandas as pd

from tcapy.util.mediator import Mediator
from tcapy.conf.constants import Constants

from tcapy.util.loggermanager import LoggerManager

# Compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

# from tcapy.util.loggermanager import LoggerManager

constants = Constants()

########################################################################################################################

class Metric(ABC):
    """Abstract class for the calculation of transaction cost metrics. It is implemented by classes such as MetricSlippage
    (for calculating slippage between mid and executed price) and MetricMarketImpact (for market impact between arrival
    and a predefined time in the future)

    """

    def __init__(self, trade_order_list=None):
        # self.logger = LoggerManager().getLogger(__name__)
        if not(isinstance(trade_order_list, list)) and trade_order_list is not None:
            trade_order_list = [trade_order_list]

        self._trade_order_list = trade_order_list
        self._time_series_ops = Mediator.get_time_series_ops()

    def set_params(self, trade_order_list=None):
        if not(isinstance(trade_order_list, list)) and trade_order_list is not None:
            trade_order_list = [trade_order_list]

        self._trade_order_list = trade_order_list

    @abc.abstractmethod
    def calculate_metric(self, trade_order_df=None, market_df=None, trade_order_name=None):
        pass

    def _get_benchmark_field(self, market_df, benchmark):

        if benchmark in market_df.columns:
            return benchmark

        # If we ask for bid or ask (but they don't exist) return 'mid'
        if benchmark == 'bid' or benchmark == 'ask':
            # default to returning the 'mid'
            return 'mid'

        # Otherwise the benchmark has been precalculated in the trade order?
        return benchmark

    def _check_calculate_metric(self, trade_order_name):
        if trade_order_name is not None and self._trade_order_list is not None:
            if trade_order_name not in self._trade_order_list:
                return False

        return True

    def get_metric_name(self):
        return self._metric_name

    def _combine_metric_trade(self, metric_df, trade_df, join_on='id'):
        """ Converts metrics into Joins together metric and trade dataframes, and assigns them to field variables, before returning

        Parameters
        ----------
        metric_df : DataFrame
            Contains calculates trade metrics

        trade_df : DataFrame
            A trade blotter, contains internal trades

        Returns
        -------
        pd.DataFrame tuple with metrics + trades and metrics in isolation
        """

        # metric_trade_df = trade_df.join(metric_df, on=join_on, how='outer')

        if not(metric_df.index.equals(trade_df.index)):
            Exception("Indices are not equal")

        if join_on in metric_df.columns:
            metric_df = metric_df.drop(join_on, axis=1)

        metric_trade_df = pd.concat([trade_df, metric_df], axis=1, sort=False)

        return metric_trade_df, metric_df

    def _get_benchmark_time_points(self, trade_df, market_df, metric_df, is_side, benchmark_label, output_label,
                                   timedelta_amount=None, just_before_point=False):
        """Gets all the trade times and searches market data for those points, and returns the nearest market data to
        those points. Analogous to doing a VLOOKUP on market data, using trade data as an input.

        Parameters
        ----------
        trade_df : DataFrame
            Contains all the trade executions

        market_df : DataFrame
             Market data

        metric_df : DataFrame
            To be filled with benchmark time points

        is_side : bool mask
            Filter only these trades

        benchmark_label : str
            This field will be read from market_df

        output_label : str
            Field to be output in metric_df

        timedelta_amount : TimeDelta (optional)
            Amount to shift our specified time points

        just_before_point : bool (default False)
            Should we get the point just before our time point

        Returns
        -------
        DataFrame with market prices corresponding to trades times
        """

        side_dt = trade_df.index[is_side]

        if (isinstance(benchmark_label, str)): benchmark_label = [benchmark_label]
        if (isinstance(output_label, str)): output_label = [output_label]

        for i in range(0, len(benchmark_label)):
            benchmark_label_ = benchmark_label[i];
            output_label_ = output_label[i]

            dt = None

            # First check if the benchmark label is in the market data (eg. for bid/mid/ask), in which grab from there
            if benchmark_label_ in market_df.columns:

                benchmark, dt = self._time_series_ops.vlookup_style_data_frame(side_dt, market_df, benchmark_label_,
                                                                              timedelta_amount=timedelta_amount,
                                                                              just_before_point=just_before_point)

            # Otherwise it could be a column in the trade data (eg. VWAP, TWAP etc. which need to calculated for each
            # trade individually beforehand)
            else:
                benchmark = trade_df[benchmark_label_]

            # metric_df.loc[output_label_, is_side] = benchmark
            metric_df[output_label_][is_side] = benchmark

            # mark time of "benchmark" (for slippage and market impact points)
            if output_label_ + '_time' in metric_df.columns:
                if dt is not None:
                    metric_df[output_label_ + '_time'][is_side] = pd.Series(index=benchmark.index, data=dt)

            # metric_df[output_label] = pd.Series(index=trade_df.index[is_side.values], data=benchmark.values)
        # TODO metric_df[output_label + '_time'][is_side] = pd.Series(index=is_side.index, data=pd.to_datetime(actual_dt))

        return metric_df

########################################################################################################################

class MetricCalc(Metric):
    """Metrics which can typically summarise the execution into a single number, such as slippage, market impact,
    implementation shortfall etc.
    """
    pass

########################################################################################################################

class MetricSlippage(MetricCalc):
    """Calculates the slippage for each trade/order, when given appropriate market data. Users can specify the reference
    field (typically the mid price) and also which field to use the price (typically the executed price of the trade/order).
    If spread data is supplied, as part of the market data, it will also flag trades/orders which have been executed from
    outside the spread.

    """

    def __init__(self, trade_order_list=None, executed_price='executed_price', bid_benchmark='mid',
                 ask_benchmark='mid', bid_mid_spread='bid_mid_spread', ask_mid_spread='ask_mid_spread',
                 metric_post_fix=''):
        super(MetricSlippage, self).__init__(trade_order_list=trade_order_list)

        self._executed_price = executed_price  # field for executed price of trade
        self._bid_benchmark = bid_benchmark  # field for market data which is assumed to be the bid
        self._ask_benchmark = ask_benchmark  # field for market data which is assumed to be the ask
        self._bid_mid_spread = bid_mid_spread  # size of the spread between bid and mid
        self._ask_mid_spread = ask_mid_spread  # size of the spread between ask and mid

        self._metric_name = 'slippage' + metric_post_fix

    def calculate_metric(self, trade_order_df=None, market_df=None, trade_order_name=None, executed_price=None,
                         mid_benchmark=None, bid_benchmark=None, ask_benchmark=None, bid_mid_spread=None,
                         ask_mid_spread=None):
        """Calculates the difference between the execution and the benchmark as a percentage, so that it is comparable
        across many assets. Also records the market prices used in the slippage calculation. We calculate slippage such
        that a positive number implies we have made money on a trade, whilst a negative number always implies a cost to
        each trade. If spread information is supplied, will also flag anomalous trades, ie. those traded outside the spread.

        Parameters
        ----------
        trade_order_df : DataFrame
            Trade executions from internal database

        market_df : DataFrame
            Market data collected from external price streams

        bid_benchmark : str
            Field to use for calculating slippage for bid (ie. for sell trades)

        ask_benchmark : str
            Field to use for calculating slippage for ask (ie. for buy trades)

        bid_mid_spread : str
            Field which contains the spread between the mid and bid (where negative implies bid is below the mid)

        ask_mid_spread : str
            Field which contains the spread between the mid and ask (where negative implied ask is above the mid)

        Returns
        -------
        DataFrame with slippage calculations
        """

        # from tcapy.util.loggermanager import LoggerManager

        # logger = LoggerManager().getLogger(__name__)
        if not (self._check_calculate_metric(trade_order_name)): return trade_order_df, None

        if executed_price is None: executed_price = self._executed_price
        if bid_benchmark is None: bid_benchmark = self._bid_benchmark
        if ask_benchmark is None: ask_benchmark = self._ask_benchmark
        if bid_mid_spread is None: bid_mid_spread = self._bid_mid_spread
        if ask_mid_spread is None: ask_mid_spread = self._ask_mid_spread

        # Get fields for benchmarks for bid/ask
        # Note: if these don't exist in the market data, we by default use the 'mid' field
        bid_benchmark = self._get_benchmark_field(market_df, bid_benchmark)
        ask_benchmark = self._get_benchmark_field(market_df, ask_benchmark)

        metric_df = pd.DataFrame(index=trade_order_df.index)
        metric_df[executed_price] = trade_order_df[executed_price]
        metric_df['id'] = trade_order_df['id']

        is_sell = trade_order_df['side'] == -1
        is_buy = trade_order_df['side'] == 1

        metric_df[self._metric_name] = np.nan
        metric_df[self._metric_name + '_benchmark'] = np.nan
        metric_df[self._metric_name + '_benchmark_time'] = trade_order_df.index

        bid_benchmark_list = [bid_benchmark];
        ask_benchmark_list = [ask_benchmark]
        metric_list = [self._metric_name + '_benchmark']

        # Add the bid/mid spreads if they exist
        if bid_mid_spread is not None and ask_mid_spread is not None:
            if bid_mid_spread in market_df.columns and ask_mid_spread in market_df.columns:
                bid_benchmark_list.append(bid_mid_spread)
                ask_benchmark_list.append(ask_mid_spread)

                metric_list.append('spread_to_benchmark')

                metric_df['spread_to_benchmark'] = np.nan

        metric_df = self._get_benchmark_time_points(trade_order_df, market_df, metric_df, is_sell, bid_benchmark_list,
                                                    metric_list, just_before_point=True)

        metric_df = self._get_benchmark_time_points(trade_order_df, market_df, metric_df, is_buy, ask_benchmark_list,
                                                    metric_list, just_before_point=True)

        # Make sign consistent for buys and sells for slippage (or negative is always a cost for the client)
        metric_df[self._metric_name] = np.multiply(
            ((metric_df[executed_price].values / metric_df[self._metric_name + '_benchmark'].values) - 1.0),
            -trade_order_df['side'].values)

        metric_df[self._metric_name + '_anomalous'] = np.nan

        metric_list.append(self._metric_name);
        metric_list.append('id')

        # Flag anomalous trades by slippage (compared to the spread to benchmark)
        if 'spread_to_benchmark' in metric_df.columns:
            metric_df[self._metric_name + '_anomalous'] = np.where(
                metric_df[self._metric_name] < metric_df['spread_to_benchmark'], 1, 0)
            metric_list.append(self._metric_name + '_anomalous')

        # metric_trade_df = pd.merge(market_trade_order_df, metric_df[metric_list],
        #                                how='left', left_index=True, on=['id'])

        metric_trade_df, metric_df = self._combine_metric_trade(metric_df[metric_list], trade_order_df)

        # metric_df = metric_df[self._metric_name]

        return metric_trade_df, metric_df

    def get_metric_name(self):
        return self._metric_name

########################################################################################################################

class MetricImpShortfall(MetricSlippage):
    def __init__(self, trade_order_list=None, executed_price='executed_price',
                 bid_benchmark='arrival', ask_benchmark='arrival',
                 bid_mid_spread='bid_mid_spread', ask_mid_spread='ask_mid_spread', metric_post_fix=''):
        super(MetricImpShortfall, self).__init__(trade_order_list=trade_order_list, executed_price=executed_price,
                                                 bid_benchmark=bid_benchmark, ask_benchmark=ask_benchmark,
                                                 bid_mid_spread=bid_mid_spread, ask_mid_spread=ask_mid_spread)

        self._metric_name = 'impshortfall' + metric_post_fix

########################################################################################################################

class MetricMarketImpact(MetricCalc):
    """Calculates the market impact of a trade (ie. the price moves following it). Similar to markout style calculation,
    but only calculates a single point.

    """

    def __init__(self, trade_order_list=None, executed_price='executed_price', bid_benchmark='bid',
                 ask_benchmark='ask', market_impact_multipler=constants.market_impact_multiplier):
        super(MetricMarketImpact, self).__init__(trade_order_list=trade_order_list)

        self._executed_price = executed_price
        self._bid_benchmark = bid_benchmark
        self._ask_benchmark = ask_benchmark
        self._market_impact_multiplier = market_impact_multipler

    def calculate_metric(self, trade_order_df=None, market_df=None, trade_order_name=None, executed_price=None,
                         bid_benchmark=None, ask_benchmark=None, market_impact_multiplier=None):
        if not (self._check_calculate_metric(trade_order_name)): return trade_order_df, None

        if executed_price is None: executed_price = self._executed_price
        if bid_benchmark is None: bid_benchmark = self._bid_benchmark
        if ask_benchmark is None: ask_benchmark = self._ask_benchmark
        if market_impact_multiplier is None: market_impact_multiplier = self._market_impact_multiplier

        # get fields for benchmarks for bid/ask
        # note: if these don't exist in the market data, we by default use the 'mid' field
        bid_benchmark = self._get_benchmark_field(market_df, bid_benchmark)
        ask_benchmark = self._get_benchmark_field(market_df, ask_benchmark)

        # strip down market data as much as possible (this is a massive dataset) to only the benchmark fields
        if bid_benchmark == ask_benchmark:
            market_df = pd.DataFrame(market_df[[bid_benchmark]])
        else:
            market_df = pd.DataFrame(market_df[[bid_benchmark, ask_benchmark]])

        is_sell = trade_order_df['side'] == -1
        is_buy = trade_order_df['side'] == 1

        metric_df = pd.DataFrame(index=trade_order_df.index)
        metric_df['id'] = trade_order_df['id']

        # transient market impact
        metric_df[self._metric_name + '_benchmark'] = np.nan
        metric_df[self._metric_name + '_benchmark_time'] = metric_df.index

        metric_df[executed_price] = trade_order_df[executed_price]

        # Market impact
        time_delta = self._time_series_ops.get_time_delta(self._market_impact_gap)
        metric_df = self._get_benchmark_time_points(trade_order_df, market_df, metric_df, is_sell, bid_benchmark,
                                                    self._metric_name + '_benchmark', timedelta_amount=time_delta,
                                                    just_before_point=False)

        metric_df = self._get_benchmark_time_points(trade_order_df, market_df, metric_df, is_buy, ask_benchmark,
                                                    self._metric_name + '_benchmark', timedelta_amount=time_delta,
                                                    just_before_point=False)

        # Make sign consistent with trade direction, for buy trades a move higher afterwards is BAD (
        # for sell trades a move lower is BAD afterwards)
        metric_df[self._metric_name] = np.multiply(((metric_df[executed_price].values /
                                                    metric_df[self._metric_name + '_benchmark'].values) - 1.0),
                                                    market_impact_multiplier * trade_order_df['side'].values)

        metric_df = metric_df.drop(executed_price, axis=1)
        metric_list = [self._metric_name + '_benchmark', self._metric_name + '_benchmark_time', self._metric_name, 'id']

        #metric_trade_df = pd.merge(market_trade_order_df, metric_df[metric_list],
        #                                how='left', left_index=True, on=['id'])

        metric_trade_df, metric_df = self._combine_metric_trade(metric_df[metric_list], trade_order_df)

        # metric_df = metric_df[self._metric_name]

        return metric_trade_df, metric_df


class MetricTransientMarketImpact(MetricMarketImpact):
    def __init__(self, trade_order_list=None, executed_price='executed_price', bid_benchmark='bid', ask_benchmark='ask',
                 market_impact_multipler=constants.market_impact_multiplier,
                 transient_market_impact_gap=constants.transient_market_impact_gap, metric_post_fix=''):
        super(MetricTransientMarketImpact, self).__init__(trade_order_list=trade_order_list,
                                                          executed_price=executed_price,
                                                          bid_benchmark=bid_benchmark, ask_benchmark=ask_benchmark,
                                                          market_impact_multipler=market_impact_multipler)

        self._market_impact_gap = transient_market_impact_gap
        self._metric_name = 'transient_market_impact' + metric_post_fix


class MetricPermanentMarketImpact(MetricMarketImpact):
    def __init__(self, trade_order_list=None, executed_price='executed_price', bid_benchmark='bid', ask_benchmark='ask',
                 market_impact_multipler=constants.market_impact_multiplier,
                 permanent_market_impact_gap=constants.permanent_market_impact_gap, metric_post_fix=''):
        super(MetricPermanentMarketImpact, self).__init__(trade_order_list=trade_order_list,
                                                          executed_price=executed_price,
                                                          bid_benchmark=bid_benchmark, ask_benchmark=ask_benchmark,
                                                          market_impact_multipler=market_impact_multipler)

        self._market_impact_gap = permanent_market_impact_gap
        self._metric_name = 'permanent_market_impact' + metric_post_fix


########################################################################################################################

class MetricExecutedPriceNotional(MetricCalc):
    """Calculates the derived execution price for orders, based upon their trades. Also aggregates related
    fields, such as the notional, executed notional and cancelled notional from trades.

    """

    def __init__(self, upper_id='id', aggregated_ids='ancestor_pointer_id',
                 executed_notional='executed_notional', executed_price='executed_price', notional='notional',
                 cancelled_notional='cancelled_notional',
                 event_type='event_type', event_type_filter='trade',
                 aggregated_notional_fields=['executed_notional', 'notional'],
                 notional_reporting_currency_spot='notional_reporting_currency_mid'):
        """Initalise the derived field computation for calculating the executed price and notional for orders, based
        upon lower level trades.

        Parameters
        ----------
        upper_id : str
            Field which represents ID of a trade

        aggregated_ids : str
            Field which represents the ancestor ID (ie. the ID of the order associated with a trade)

        executed_notional : str
            Field which represents the amount of notional executed

        executed_price : str
            Field name for price of execution

        notional : str
            Field for the notional of a trade/order

        cancelled_notional : str
            Cancelled notional amount field

        event_type : str
            Event type field

        event_type_filter : str
            Filter for trades which have
            'trade' - actual trade execution

        aggregated_notional_fields : str (list)
            Fields which represent the notional

        notional_reporting_currency_spot : str
            Field for the spot rate between the notional and reporting currency (typically USD)
        """
        super(MetricExecutedPriceNotional, self).__init__()

        self._upper_id = upper_id
        self._aggregated_ids = aggregated_ids
        self._executed_notional = executed_notional
        self._executed_price = executed_price
        self._notional = notional
        self._cancelled_notional = cancelled_notional
        self._event_type = event_type
        self._event_type_filter = event_type_filter
        self._aggregated_notional_fields = aggregated_notional_fields
        self._metric_name = None
        self._notional_reporting_currency_spot = notional_reporting_currency_spot

    def calculate_metric(self, lower_trade_order_df=None, upper_trade_order_df=None, upper_id=None, aggregated_ids=None,
                         executed_notional=None, executed_price=None, notional=None,
                         cancelled_notional=None,
                         event_type=None, event_type_filter=None, aggregated_notional_fields=None,
                         notional_reporting_currency_spot=None):

        if upper_id is None: upper_id = self._upper_id
        if aggregated_ids is None: aggregated_ids = self._aggregated_ids
        if executed_notional is None: executed_notional = self._executed_notional
        if executed_price is None: executed_price = self._executed_price
        if notional is None: notional = self._notional
        if cancelled_notional is None: cancelled_notional = self._cancelled_notional
        if event_type is None: event_type = self._event_type
        if event_type_filter is None: event_type_filter = self._event_type_filter
        if aggregated_notional_fields is None: aggregated_notional_fields = self._aggregated_notional_fields
        if notional_reporting_currency_spot is None: notional_reporting_currency_spot = self._notional_reporting_currency_spot

        # for the lowest level (eg. trade executions) keep only the actual trades, ignoring any placements, cancels etc.
        # given that only actual trade messages from the broker have executed notional
        if event_type in lower_trade_order_df.columns:
            lower_trade_order_df = lower_trade_order_df[lower_trade_order_df[event_type] == event_type_filter]

        # if the notional is the lower order (ie. child/parent/grandparent orders), then we want to also calculate the aggregated
        # notional, otherwise only total up the executed notional

        # in both cases calculate the average execution rate
        if aggregated_notional_fields is not None:
            metric_df = self._time_series_ops.weighted_average_by_agg(lower_trade_order_df, executed_price,
                                                                      executed_notional, aggregated_ids,
                                                                      unweighted_data_col=aggregated_notional_fields)

        # join the upper order with the derived fields for trade price, notional etc.
        metric_upper_order_df = upper_trade_order_df.join(metric_df, on=upper_id)

        # for lower orders which are fully cancelled their executed notional won't exist (IE. will be zero!)
        metric_upper_order_df[executed_notional].fillna(0, inplace=True)

        # cancelled notional can be inferred from notional - executed notional
        if notional in metric_upper_order_df.columns:
            metric_upper_order_df[cancelled_notional] = metric_upper_order_df[notional] - metric_upper_order_df[
                executed_notional]

        # add executed notional in reporting currency too
        if notional_reporting_currency_spot is not None:
            if notional_reporting_currency_spot in metric_upper_order_df.columns:
                metric_upper_order_df['executed_notional_in_reporting_currency'] \
                    = metric_upper_order_df['executed_notional'].values * metric_upper_order_df[
                    notional_reporting_currency_spot].values

                # if notional in reporting is missing, just assume it's the same as the executed notional for reporting currency
                if 'notional_in_reporting_currency' not in metric_upper_order_df.columns and notional not in metric_upper_order_df.columns:
                    metric_upper_order_df['notional_in_reporting_currency'] = metric_upper_order_df[
                        'executed_notional_in_reporting_currency']

        #
        # if 'executed_notional_currency' not in metric_upper_order_df.columns:
        #     metric_upper_order_df['executed_notional_currency'] = metric_upper_order_df['notional_currency']

        # if we don't have notional specified, just assume it's the same as the executed notional
        if notional not in metric_upper_order_df.columns:
            metric_upper_order_df[notional] = metric_upper_order_df[executed_notional]

        return metric_upper_order_df, metric_df

########################################################################################################################

from datetime import timedelta


class MetricDisplay(Metric):
    """Metrics which display the market data in a complicated manner (eg. have more than one output), such as markout tables
    rather than as a single summarised value (like slippage)

    """

    def _mult_metric_table_by_side(self, trade_order_df, metric_df):

        # duplicate side column for every column of metric to be multiplied
        side_multiplier = np.repeat(trade_order_df['side'].values[np.newaxis, :], len(metric_df.columns), 0).transpose()

        # multiply every column by the trade/order side
        metric_df = pd.DataFrame(index=metric_df.index, columns=metric_df.columns,
                                     data=constants.market_impact_multiplier * metric_df * side_multiplier)

        return metric_df

########################################################################################################################

class MetricMarkout(MetricDisplay):
    """Calculates markout style tables for multiple trades, when combined with market data.

    """

    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid',
                 markout_windows=constants.markout_windows, markout_unit_of_measure=constants.markout_unit_of_measure):
        """Initialise markout with calculation parameters for markout window points and unit of measure for each point
        in milliseconds, seconds or minutes.

        Parameters
        ----------
        trade_order_list : str (list)
            The trades/orders for which we'd like markouts to be calculated

        bid_benchmark : str
            Field for the bid quotes

        ask_benchmark : str
            Field for the ask quotes

        markout_windows : int (list)
            The time intervals to calculate markouts

        markout_unit_of_measure : str
            Size of the markout units
        """

        super(MetricMarkout, self).__init__(trade_order_list=trade_order_list)

        self._bid_benchmark = bid_benchmark
        self._ask_benchmark = ask_benchmark

        self._markout_windows = markout_windows
        self._markout_unit_of_measure = markout_unit_of_measure

        self._str_windows = [str(x) + self._markout_unit_of_measure for x in self._markout_windows]

        self._metric_name = None

    def calculate_metric(self, trade_order_df=None, market_df=None, trade_order_name=None,
                         bid_benchmark=None, ask_benchmark=None):
        if not (self._check_calculate_metric(trade_order_name)): return trade_order_df, None

        if bid_benchmark is None: bid_benchmark = self._bid_benchmark
        if ask_benchmark is None: ask_benchmark = self._ask_benchmark

        metric_df = self._calculate_markout(trade_order_df, market_df,
                                            bid_benchmark=bid_benchmark, ask_benchmark=ask_benchmark)

        metric_df.columns = ['markout_' + x for x in metric_df.columns]
        metric_df['markout'] = metric_df[metric_df.columns[-1]].values

        # multiplies metric by the side (so that buy and sell can be compared)
        metric_df = self._mult_metric_table_by_side(trade_order_df, metric_df)

        metric_df['id'] = trade_order_df['id']

        metric_trade_df, metric_df = self._combine_metric_trade(metric_df, trade_order_df)

        return metric_trade_df, metric_df

    def _calculate_markout(self, trade_order_df, market_df, bid_benchmark=None, ask_benchmark=None):

        metric_df = pd.DataFrame(index=trade_order_df.index, columns=self._str_windows)

        # fill markout window with market prices for all "buy" side trades
        metric_df = self._fill_markout_window_with_prices(trade_order_df, market_df, metric_df, 1, ask_benchmark)

        # fill markout window with market prices for all "sell" side trades
        metric_df = self._fill_markout_window_with_prices(trade_order_df, market_df, metric_df, -1, bid_benchmark)

        # now calculate the returns for each window, against the 0 point (ie. this will be cumulative returns)
        zero_mark_str = '0' + self._markout_unit_of_measure

        for window in self._markout_windows:

            # ignore window at 0 (by definition returns will be 0 here!)
            if window != 0:
                window_str = str(window) + self._markout_unit_of_measure

                metric_df[window_str] = (metric_df[window_str] / metric_df[zero_mark_str]) - 1.0
                metric_df[window_str] = metric_df[window_str].astype(float)

        # fill the 0ms/s time with 0
        metric_df.loc[:, zero_mark_str] = 0.0

        return metric_df

    def _fill_markout_window_with_prices(self, trade_order_df, market_df, metric_df, side, benchmark):
        benchmark = self._get_benchmark_field(market_df, benchmark)

        # create columns for the markout window
        for window in self._markout_windows:
            is_side = trade_order_df['side'] == side

            sided_trades = trade_order_df[is_side].index

            # sometimes we might not have any specific sided trades in a series, so must check!
            if len(sided_trades) > 0:
                # shift the time by x milliseconds
                if self._markout_unit_of_measure == 'ms':
                    time_move = timedelta(microseconds=window * 1000)
                elif self._markout_unit_of_measure == 's':
                    time_move = timedelta(seconds=window)
                elif self._markout_unit_of_measure == 'm':
                    time_move = timedelta(minutes=window)

                # time_shifted = sided_trades + time_move

                # find all the indices in the market data that corresponds to (approximately) the time we want (should get the
                # very next tick

                # super slow implementation.. given massive loop
                # ix_price_1 = [market_df.index.get_loc(x, method='nearest') for x in time_shifted]

                # much faster vectorised version (we need to offset by + 1, such we get the NEXT tick after the point we want
                ix_price = self._time_series_ops.search_series(market_df, sided_trades, timedelta_amount=time_move,
                                                               just_before_point=False)

                # ix_price = market_df.index.searchsorted(time_shifted) + 1

                # on the very rare occasion that we are right at the end of the series, adjust those to the last ones (otherwise
                # will get array out of bounds exception!)
                # TODO
                # if ix_price[-1] > len(market_df.index): ix_price[-1] = ix_price[-1] - 1
                # ix_price[ix_price > len(market_df.index)] = len(market_df.index) - 1

                # now fill the column with the market price at the shifted time
                metric_df.ix[is_side, str(window) + self._markout_unit_of_measure] = market_df.ix[
                    ix_price, benchmark].values.T

        return metric_df

########################################################################################################################

class MetricWideBenchmarkMarkout(MetricMarkout):
    """Calculates markout style plots for specific trades, when combined with market data, presenting it alongside a
    comparison of the average drift throughout the period. Hence, we can compare the markout of a specific child trade,
    versus the average spot move over the past few days.

    """

    def __init__(self, trade_order_list=None, bid_benchmark='mid', ask_benchmark='mid', wide_benchmark='mid',
                 markout_unit_of_measure=constants.wide_benchmark_unit_of_measure,
                 benchmark_markout_unit=constants.wide_benchmark_markout_unit,
                 benchmark_windows_multiplier=constants.wide_benchmark_markout_windows_multiplier):
        """

        Parameters
        ----------
        trade_order_list : str (list)
            The trades/orders for which we'd like markouts to be calculated

        bid_benchmark : str
            Field for the bid quotes

        ask_benchmark : str
            Field for the ask quotes

        wide_benchmark : str
            Field for the wide benchmark

        markout_unit_of_measure : str
            Size of the markout units

        benchmark_markout_unit : str
            Size of the benchmark markout unit

        benchmark_windows_multiplier : int
            Multiplier for the benchmark window
        """

        super(MetricMarkout, self).__init__(trade_order_list=trade_order_list)

        self._bid_benchmark = bid_benchmark
        self._ask_benchmark = ask_benchmark
        self._markout_unit_of_measure = markout_unit_of_measure
        self._benchmark_markout_unit = benchmark_markout_unit
        self._benchmark_windows_multiplier = benchmark_windows_multiplier
        self._wide_benchmark = wide_benchmark

        self._markout_windows = [self._benchmark_markout_unit * x for x in self._benchmark_windows_multiplier]
        self._str_windows = [str(x) + self._markout_unit_of_measure for x in self._markout_windows]

        self._metric_name = None

    def calculate_metric(self, trade_order_df=None, market_df=None, trade_order_name=None, bid_benchmark=None,
                         ask_benchmark=None, wide_benchmark=None):
        if not (self._check_calculate_metric(trade_order_name)): return trade_order_df, None

        # if parameters have not been set, take whatever has been set in the field variables
        if bid_benchmark is None: bid_benchmark = self._bid_benchmark
        if ask_benchmark is None: ask_benchmark = self._ask_benchmark
        if wide_benchmark is None: wide_benchmark = self._wide_benchmark

        metric_df = self._calculate_markout(trade_order_df, market_df,
                                            bid_benchmark=bid_benchmark, ask_benchmark=ask_benchmark)
        metric_df['id'] = trade_order_df['id']

        summary_df = pd.DataFrame(index=['Avg Drift'], columns=self._str_windows)

        market_df = market_df[wide_benchmark]

        # filter the market data TODO? for specific start/end points

        # resample the market data pricing for the units
        market_df = pd.DataFrame(market_df).resample \
            (str(self._benchmark_markout_unit) + self._markout_unit_of_measure).mean()

        market_df = market_df.dropna(subset=[wide_benchmark])

        # compute the returns average over the whole window of market data
        ret_avg = float((market_df / market_df.shift(1) - 1).mean()[0])

        # add an extra line which represents the "Wide move"
        zero_mark_str = '0' + self._markout_unit_of_measure

        # for each point in time fill the window
        for i in range(0, len(self._markout_windows)):

            window = self._markout_windows[i]

            # ignore window at 0 (by definition returns will be 0 here!)
            if window != 0:
                window_str = str(window) + self._markout_unit_of_measure

                summary_df[window_str] = self._benchmark_windows_multiplier[i] * ret_avg
                summary_df[window_str] = summary_df[window_str].astype(float)

        # fill the 0ms/s time with 0
        summary_df.loc[:, zero_mark_str] = 0.0

        metric_trade_order_df, metric_df = self._combine_metric_trade(metric_df, trade_order_df)

        metric_df = metric_df.append(summary_df, sort=False)
        metric_trade_order_df = metric_trade_order_df.append(summary_df, sort=False)

        return metric_trade_order_df, metric_df



