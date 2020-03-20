from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.analysis.tcaengine import TCARequest
from tcapy.analysis.tcaengine import TCAEngineImpl

from tcapy.analysis.algos.metric import *
from tcapy.analysis.tradeorderfilter import *
from tcapy.vis.computationcaller import ComputationCaller
from tcapy.analysis.algos.resultsform import *

from tcapy.conf.constants import Constants

import pandas as pd

import abc

ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

constants = Constants()

class TCACaller(ComputationCaller, ABC):
    """Abstract class which adds listeners to the GUI buttons in the tcapy application for doing TCA calculations. At
    initialisation it adds listeners for these buttons and links them to the various text box inputs (where the user
    can specify the various TCA parameters such as start date, finish date, ticker, TCA metrics etc.)

    When a button is pressed it triggers various "calculate" methods, which convert the GUI input, into TCARequest objects
    which are then sent to TCAEngine for doing the actual TCA computation. This analysis is then cached in Redis. The
    completion of this calculation will then trigger a callback from every display component (such as a plot or table)
    which search the cache for the appropriate output to display.

    If a user wishes to create programmatically call tcapy, it is recommended they create a TCARequest directly, rather
    than attempting to use TCACaller, and then submit that to a TCAEngine.
    """

    def __init__(self, app, session_manager, callback_manager, glob_volatile_cache, layout, callback_dict=None):
        super(TCACaller, self).__init__(app, session_manager, callback_manager, glob_volatile_cache, layout,
                                                 callback_dict=callback_dict)

        self._generic_plot_flags = {
            '_candle_timeline_trade_order': 'candle-timeline-plot',
            '_table_trade_order': 'table',
            '_dist_trade_order': 'dist-plot',
            '_download_link_trade_order': 'download-link',
            '_timeline_trade_order': 'timeline-plot',
            '_bar_trade_order': 'bar-plot',
            '_dist_trade_order': 'dist-plot',
            '_metric_table_trade_order': 'table'
        }

        self._generic_line_flags = {
            '_candle_timeline_trade_order':
                ['candle-timeline-plot-lines-old', 'candle-timeline-plot-lines-relayoutData-old']
        }

        self._plot_flags = self.create_plot_flags(session_manager, layout)

        self._reload_val_dict = {None: False, 'yes': True, 'no': False}

        self._tca_engine = TCAEngineImpl()

    def fill_computation_request_kwargs(self, kwargs, fields):
        """Fills a dictionary with the appropriate parameters which can be consumed by a TCARequest object. This involves
        a large number of object conversations, eg. str based dates to TimeStamps, metric names to Metric objects etc.

        Parameters
        ----------
        kwargs : dict
            Contains parameters related to TCA analysis

        fields : str(list)
            List of TCA fields we should fill with None if they don't exist in kwargs

        Returns
        -------
        dict
        """

        # fill the major fields

        kwargs['ticker'] = self._util_func.remove_none_list(kwargs['ticker']);
        kwargs['venue'] = self._util_func.remove_none_list(kwargs['venue'])

        # convert date strings into TimeStamp formats
        kwargs['start_date'] = pd.Timestamp(self._util_func.parse_datetime(str(kwargs['start_date'])))
        kwargs['finish_date'] = pd.Timestamp(self._util_func.parse_datetime(str(kwargs['finish_date'])))

        try:
            kwargs['reload'] = self._reload_val_dict[kwargs['reload']]
        except:
            kwargs['reload'] = False

        if 'event_type' not in kwargs.keys():
            kwargs['event_type'] = 'trade'

        if 'market_data' not in kwargs.keys():
            kwargs['market_data'] = constants.default_market_data_store

        # fill empty fields with None
        for f in fields:
            if f not in kwargs:
                kwargs[f] = None

        # add a trade filter for time day
        if kwargs['filter_time_of_day'] is not None:
            if kwargs['filter_time_of_day'] == 'yes':
                if 'start_time_of_day' in kwargs and 'finish_time_of_day' in kwargs:
                    kwargs = self.add_list_kwargs(kwargs, 'trade_order_filter',
                                                  TradeOrderFilterTimeOfDayWeekMonth(
                                                      time_of_day={'start_time': kwargs['start_time_of_day'],
                                                                   'finish_time': kwargs['finish_time_of_day']}))

        filter_tags = ['broker', 'algo'];
        tag_value_combinations = {}

        for f in filter_tags:
            if kwargs[f] is not None:
                tag_value_combinations[f + '_id'] = kwargs[f]

        if len(tag_value_combinations) > 0:
            kwargs = self.add_list_kwargs(kwargs, 'trade_order_filter',
                                          TradeOrderFilterTag(tag_value_combinations=tag_value_combinations))

        # add metrics which have been specified (including as strings, which will be added with default parameters)
        if kwargs['metric_calcs'] is not None:
            if not (isinstance(kwargs['metric_calcs'], list)):
                kwargs['metric_calcs'] = [kwargs['metric_calcs']]

            for i in range(0, len(kwargs['metric_calcs'])):
                kwargs['metric_calcs'][i] = self.fill_metrics(kwargs['metric_calcs'][i],
                                                              kwargs['metric_trade_order_list'],
                                                              kwargs['event_type'])

        return kwargs

    def fill_metrics(self, metric, metric_trade_order_list, event_type):
        """Converts string describing metrics to the appropriate Metric object (with default parameters), which can later
        be consumed by the TCARequest object.

        Parameters
        ----------
        metric : str or Metric
            Can be a string (eg. 'slippage', 'transient_market_impact', 'permanent_market_impact') or an actual Metric
            object

        metric_trade_order_list : str (list)
            For which trades/orders should this metric be computed for.

        event_type : str
            Trade event type (eg. 'trade', 'cancel', 'cancel/replace' etc)

        Returns
        -------
        Metric
        """
        # if we are given strings of Metric, we need to create the appropriate Metric object in its place
        # NOTES: that we'll only have default arguments
        try:
            metric = metric.replace(' ', '_')

            executed_price = 'executed_price'

            # for placements, we wouldn't have an execution price, so closest we can do is the arrival price = mid for trades
            if event_type != 'trade':
                executed_price = 'arrival'

            if metric == 'slippage':
                return MetricSlippage(trade_order_list=metric_trade_order_list, executed_price=executed_price)
            elif metric == 'transient_market_impact':
                return MetricTransientMarketImpact(trade_order_list=metric_trade_order_list,
                                                   executed_price=executed_price)
            elif metric == 'permanent_market_impact':
                return MetricPermanentMarketImpact(trade_order_list=metric_trade_order_list,
                                                   executed_price=executed_price)

            ## ADD new metrics you write here (or better to subclass in your version of TCACaller)
        except:
            pass

        return metric

    def create_computation_request(self, **kwargs):
        """Creates a TCARequest object, populating its' fields with those from a kwargs dictionary, which consisted of
        parameters such as the start date, finish date, ticker, metrics to be computed, benchmark to be computed etd.

        The TCARequest object can later be consumed by a TCAEngine when it runs a TCA analysis.

        Parameters
        ----------
        kwargs : dict
            For describing a TCA analysis, such as the start date, finish date, ticker etc.

        Returns
        -------
        TCARequest
        """

        if 'tca_request' in kwargs.keys():
            return kwargs['tca_request']

        # convert various string/objects into forms which can be accepted by TCARequest
        kwargs = self.fill_computation_request_kwargs(
            kwargs, ['trade_order_mapping', 'trade_order_filter', 'benchmark_calcs', 'metric_calcs',
                     'join_tables', 'filter_time_of_day', 'broker', 'algo', 'dummy_market'])

        # create a TCARequest object which can be consumed by TCAEngine, to run a TCA calculation
        return TCARequest(start_date=kwargs['start_date'],
                          finish_date=kwargs['finish_date'],
                          ticker=kwargs['ticker'],
                          venue=kwargs['venue'],
                          event_type=kwargs['event_type'],
                          market_data_store=kwargs['market_data'],
                          tca_type=kwargs['tca_type'],
                          reload=kwargs['reload'],
                          trade_order_mapping=kwargs['trade_order_mapping'],
                          trade_order_filter=kwargs['trade_order_filter'],
                          metric_calcs=kwargs['metric_calcs'],
                          benchmark_calcs=kwargs['benchmark_calcs'],
                          join_tables=kwargs['join_tables'],
                          results_form=kwargs['results_form'],
                          dummy_market=kwargs['dummy_market'])

    def run_computation_request(self, tca_request):
        """Kicks of the TCA analysis in the underlying TCAEngine using parameters specified

        Parameters
        ----------
        tca_request : TCARequest
            Governs start date/finish date, tickers etc. of TCA analysis

        Returns
        -------
        dict
        """
        return self._tca_engine.calculate_tca(tca_request)