from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from collections import OrderedDict

from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.timeseries import TimeSeriesOps

from tcapy.util.customexceptions import *

constants = Constants()

import datetime
import pytz

class DataRequest(object):
    """Has parameters for any type of data request or TCA computation we make. This is subclassed by, MarketRequest,
    TradeRequest and TCARequest

    """

    def __init__(self, start_date=None, finish_date=None, ticker=None, data_store=constants.default_data_store, data_offset_ms=None,
                 reload=False, instrument=constants.default_instrument, asset_class=constants.default_asset_class, access_control=None,
                 trade_data_database_table=None,
                 market_data_database_table=None,
                 use_multithreading=constants.use_multithreading, multithreading_params=constants.multithreading_params, data_norm=None):

        self.start_date = start_date
        self.finish_date = finish_date
        self.ticker = ticker
        self.data_store = data_store
        self.data_offset_ms = data_offset_ms
        self.reload = reload
        self.instrument = instrument
        self.asset_class = asset_class
        self.access_control = access_control
        self.trade_data_database_table = trade_data_database_table
        self.market_data_database_table = market_data_database_table
        self.data_norm = data_norm

        self.use_multithreading = use_multithreading
        self.multithreading_params = multithreading_params

    @property
    def start_date(self):
        return self.__start_date

    @start_date.setter
    def start_date(self, start_date):
        self.__start_date = TimeSeriesOps().date_parse(start_date)

    @property
    def finish_date(self):
        return self.__finish_date

    @finish_date.setter
    def finish_date(self, finish_date):
        self.__finish_date = TimeSeriesOps().date_parse(finish_date)

    @property
    def ticker(self):
        return self.__ticker

    @ticker.setter
    def ticker(self, ticker):
        self.__ticker = ticker

    @property
    def data_store(self):
        return self.__data_store

    @data_store.setter
    def data_store(self, data_store):
        self.__data_store = self._check_data_store(data_store)
        
    @property
    def data_offset_ms(self):
        return self.__data_offset_ms

    @data_offset_ms.setter
    def data_offset_ms(self, data_offset_ms):
        self.__data_offset_ms = data_offset_ms

    @property
    def reload(self):
        return self.__reload

    @reload.setter
    def reload(self, reload):
        self.__reload = reload
        
    @property
    def instrument(self):
        return self.__instrument

    @instrument.setter
    def instrument(self, instrument):
        self.__instrument = instrument
        
    @property
    def asset_class(self):
        return self.__asset_class

    @asset_class.setter
    def asset_class(self, asset_class):
        self.__asset_class = asset_class
        
    @property
    def access_control(self):
        return self.__access_control

    @access_control.setter
    def access_control(self, access_control):
        self.__access_control = access_control

    @property
    def use_multithreading(self):
        return self.__use_multithreading

    @use_multithreading.setter
    def use_multithreading(self, use_multithreading):
        self.__use_multithreading = use_multithreading

    @property
    def multithreading_params(self):
        return self.__multithreading_params

    @multithreading_params.setter
    def multithreading_params(self, multithreading_params):
        self.__multithreading_params = multithreading_params

    @property
    def trade_data_database_name(self):
        return self.__trade_data_database_name

    @trade_data_database_name.setter
    def trade_data_database_name(self, trade_data_database_name):
        self.__trade_data_database_name = trade_data_database_name

    @property
    def market_data_database_table(self):
        return self.__market_data_database_table

    @market_data_database_table.setter
    def market_data_database_table(self, market_data_database_table):
        self.__market_data_database_table = market_data_database_table

    @property
    def data_norm(self):
        return self.__data_norm

    @data_norm.setter
    def data_norm(self, data_norm):
        self.__data_norm = data_norm

    def _check_data_store(self, data_store):
        try:
            # constants = Constants()

            if not data_store in constants.valid_data_store or '.csv' not in data_store or '.h5' not in data_store:
                err_msg = data_store + " is not a defined data source."

                LoggerManager.getLogger(__name__).error(err_msg)

                raise ValidationException(err_msg)
        except:
            pass

        return data_store

########################################################################################################################

class MarketRequest(DataRequest):
    """When making a request for market data, we use the MarketRequest object. Typically, we ask for parameters relating
    to the start/finish dates, the ticker and other associated parameters

    """

    def __init__(self, market_request=None, start_date=None, finish_date=None, ticker=None, data_store=constants.default_market_data_store,
                 data_offset_ms=None,
                 reload=False, instrument=constants.default_instrument, asset_class=constants.default_asset_class, access_control=None,
                 use_multithreading=constants.use_multithreading, multithreading_params=constants.multithreading_params, data_norm=None,
                 market_data_database_table=None):
        # constants = Constants()

        if market_request is not None:
            start_date = market_request.start_date
            finish_date = market_request.finish_date
            ticker = market_request.ticker

            if hasattr(market_request, 'market_data_store'):
                data_store = market_request.market_data_store
                data_offset_ms = market_request.market_data_offset_ms

            else:
                data_store = market_request.data_store
                data_offset_ms = market_request.data_offset_ms

            reload = market_request.reload
            instrument = market_request.instrument
            asset_class = market_request.asset_class
            access_control = market_request.access_control
            use_multithreading = market_request.use_multithreading
            multithreading_params = market_request.multithreading_params
            data_norm = market_request.data_norm

            market_data_database_table = market_request.market_data_database_table

        self.start_date = start_date
        self.finish_date = finish_date
        self.ticker = ticker
        self.data_store = data_store
        self.data_offset_ms = data_offset_ms
        self.reload = reload
        self.instrument = instrument
        self.asset_class = asset_class
        self.access_control = access_control

        self.use_multithreading = use_multithreading
        self.multithreading_params = multithreading_params
        self.data_norm = data_norm

        self.market_data_database_table = market_data_database_table

########################################################################################################################

class TradeRequest(DataRequest):
    """When making request for trades/orders, we use the TradeRequest object. Typically, we ask for parameters relating
    to start/finish dates, the ticker and other similar parameters.

    """

    def __init__(self, trade_request=None, start_date=None, finish_date=None, ticker=None, data_store=constants.default_data_store, data_offset_ms=None,
                 reload=False, instrument=constants.default_instrument, asset_class=constants.default_asset_class, access_control=None,
                 trade_data_database_name=None,
                 # market_data_database_table=None,
                 use_multithreading=constants.use_multithreading, multithreading_params=constants.multithreading_params, data_norm=None,
                 trade_order_type=None, trade_order_mapping=None, event_type='trade'):

        if trade_request is not None:
            start_date = trade_request.start_date
            finish_date = trade_request.finish_date
            ticker = trade_request.ticker

            if hasattr(trade_request, 'trade_data_store'):
                data_store = trade_request.trade_data_store
                data_offset_ms = trade_request.trade_data_offset_ms
            else:
                data_store = trade_request.data_store
                data_offset_ms = trade_request.data_offset_ms

            reload = trade_request.reload
            instrument = trade_request.instrument
            asset_class = trade_request.asset_class
            access_control = trade_request.access_control
            use_multithreading = trade_request.use_multithreading
            multithreading_params = trade_request.multithreading_params
            trade_data_database_name = trade_request.trade_data_database_name
            # market_data_database_table = trade_request.market_data_database_table
            data_norm = trade_request.data_norm

            if hasattr(trade_request, 'trade_order_type'): # only TradeRequest has trade_order_type
                trade_order_type = trade_request.trade_order_type

            event_type = trade_request.event_type
            trade_order_mapping = trade_request.trade_order_mapping
        # constants = Constants()

        self.start_date = start_date
        self.finish_date = finish_date
        self.ticker = ticker
        self.data_store = data_store
        self.data_offset_ms = data_offset_ms
        self.reload = reload
        self.instrument = instrument
        self.asset_class = asset_class
        self.access_control = access_control
        self.trade_data_database_name = trade_data_database_name
        # self.market_data_database_table = market_data_database_table

        self.use_multithreading = use_multithreading
        self.multithreading_params = multithreading_params
        self.data_norm = data_norm

        self.trade_order_type = trade_order_type
        self.event_type = event_type

        if trade_order_mapping is None and 'csv' not in self.data_store and '.h5' not in self.data_store:
            trade_order_mapping = constants.trade_order_mapping[self.data_store]

        self.trade_order_mapping = trade_order_mapping

    @property
    def trade_order_type(self):
        return self.__trade_order_type

    @trade_order_type.setter
    def trade_order_type(self, trade_order_type):
        self.__trade_order_type = trade_order_type

    def _check_trade_order_type(self, trade_order_type):
        try:
            # constants = Constants()
            valid_trade_order_type = constants.trade_order_list

            if not trade_order_type in valid_trade_order_type:
                # don't make LoggerManager field variable so this can be pickled (important for Celery)
                LoggerManager().getLogger(__name__).error(trade_order_type & " is not a defined trade or order.")

                raise ValidationException(trade_order_type & " is not a defined trade or order.")
        except:
            pass

        return trade_order_type

    @property
    def trade_order_mapping(self):
        return self.__trade_order_mapping

    @trade_order_mapping.setter
    def trade_order_mapping(self, trade_order_mapping):
        self.__trade_order_mapping = trade_order_mapping

    @property
    def event_type(self):
        return self.__reload

    @event_type.setter
    def event_type(self, event_type):
        self.__reload = event_type

class ComputationRequest(object):
    """Generate object for any long run computation/analysis"""
    pass

class TCARequest(TradeRequest, ComputationRequest):
    """Defines the parameters which specify the type of TCA we would like to compute, including such parameters like:
    - start/finish dates
    - _tickers we wish to analyse (note: TCAAggregatedEngine, allows multi-ticker, TCADetailedEngine is only for a single ticker)
    - the benchmarks

    TCARequest is a lightweight object which can be pickled
    """

    def __init__(self, start_date=None, finish_date=None, ticker=None, venue='All',
                 market_data_store=constants.default_market_data_store,
                 trade_data_store=constants.default_trade_data_store,
                 trade_data_database_name=None,
                 market_data_database_table=None,
                 market_data_offset_ms=None, trade_data_offset_ms=None,
                 bid_benchmark='mid', ask_benchmark='mid',
                 tca_request=None, reload=False, instrument=constants.default_instrument, asset_class=constants.default_asset_class,
                 event_type='trade', trade_order_mapping=None, trade_order_filter=[],
                 benchmark_calcs=[],
                 metric_calcs=[], results_form=[], metric_display=[], join_tables=[],
                 extra_lines_to_plot=[],
                 tca_type='detailed',
                 reporting_currency=constants.reporting_currency, dummy_market=False, summary_display=None, access_control=None,
                 use_multithreading=constants.use_multithreading, multithreading_params=constants.multithreading_params, data_norm=None,
                 tca_provider=constants.tcapy_provider):

        # constants = Constants()

        if tca_request is not None:
            start_date = tca_request.start_date
            finish_date = tca_request.finish_date
            ticker = tca_request.ticker
            venue = tca_request.venue
            market_data_store = tca_request.market_data_store
            trade_data_store = tca_request.trade_data_store
            trade_data_database_name = tca_request.trade_data_database_name
            market_data_database_table = tca_request.market_data_database_table
            market_data_offset_ms = tca_request.market_data_offset_ms
            trade_data_offset_ms = tca_request.trade_data_offset_ms

            bid_benchmark = tca_request.bid_benchmark
            ask_benchmark = tca_request.ask_benchmark
            reload = tca_request.reload
            instrument = tca_request.instrument
            asset_class = tca_request.asset_class

            event_type = tca_request.event_type
            trade_order_mapping = tca_request.trade_order_mapping
            trade_order_filter = tca_request.trade_order_filter
            benchmark_calcs = tca_request.benchmark_calcs
            metric_calcs = tca_request.metric_calcs

            results_form = tca_request.results_form
            metric_display = tca_request.metric_display
            join_tables = tca_request.join_tables
            extra_lines_to_plot = tca_request.extra_lines_to_plot
            
            tca_type = tca_request.tca_type
            reporting_currency = tca_request.reporting_currency
            dummy_market = tca_request.dummy_market
            summary_display = tca_request.summary_display
            access_control = tca_request.access_control
            use_multithreading = tca_request.use_multithreading
            multithreading_params = tca_request.multithreading_params
            data_norm = tca_request.data_norm
            
            tca_provider = tca_request.tca_provider

        self.start_date = start_date  # start date of computation
        self.finish_date = finish_date  # finish date of computation
        self.ticker = ticker  # which _tickers to do TCA on?
        self.venue = venue  # which venues do we want to analyse?
        self.market_data_store = market_data_store  # what is the dataset for market data?
        self.trade_data_store = trade_data_store  # what is the dataset for trade data?
        self.trade_data_database_name = trade_data_database_name
        self.market_data_database_table = market_data_database_table
        self.market_data_offset_ms = market_data_offset_ms
        self.trade_data_offset_ms = trade_data_offset_ms

        self.bid_benchmark = bid_benchmark  # which field to use as benchmark for the bid eg. 'mid'
        self.ask_benchmark = ask_benchmark  # which field to use as benchmark for the ask eg. 'mid'
        self.reload = reload  # should we forcibly reload data directly from database?
        self.instrument = instrument
        self.asset_class = asset_class
        self.event_type = event_type  # is it a trade or cancellation 'trade', 'placement', cancellation', 'cancel/replace'

        # If we are doing pure market analysis (without trades), we should not specify a trade_data_store
        if self.trade_data_store is not None:
            if trade_order_mapping is None and 'csv' not in self.trade_data_store and 'h5' not in self.trade_data_store:
                trade_order_mapping = constants.trade_order_mapping[self.trade_data_store]

            if isinstance(trade_order_mapping, str):
                trade_order_mapping = [trade_order_mapping]

            # Flesh out the trade_order_mapping if specified by a list
            if isinstance(trade_order_mapping, list):

                trade_order_mapping_dict = OrderedDict()
                saved_trade_order_mapping = constants.trade_order_mapping[self.trade_data_store]

                for t in trade_order_mapping:
                    trade_order_mapping_dict[t] = saved_trade_order_mapping[t]

                trade_order_mapping = trade_order_mapping_dict

        self.trade_order_mapping = trade_order_mapping
        self.trade_order_filter = trade_order_filter
        self.benchmark_calcs = benchmark_calcs
        self.metric_calcs = metric_calcs
        
        self.metric_display = metric_display
        self.results_form = results_form
        self.join_tables = join_tables
        self.extra_lines_to_plot = extra_lines_to_plot
        
        self.tca_type = tca_type
        self.reporting_currency = reporting_currency
        self.dummy_market = dummy_market
        self.summary_display = summary_display
        self.access_control = access_control
        self.use_multithreading = use_multithreading
        self.multithreading_params = multithreading_params
        self.data_norm = data_norm
        
        self.tca_provider = tca_provider

        # For market analysis we don't want to load any trade data at all
        if self.tca_type == 'market-analysis':
            self.trade_data_store = None
            self.trade_order_mapping = None

    def _listify(self, prop):
        if not (isinstance(prop, list)) and prop is not None:
            prop = [prop]

        if prop is None: prop = []

        return prop

    @property
    def market_data_store(self):
        return self.__market_data_store

    @market_data_store.setter
    def market_data_store(self, market_data_store):
        self.__market_data_store = self._check_data_store(market_data_store)

    @property
    def trade_data_store(self):
        return self.__trade_data_store

    @trade_data_store.setter
    def trade_data_store(self, trade_data_store):
        self.__trade_data_store = self._check_data_store(trade_data_store)

    @property
    def market_data_offset_ms(self):
        return self.__market_data_offset_ms

    @market_data_offset_ms.setter
    def market_data_offset_ms(self, market_data_offset_ms):
        self.__market_data_offset_ms = market_data_offset_ms
        
    @property
    def trade_data_offset_ms(self):
        return self.__trade_data_offset_ms

    @trade_data_offset_ms.setter
    def trade_data_offset_ms(self, trade_data_offset_ms):
        self.__trade_data_offset_ms = trade_data_offset_ms

    @property
    def bid_benchmark(self):
        return self.__bid_benchmark

    @bid_benchmark.setter
    def bid_benchmark(self, bid_benchmark):
        self.__bid_benchmark = bid_benchmark

    @property
    def ask_benchmark(self):
        return self.__ask_benchmark

    @ask_benchmark.setter
    def ask_benchmark(self, ask_benchmark):
        self.__ask_benchmark = ask_benchmark

    @property
    def venue(self):
        return self.__venue

    @venue.setter
    def venue(self, venue):
        if isinstance(venue, str):
            venue = [venue]

        self.__venue = venue

    @property
    def trade_order_filter(self):
        return self.__trade_order_filter

    @trade_order_filter.setter
    def trade_order_filter(self, trade_order_filter):
        self.__trade_order_filter = self._listify(trade_order_filter)

    @property
    def benchmark_calcs(self):
        return self.__benchmark_calcs

    @benchmark_calcs.setter
    def benchmark_calcs(self, benchmark_calcs):
        self.__benchmark_calcs = self._listify(benchmark_calcs)

    @property
    def metric_calcs(self):
        return self.__metric_calcs

    @metric_calcs.setter
    def metric_calcs(self, metric_calcs):
        self.__metric_calcs = self._listify(metric_calcs)

    @property
    def metric_display(self):
        return self.__metric_display

    @metric_display.setter
    def metric_display(self, metric_display):
        self.__metric_display = self._listify(metric_display)

    @property
    def results_form(self):
        return self.__results_form

    @results_form.setter
    def results_form(self, results_form):
        self.__results_form = self._listify(results_form)

    @property
    def join_tables(self):
        return self.__join_tables

    @join_tables.setter
    def join_tables(self, join_tables):
        self.__join_tables = self._listify(join_tables)
        
    @property
    def extra_lines_to_plot(self):
        return self.__extra_lines_to_plot

    @extra_lines_to_plot.setter
    def extra_lines_to_plot(self, extra_lines_to_plot):
        self.__extra_lines_to_plot = self._listify(extra_lines_to_plot)

    @property
    def tca_type(self):
        return self.__tca_type

    @tca_type.setter
    def tca_type(self, tca_type):
        self.__tca_type = tca_type

    @property
    def reporting_currency(self):
        return self.__reporting_currency

    @reporting_currency.setter
    def reporting_currency(self, reporting_currency):
        self.__reporting_currency = reporting_currency

    @property
    def dummy_market(self):
        return self.__dummy_market

    @dummy_market.setter
    def dummy_market(self, dummy_market):
        self.__dummy_market = dummy_market

    @property
    def summary_display(self):
        return self.__summary_display

    @summary_display.setter
    def summary_display(self, summary_display):
        self.__summary_display = summary_display
        
    @property
    def tca_provider(self):
        return self.__tca_provider

    @tca_provider.setter
    def tca_provider(self, tca_provider):
        self.__tca_provider = tca_provider

class ValidateRequest(object):

    def _raise_validation_error(self, err):
        logger = LoggerManager().getLogger(__name__)

        logger.error(err)

        raise ValidationException(err)

    def validate_request(self, request):
        # constants = Constants()

        if request.start_date > request.finish_date:
            self._raise_validation_error("Start date is after finish date: " + str(request.start_date) + " - " + str(request.finish_date))

        if request.finish_date > datetime.datetime.utcnow().replace(tzinfo=pytz.utc):
            self._raise_validation_error("Finish date can't be in the future: " + str(request.finish_date))

        ticker = request.ticker

        if not(isinstance(ticker, list)):
            ticker = [ticker]

        for t in ticker:
            if t is None:
                self._raise_validation_error("Ticker can't be null!")
            elif len(t) > 6 and request.asset_class != 'fx':
                self._raise_validation_error("Ticker " + str(t) + " is not in the correct format.")
            elif t not in constants.available_tickers_dictionary.keys() \
                    and t not in constants.available_tickers_dictionary['All']:
                self._raise_validation_error("Ticker " + str(t) + " is not in the available list of _tickers!")

        venue = request.venue

        if venue is not None:
            if not(isinstance(venue, list)):
                venue = [venue]

                for v in venue:
                    if v not in constants.available_venues_dictionary.keys() \
                            and v not in constants.available_venues_dictionary['All']:
                        self._raise_validation_error("Venue " + str(v) + " is not in the available list of venues!")

        if isinstance(request, TCARequest):
            if request.tca_type not in ['detailed', 'aggregated', 'compliance', 'market-analysis']:
                self._raise_validation_error(str(request.tca_type) + " is not a valid TCA type.")
        elif isinstance(request, TradeRequest):
            pass
        elif isinstance(request, MarketRequest):
            pass

