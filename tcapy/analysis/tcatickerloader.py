from __future__ import division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import datetime

import pandas as pd

# from tcapy project
from tcapy.data.datafactory import DataFactory

from tcapy.analysis.algos.benchmark import *
from tcapy.analysis.algos.metric import MetricExecutedPriceNotional

from tcapy.conf.constants import Constants
from tcapy.util.fxconv import FXConv
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.timeseries import TimeSeriesOps
from tcapy.util.utilfunc import UtilFunc

from tcapy.analysis.tradeorderfilter import TradeOrderFilterTag
from tcapy.analysis.dataframeholder import DataFrameHolder

from tcapy.analysis.tcarequest import TCARequest, MarketRequest, TradeRequest

constants = Constants()

# compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

class TCATickerLoader(ABC):
    """This class is designed to load up market and trade data for single tickers and also makes appropriate metric calculations
    for that specific ticker. It is generally called by the higher level TCAMarketTradeLoader class, which can handle multiple tickers.

    """

    def __init__(self, version=constants.tcapy_version):
        self._data_factory = DataFactory(version=version)

        self._util_func = UtilFunc()  # general utility operations (such as flatten lists)
        self._fx_conv = FXConv()  # for determining if FX crosses are in the correct convention
        self._time_series_ops = TimeSeriesOps()  # time series operations, such as filtering by date

        self._metric_executed_price = MetricExecutedPriceNotional()  # for determining the executed notionals/price of orders
        # from trades

        self._benchmark_mid = BenchmarkMid()  # to calculate mid price from bid/ask quote market data
        self._trade_order_tag = TradeOrderFilterTag()  # to filter trade/orders according to the values of certain tags
        self._version = version

    def get_market_data(self, market_request):
        """Gets market data for a particular ticker. When we ask for non-standard FX crosses, only the mid-field is
        returned (calculated as a cross rate). We do not give bid/ask quotes for calculated non-standard tickers, as these
        can difficult to estimate.

        Parameters
        ----------
        market_request : MarketRequest
            The type of market data to get

        Returns
        -------
        DataFrame
        """
        logger = LoggerManager.getLogger(__name__)

        if isinstance(market_request, TCARequest):
            market_request = MarketRequest(market_request=market_request)

        old_ticker = market_request.ticker

        if market_request.asset_class == 'fx':
            # Check if we can get ticker directly or need to create synthetic cross rates
            ticker = self._fx_conv.correct_notation(market_request.ticker)
        else:
            # If not FX we don't have to invert
            ticker = old_ticker

        # If ticker is in the correct convention is in crosses where we collect data (typically this will be the USD
        # crosses, also some liquid non-USD pairs like EURJPY)

        # available_tickers = []

        if 'csv' in market_request.data_store or 'h5' in market_request.data_store or 'gzip' in market_request.data_store or \
                isinstance(market_request.data_store, pd.DataFrame) :
            # For CSV (or H5) we don't have much choice, and could differ between CSV files (if CSV has 'ticker' field, will
            # match on that)
            available_tickers = [ticker]
        elif market_request.data_store in constants.market_data_tickers:
            available_tickers = self._util_func.dict_key_list(
                constants.market_data_tickers[market_request.data_store].keys())

        else:
            err_msg = 'Ticker ' + str(
                ticker) + " doesn't seem available in the data source " + market_request.data_store

            logger.error(err_msg)

            raise Exception(err_msg)

        if ticker in available_tickers:

            # In the correct convention or is not FX
            if ticker == old_ticker:
                market_df = self._get_correct_convention_market_data(market_request)

            # Otherwise need to flip to the correct convention (only will return 'mid')
            else:
                market_request_flipped = MarketRequest(market_request=market_request)
                market_request_flipped.ticker = ticker

                market_df = self._invert_quoting_market(
                    self._get_correct_convention_market_data(market_request_flipped))

                if 'ticker' in market_df.columns:
                    market_df['ticker'] = old_ticker
        else:
            if market_request.asset_class == 'fx' and market_request.instrument == 'spot':
                # Otherwise we need to get both legs
                # eg. for NZDCAD, we shall download NZDUSD and USDCAD => multiply them to get NZDCAD

                # get the USD crosses for each leg and then multiply
                market_request_base = MarketRequest(market_request=market_request)
                market_request_terms = MarketRequest(market_request=market_request)

                market_request_base.ticker = old_ticker[0:3] + 'USD'
                market_request_terms.ticker = 'USD' + old_ticker[3:7]

                tickers_exist = self._fx_conv.currency_pair_in_list(
                        self._fx_conv.correct_notation(market_request_base.ticker), available_tickers) and \
                        self._fx_conv.currency_pair_in_list(
                            self._fx_conv.correct_notation(market_request_terms.ticker), available_tickers)

                # If both USD tickers don't exist try computing via EUR tickers? (eg. USDSEK from EURUSD & EURSEK)
                if not(tickers_exist):
                    market_request_base.ticker = old_ticker[0:3] + 'EUR'
                    market_request_terms.ticker = 'EUR' + old_ticker[3:7]

                    tickers_exist = self._fx_conv.currency_pair_in_list(
                        self._fx_conv.correct_notation(market_request_base.ticker), available_tickers) and \
                                    self._fx_conv.currency_pair_in_list(
                                        self._fx_conv.correct_notation(market_request_terms.ticker), available_tickers)

                # Check if that currency (in the CORRECT convention) is in the available tickers
                # we will typically not collect market data for currencies in their wrong convention
                if tickers_exist:

                    fields_try = ['bid', 'ask', 'mid']

                    market_base_df = self.get_market_data(market_request_base)
                    market_terms_df = self.get_market_data(market_request_terms)

                    fields = []

                    for f in fields_try:
                        if f in market_base_df.columns and f in market_terms_df.columns:
                            fields.append(f)

                    # Remove any other columns (eg. with ticker name etc.)
                    market_base_df = market_base_df[fields]
                    market_terms_df = market_terms_df[fields]

                    # Need to align series to multiply (and then fill down points which don't match)
                    # can't use interpolation, given that would use FUTURE data
                    market_base_df, market_terms_df = market_base_df.align(market_terms_df, join="outer")
                    market_base_df = market_base_df.fillna(method='ffill')
                    market_terms_df = market_terms_df.fillna(method='ffill')

                    market_df = pd.DataFrame(data=market_base_df.values * market_terms_df.values, columns=fields,
                                             index=market_base_df.index)

                    # Values at the start of the series MIGHT be nan, so need to ignore those
                    market_df = market_df.dropna(subset=['mid'])

                    if 'ticker' in market_df.columns:
                        market_df['ticker'] = old_ticker

                else:
                    # Otherwise couldn't compute either from the USD legs or EUR legs
                    logger.warn("Couldn't find market data for ticker: " + str(ticker))

                    return None
            else:
                # Otherwise couldn't find the non-FX ticker
                logger.warn("Couldn't find market data for ticker: " + str(ticker))

                return None

        return market_df

    def get_trade_order_data(self, tca_request, trade_order_type, start_date=None, finish_date=None):
        """Gets trade data for specified parameters (eg. start/finish dates tickers). Will also try to find trades
        when they have booked in the inverted market convention, and change the fields appropriately. For example, if
        we ask for GBPUSD trade data, it will also search for USDGBP and convert those trades in the correct convention.

        Parameters
        ----------
        tca_request : TCARequest
            What type of trade data do we want

        trade_order_type : str
            Do we want trade or order data?

        Returns
        -------
        DataFrame
        """
        logger = LoggerManager().getLogger(__name__)

        # by default, assume we want trade data (rather than order data)
        if trade_order_type is None:
            trade_order_type = 'trade_df'

        if start_date is None and finish_date is None:
            start_date = tca_request.start_date
            finish_date = tca_request.finish_date

        # Create request for actual executed trades
        trade_request = TradeRequest(trade_request=tca_request)

        trade_request.start_date = start_date; trade_request.finish_date = finish_date
        trade_request.trade_order_type = trade_order_type

        # Fetch all the trades done in that ticker (will be sparse-like randomly spaced tick data)
        # assumed to be the correct convention (eg. GBPUSD)
        trade_df = self._data_factory.fetch_table(data_request=trade_request)

        # if fx see if inverted or not
        if tca_request.asset_class == 'fx' and tca_request.instrument == 'spot':
            # Also fetch data in the inverted cross (eg. USDGBP) as some trades may be recorded this way
            inv_trade_request = TradeRequest(trade_request=tca_request)

            inv_trade_request.start_date = start_date;
            inv_trade_request.finish_date = finish_date
            inv_trade_request.trade_order_type = trade_order_type

            inv_trade_request.ticker = self._fx_conv.reverse_notation(trade_request.ticker)

            trade_inverted_df = self._data_factory.fetch_table(data_request=inv_trade_request)

            # Only add inverted trades if they exist!
            if not (trade_inverted_df.empty):

                invert_price_columns = ['executed_price', 'price_limit', 'market_bid', 'market_mid', 'market_ask',
                                        'arrival_price']
                invert_price_columns = [x for x in invert_price_columns if x in trade_inverted_df.columns]

                # For trades (but not orders), there is an executed price field, which needs to be inverted
                if invert_price_columns != []:
                    trade_inverted_df[invert_price_columns] = 1.0 / trade_inverted_df[invert_price_columns].values

                trade_inverted_df['side'] = -trade_inverted_df['side']  # buys become sells, and vice versa!
                trade_inverted_df['ticker'] = trade_request.ticker

                if trade_df is not None:
                    trade_df = trade_df.append(trade_inverted_df)
                    trade_df = trade_df.sort_index()
                else:
                    trade_df = trade_inverted_df

        # Check if trade data is not empty? if it is return None
        if self._check_is_empty_trade_order(trade_df, tca_request, start_date, finish_date, trade_order_type):
            return None

        if tca_request.asset_class == 'fx' and tca_request.instrument == 'spot':

            # Check if any notionals of any trade/order are quoted in the TERMS currency?
            terms_notionals = trade_df['notional_currency'] == tca_request.ticker[3:6]

            # If any notional are quoted as terms, we should invert these so we quote notionals with base currency
            # for consistency
            if terms_notionals.any():
                inversion_ticker = tca_request.ticker[3:6] + tca_request.ticker[0:3]

                inversion_spot, trade_df = self._fill_reporting_spot(inversion_ticker, trade_df, start_date,
                                                                     finish_date, tca_request)

                notional_fields = ['notional', 'order_notional', 'executed_notional']

                # Need to check terms notionals again, as trade data could have shrunk (because can only get trades, where we have market data)
                terms_notionals = trade_df['notional_currency'] == str(tca_request.ticker[3:6])

                # Only get the inversion spot if any terms notionals are quoted wrong way around
                if terms_notionals.any():
                    if inversion_spot is not None:
                        for n in notional_fields:
                            if n in trade_inverted_df.columns:
                                # trade_df[n][terms_notionals] = trade_df[n][terms_notionals].values * inversion_spot[terms_notionals].values
                                trade_df[n][terms_notionals] = pd.Series(index=trade_df.index[terms_notionals.values],
                                                                         data=trade_df[n][terms_notionals].values *
                                                                              inversion_spot[terms_notionals].values)
                    else:
                        logger.warn("Couldn't get spot data for " + inversion_ticker + " to invert notionals. Hence not returning trading data.")

                if terms_notionals.any():
                    trade_df['notional_currency'][terms_notionals] = trade_request.ticker[0:3]

            # Also represent notional is reporting currency notional amount (eg. if we are USD based investors, convert
            # notional to USDs)

            # Using a reporting currency can be particularly useful if we are trying to aggregate metrics from many different
            # currency pairs (and wish to weight by a commonly measured reporting notional)

            # Eg. if we don't have USDUSD, then we need to convert
            if trade_request.ticker[0:3] != tca_request.reporting_currency:

                # So if we have EURJPY, we want to download EURUSD data
                reporting_ticker = trade_request.ticker[0:3] + tca_request.reporting_currency

                reporting_spot, trade_df = self._fill_reporting_spot(
                    reporting_ticker, trade_df, start_date, finish_date, tca_request)

                if reporting_spot is not None:
                    trade_df['notional_reporting_currency_mid'] = reporting_spot.values

                    # trade_df['notional_reporting_currency_mid'] = \
                    #     self._time_series_ops.vlookup_style_data_frame(trade_df.index, market_conversion_df, 'mid')[0].values

                    trade_df['reporting_currency'] = tca_request.reporting_currency

                    columns_to_report = ['executed_notional', 'notional', 'order_notional']

                    for c in columns_to_report:
                        if c in trade_df.columns:
                            trade_df[c + '_in_reporting_currency'] = \
                                trade_df['notional_reporting_currency_mid'].values * trade_df[c]
                else:
                    logger.warn(
                        "Couldn't get spot data to convert notionals into reporting currency. Hence not returning trading data.")

                    return None
            else:
                # ie. USDUSD, so spot is 1
                trade_df['notional_reporting_currency_mid'] = 1.0

                # Reporting currency is the same as the notional of the trade, so no need to convert, just
                # replicate columns
                trade_df['reporting_currency'] = tca_request.reporting_currency

                columns_to_report = ['executed_notional', 'notional', 'order_notional']

                for c in columns_to_report:
                    if c in trade_df.columns:
                        trade_df[c + '_in_reporting_currency'] = trade_df[c]

        return trade_df

    def get_trade_order_holder(self, tca_request):
        logger = LoggerManager.getLogger(__name__)

        logger.debug(
            "Get trade order holder for " + str(tca_request.ticker) + " from " + str(tca_request.start_date)
            + " - " + str(tca_request.finish_date))

        # Get all the trade/orders which have been requested, eg. trade_df and order_df
        # do separate calls given they are assumed to be stored in different database tables
        trade_order_holder = DataFrameHolder()

        if tca_request.trade_order_mapping is not None:
            for trade_order_type in tca_request.trade_order_mapping:
                trade_order_df = self.get_trade_order_data(tca_request, trade_order_type)

                trade_order_holder.add_dataframe(trade_order_df, trade_order_type)

        return trade_order_holder

    def get_market_trade_order_holder(self, tca_request):
        """Gets the both the market data and trade/order data associated with a TCA calculation as a tuple of
        (DataFrame, DataFrameHolder)

        Parameters
        ----------
        tca_request : TCARequest
            Parameters for a TCA calculation

        Returns
        -------
        DataFrame, DataFrameHolder
        """

        logger = LoggerManager.getLogger(__name__)

        logger.debug(
            "Get market and trade/order data for " + str(tca_request.ticker) + " from " + str(tca_request.start_date)
            + " - " + str(tca_request.finish_date))

        # Get all the trade/orders which have been requested, eg. trade_df and order_df
        # do separate calls given they are assumed to be stored in different database tables
        return self.get_market_data(tca_request), \
               self.get_trade_order_holder(tca_request)

    def calculate_metrics_single_ticker(self, market_trade_order_combo, tca_request, dummy_market):
        """Calls auxillary methods to get market/trade data for a single ticker. If necessary splits up the request into
        smaller date chunks to collect market and trade data in parallel (using Celery)

        Parameters
        ----------
        tca_request : TCARequest
            Parameter for the TCA analysis

        dummy_market : bool
            Should we put a dummy variable instead of returning market data

        Returns
        -------
        DataFrame, DataFrameHolder, str
        """

        trade_order_filter = tca_request.trade_order_filter
        benchmark_calcs = tca_request.benchmark_calcs
        metric_calcs = tca_request.metric_calcs
        ticker = tca_request.ticker

        logger = LoggerManager.getLogger(__name__)

        # Reassemble market and trade data from the tuple
        market_df, trade_order_df_dict = self.trim_sort_market_trade_order(
            market_trade_order_combo, tca_request.start_date, tca_request.finish_date, tca_request.ticker)

        valid_market = False

        if market_df is not None:
            if not (market_df.empty):
                valid_market = True

        if len(trade_order_df_dict.keys()) > 0 and valid_market:

            # NOTE: this will not filter orders, only TRADES (as orders do not have venue parameters)
            logger.debug("Filter trades by venue")

            simple_filters = {'venue': tca_request.venue}

            if 'trade_df' in self._util_func.dict_key_list(trade_order_df_dict.keys()):
                for s in simple_filters.keys():
                    trade_order_df_dict['trade_df'] = self._trade_order_tag.filter_trade_order(
                        trade_order_df=trade_order_df_dict['trade_df'],
                        tag_value_combinations={s: simple_filters[s]})

            # Do additional more customised post-filtering of the trade/orders (eg. by broker_id, algo_id)
            if trade_order_filter is not None:
                for a in trade_order_filter:
                    trade_order_df_dict = a.filter_trade_order_dict(trade_order_df_dict=trade_order_df_dict)

            # NOTE: this will not filter orders, only TRADES (as orders do not have event type parameters)
            simple_filters = {'event_type': tca_request.event_type}

            if 'trade_df' in self._util_func.dict_key_list(trade_order_df_dict.keys()):
                for s in simple_filters.keys():
                    trade_order_df_dict['trade_df'] = self._trade_order_tag.filter_trade_order(
                        trade_order_df=trade_order_df_dict['trade_df'],
                        tag_value_combinations={s: simple_filters[s]})

            # Remove any trade/orders which aren't empty
            t_remove = []

            for t in trade_order_df_dict.keys():
                if trade_order_df_dict[t] is None:
                    t_remove.append(t)

                    logger.warning(t + " is empty.. might cause problems later!")
                elif trade_order_df_dict[t].empty:
                    t_remove.append(t)

                    logger.warning(t + " is empty.. might cause problems later!")

            for t in t_remove:
                trade_order_df_dict.pop(t)

            trade_order_list = self._util_func.dict_key_list(trade_order_df_dict.keys())

            # Check if we have any trades/orders left to analyse?
            if len(trade_order_list) == 0:
                logger.error("No trade/orders for " + ticker)
            else:
                # ok we have some trade/orders left to analyse
                if not (isinstance(trade_order_list, list)):
                    trade_order_list = [trade_order_list]

                logger.debug("Calculating derived fields and benchmarks")

                logger.debug("Calculating execution fields")

                # Calculate derived executed fields for orders
                # can only do this if trade_df is also available
                if len(trade_order_df_dict.keys()) > 1 and 'trade_df' in self._util_func.dict_key_list(trade_order_df_dict.keys()):

                    # For the orders, calculate the derived fields for executed notional, trade etc.
                    aggregated_notional_fields = 'executed_notional'

                    # Calculate the derived fields of the orders from the trades
                    # alao calculate any benchmarks for the orders
                    for i in range(1, len(trade_order_list)):
                        # NOTIONAL_EXECUTED: add derived field for executed price and notional executed for the orders
                        trade_order_df_dict[trade_order_list[i]] = self._metric_executed_price.calculate_metric(
                            lower_trade_order_df=trade_order_df_dict[trade_order_list[i - 1]],
                            upper_trade_order_df=trade_order_df_dict[trade_order_list[i]],
                            aggregated_ids=constants.order_name + '_pointer_id',
                            aggregated_notional_fields=aggregated_notional_fields,
                            notional_reporting_currency_spot='notional_reporting_currency_mid'
                        )[0]

                # TODO not sure about this?
                if 'trade_df' in self._util_func.dict_key_list(trade_order_df_dict.keys()):
                    if 'notional' not in trade_order_df_dict['trade_df'].columns:
                        trade_order_df_dict['trade_df']['notional'] = trade_order_df_dict['trade_df']['executed_notional']

                logger.debug("Calculating benchmarks")

                # Calculate user specified benchmarks for each trade order (which has been selected)
                if benchmark_calcs is not None:
                    for b in benchmark_calcs:

                        # For benchmarks which only modify market data (and don't need trade specific information)
                        if isinstance(b, BenchmarkMarket):
                            logger.debug("Calculating " + type(b).__name__ + " for market data")

                            _, market_df = b.calculate_benchmark(market_df=market_df)

                    for i in range(0, len(trade_order_df_dict)):
                        for b in benchmark_calcs:
                            # For benchmarks which need to be generated on a trade by trade basis (eg. VWAP, arrival etc)
                            if not (isinstance(b, BenchmarkMarket)):
                                logger.debug("Calculating " + type(b).__name__ + " for " + trade_order_list[i])

                                if trade_order_df_dict[trade_order_list[i]] is not None:
                                    if not (trade_order_df_dict[trade_order_list[i]].empty):
                                        trade_order_df_dict[trade_order_list[i]], _ = b.calculate_benchmark(
                                            trade_order_df=trade_order_df_dict[trade_order_list[i]],
                                            market_df=market_df,
                                            trade_order_name=trade_order_list[i])

                logger.debug("Calculating metrics")

                # Calculate user specified metrics for each trade order (which has been selected)
                if metric_calcs is not None:
                    for i in range(0, len(trade_order_df_dict)):
                        for m in metric_calcs:
                            logger.debug("Calculating " + type(m).__name__ + " for " + trade_order_list[i])

                            if trade_order_df_dict[trade_order_list[i]] is not None:
                                if not (trade_order_df_dict[trade_order_list[i]].empty):
                                    trade_order_df_dict[trade_order_list[i]], _ = m.calculate_metric(
                                        trade_order_df=trade_order_df_dict[trade_order_list[i]], market_df=market_df,
                                        trade_order_name=trade_order_list[i])

                logger.debug("Completed derived field calculations for " + ticker)

            trade_order_df_dict = self._calculate_additional_metrics(market_df, trade_order_df_dict, tca_request)

            if dummy_market:
                market_df = None

        trade_order_df_keys = self._util_func.dict_key_list(trade_order_df_dict.keys())
        trade_order_df_values = []

        for k in trade_order_df_keys:
            trade_order_df_values.append(trade_order_df_dict[k])

        # print("--- dataframes/keys ---")
        # print(trade_order_df_values)
        # print(trade_order_df_keys)

        return market_df, trade_order_df_values, ticker, trade_order_df_keys

    def _fill_reporting_spot(self, ticker, trade_df, start_date, finish_date, tca_request):
        logger = LoggerManager.getLogger(__name__)

        market_request = MarketRequest(start_date=start_date, finish_date=finish_date,
                                       ticker=ticker, data_store=tca_request.market_data_store,
                                       data_offset_ms=tca_request.market_data_offset_ms,
                                       use_multithreading=tca_request.use_multithreading,
                                       multithreading_params=tca_request.multithreading_params)

        market_conversion_df = self.get_market_data(market_request)

        # Make sure the trades/orders are within the market data (for the purposes of the reporting spot)
        # we don't need to consider the length of the order, JUST the starting point
        trade_df = self.strip_trade_order_data_to_market(trade_df, market_conversion_df, consider_order_length=False)

        reporting_spot = None

        # need to check whether we actually have any trade data/market data
        if trade_df is not None and market_conversion_df is not None:
            if not (trade_df.empty) and not (market_conversion_df.empty):

                try:
                    reporting_spot = \
                        self._time_series_ops.vlookup_style_data_frame(trade_df.index, market_conversion_df, 'mid')[
                            0]

                except:
                    logger.error("Reporting spot is missing for this trade data sample!")

                if reporting_spot is None:
                    market_start_finish = "No market data in this sample. "

                    if market_conversion_df is not None:
                        market_start_finish = "Market data is between " + str(
                            market_conversion_df.index[0]) + " - " + str(market_conversion_df.index[-1]) + ". "

                    logger.warn(market_start_finish)
                    logger.warn("Trade data is between " + str(trade_df.index[0]) + " - " + str(
                        trade_df.index[-1]) + ".")

                    logger.warn(
                        "Couldn't get spot data to convert notionals currency. Hence not returning trading data.")

        return reporting_spot, trade_df

    def _invert_quoting_market(self, market_df):
        """Inverts the quote data for an FX pair (eg. converts USD/GBP to GBP/USD) by calculating the reciprical. Also
        swaps around the bid/ask fields for consistency.

        Parameters
        ----------
        market_df : DataFrame
            Contains market data, typically quote data

        Returns
        -------
        DataFrame
        """

        if isinstance(market_df, pd.Series):
            market_df = pd.DataFrame(market_df)

        if 'mid' in market_df.columns:
            market_df['mid'] = 1.0 / market_df['mid'].values

        # Need to swap around bid/ask when inverting market data!
        if 'bid' in market_df.columns and 'ask' in market_df.columns:

            market_df['bid'] = 1.0 / market_df['ask'].values;
            market_df['ask'] = 1.0 / market_df['bid'].values

        return market_df

    def _get_correct_convention_market_data(self, market_request, start_date=None, finish_date=None):
        """Gets market data for a ticker, when it is in the correct market convention. Otherwise throws an exception.

        Parameters
        ----------
        market_request : MarketRequest
            Parameters for the market data.

        Returns
        -------
        DataFrame
        """

        # Check that cross is in correct convention
        if self._fx_conv.correct_notation(market_request.ticker) != market_request.ticker:
            raise Exception('Method expecting only crosses in correct market convention')

        if start_date is None and finish_date is None:
            start_date = market_request.start_date
            finish_date = market_request.finish_date

        return self._get_underlying_market_data(start_date, finish_date, market_request)

    def _get_underlying_market_data(self, start_date, finish_date, market_request):
        # Create request for market data
        market_request = MarketRequest(start_date=start_date, finish_date=finish_date,
                                       ticker=market_request.ticker, data_store=market_request.data_store,
                                       data_offset_ms=market_request.data_offset_ms,
                                       market_data_database_table=market_request.market_data_database_table)

        # Fetch market data in that ticker (will be tick data)
        market_df = self._data_factory.fetch_table(data_request=market_request)

        # TODO do further filtering of market and trade data as necessary
        if constants.resample_ms is not None:
            market_df = self._time_series_ops.resample_time_series(market_df, resample_ms=constants.resample_ms)

            market_df.dropna(inplace=True)

        ## TODO drop stale quotes for market data and add last update time?

        # Calculate mid market rate, if it doesn't exist
        _, market_df = self._benchmark_mid.calculate_benchmark(market_df=market_df)

        return market_df

    def trim_sort_market_trade_order(self, market_trade_order_tuple, start_date, finish_date, ticker):
        """Takes market and trade/order data, then trims it so that the trade/order data is entirely within the
        start/finish date range of market data. If trade/order data does not fully overlap with the market data
        it can cause problems later when computing metrics/benchmarks.

        Parameters
        ----------
        market_trade_order_tuple : tuple
            Tuple of market data with trade/order data

        start_date : datetime
            Start date of TCA analysis

        finish_date : datetime
            Finish data of TCA analysis

        ticker : str
            Ticker

        Returns
        -------
        DataFrame, DataFrame (dict)
        """
        logger = LoggerManager.getLogger(__name__)

        market_df, trade_order_holder = self._convert_tuple_to_market_trade(market_trade_order_tuple)
        logger.debug("Filter the market date by start/finish date")

        # Check market data and trade data is not empty!
        market_df = self._time_series_ops.filter_start_finish_dataframe(market_df, start_date, finish_date)

        # When reassembling the market data, give user option of sorting it, in case the order of loading was in an odd order
        if market_df is not None and constants.re_sort_market_data_when_assembling:
            if not (market_df.empty):
                logger.debug("Filtered by start/finish date now sorting")

                market_df = market_df.sort_index()

        # Check if there's any market data? if we have none at all, then can't do any TCA, so give up!
        if market_df is None or len(market_df.index) == 0:
            err_msg = "No market data between selected dates for " + ticker + " between " + str(start_date) + " - " \
                      + str(finish_date)

            logger.warn(err_msg)

            # raise DataMissingException(err_msg)

        logger.debug("Combine trade/order data")

        # Combine all the trades in a single dataframe (and also the same for orders)
        # which are placed into a single dict
        trade_order_df_dict = trade_order_holder.get_combined_dataframe_dict()

        # Make sure the trade data is totally within the market data (if trade data is outside market data, then
        # can't calculate any metrics later)
        for k in self._util_func.dict_key_list(trade_order_df_dict.keys()):
            trade_order_df_dict[k] = self.strip_trade_order_data_to_market(trade_order_df_dict[k], market_df)

        valid_trade_order_data = trade_order_holder.check_empty_combined_dataframe_dict(trade_order_df_dict)

        if (not (valid_trade_order_data)):
            err_msg = "No trade/order data between selected dates for " + ticker + " between " + str(start_date) + " - " \
                      + str(finish_date)

            logger.warn(err_msg)

            # raise DataMissingException(err_msg)

        return market_df, trade_order_df_dict

    def strip_trade_order_data_to_market(self, trade_order_df, market_df, consider_order_length=True):
        """Strips down the trade/order data so that it is within the market data provided. Hence, trade/order data
        will fully overlap with the market data.

        Parameters
        ----------
        trade_order_df : DataFrame
            Trade/order data from the client

        market_df : DataFrame
            Market data

        consider_order_length : bool (default: True)
            Should we consider the length of the order, when we consider the overlap?

        Returns
        -------
        DataFrame
        """

        if market_df is not None and trade_order_df is not None:
            if not (market_df.empty) and not (trade_order_df.empty):

                add_cond = True

                # For orders (ensure that the start/end time of every order is within the market data start/finish dates)
                # this is important, given that we often want to calculate benchmarks over orders from market data
                if consider_order_length:

                    if 'benchmark_date_start' in trade_order_df.columns and 'benchmark_date_end' in trade_order_df.columns \
                            and trade_order_df is not None:

                        add_cond = (trade_order_df['benchmark_date_start'] >= market_df.index[0]) & (trade_order_df['benchmark_date_end'] <= market_df.index[-1])

                # for trades (ensure that every trade is within the market data start/finish dates)
                trade_order_df = trade_order_df.loc[(trade_order_df.index >= market_df.index[0]) & (trade_order_df.index <= market_df.index[-1]) & add_cond]

        return trade_order_df

    def _strip_start_finish_dataframe(self, data_frame, start_date, finish_date, tca_request):
        """Strips down the data frame to the dates which have been requested in the initial TCA request

        Parameters
        ----------
        data_frame : DataFrame
            Data to be stripped down

        start_date : datetime
            Start date of the computation

        finish_date : datetime
            Finish date of the computation

        tca_request : TCARequest
            Parameters for the TCA request

        Returns
        -------
        DataFrame
        """

        # print(data_frame)

        if start_date != tca_request.start_date:
            if data_frame is not None:
                if not (data_frame.empty):
                    data_frame = data_frame.loc[data_frame.index >= tca_request.start_date]

        if finish_date != tca_request.finish_date:
            if data_frame is not None:
                if not (data_frame.empty):
                    data_frame = data_frame.loc[data_frame.index <= tca_request.finish_date]

        return data_frame

    def _check_is_empty_trade_order(self, trade_df, tca_request, start_date, finish_date, trade_order_type):

        logger = LoggerManager.getLogger(__name__)

        if trade_df is None:
            logger.warn(
                "Missing trade data for " + tca_request.ticker + " between " + str(start_date) + " - " + str(
                    finish_date) + " in "
                + trade_order_type)

            return True

        elif trade_df.empty:
            logger.warn(
                "Missing trade data for " + tca_request.ticker + " between " + str(start_date) + " - " + str(
                    finish_date) + " in "
                + trade_order_type)

            return True

        return False

    @abc.abstractmethod
    def _calculate_additional_metrics(self, market_df, trade_order_df_dict, tca_request):
        pass

    @abc.abstractmethod
    def _convert_tuple_to_market_trade(self, market_trade_order_tuple):
        pass

    @abc.abstractmethod
    def get_tca_version(self):
        pass