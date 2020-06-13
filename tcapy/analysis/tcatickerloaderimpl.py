from __future__ import division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#
import pandas as pd

from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.mediator import Mediator

from tcapy.data.volatilecache import CacheHandle
from tcapy.vis.displaylisteners import PlotRender

from tcapy.analysis.dataframeholder import DataFrameHolder
from tcapy.analysis.tcatickerloader import TCATickerLoader
from tcapy.analysis.tcarequest import MarketRequest

constants = Constants()

class TCATickerLoaderImpl(TCATickerLoader):
    """This add caching of market and trade data in chunks of monthly or other arbitrary periods (usually in Redis) to 
    speed up data fetching rather than hammering a database repeatedly.
    """

    def __init__(self, version=constants.tcapy_version, volatile_cache_engine=constants.volatile_cache_engine):
        super(TCATickerLoaderImpl, self).__init__(version=version, volatile_cache_engine=volatile_cache_engine)

        self._plot_render = PlotRender()

    def _convert_tuple_to_market_trade(self, market_trade_order_tuple):
        volatile_cache = Mediator.get_volatile_cache(volatile_cache_engine=self._volatile_cache_engine)

        # Gather market and trade/order data (which might be stored in a list)
        if isinstance(market_trade_order_tuple, list):
            market_df_list = []
            trade_order_holder = DataFrameHolder()

            for market_df_single, trade_order_holder_single in market_trade_order_tuple:
                market_df_list.append(market_df_single)

                trade_order_holder.add_dataframe_holder(trade_order_holder_single)

            market_df_list = volatile_cache.get_dataframe_handle(market_df_list, burn_after_reading=True)

            # to ensure that any spurious/None elements are removed
            market_df_list = [x for x in market_df_list if isinstance(x, pd.DataFrame)]

            # want to make sure the data is properly ordered too (not guarenteed we'll get it back in right order)
            market_df = self._time_series_ops.concat_dataframe_list(market_df_list)

        else:
            market_df = volatile_cache.get_dataframe_handle(market_trade_order_tuple[0], burn_after_reading=True)
            trade_order_holder = market_trade_order_tuple[1]

        return market_df, trade_order_holder

    def get_market_trade_order_holder(self, tca_request, return_cache_handles=True):
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
        # by default these will be returned as CacheHandles, which are easier to pass around Celery
        return self.get_market_data(tca_request, return_cache_handles=return_cache_handles), \
               self.get_trade_order_holder(tca_request)

    def get_market_data(self, market_request, return_cache_handles=False):
        # Handles returns a pointer

        volatile_cache = Mediator.get_volatile_cache(volatile_cache_engine=self._volatile_cache_engine)

        cache = True

        # Don't attempt to cache DataFrames
        if hasattr(market_request, 'market_data_store'):
            if (isinstance(market_request.market_data_store, pd.DataFrame)):
                cache = False
        elif isinstance(market_request.data_store, pd.DataFrame):
            cache = False

        # If we have allowed the caching of monthly/periodic market data
        if market_request.multithreading_params['cache_period_market_data'] and cache:
            old_start_date = market_request.start_date;
            old_finish_date = market_request.finish_date

            # so we can also take TCARequest objects
            if hasattr(market_request, 'market_data_store'):
                data_store = market_request.market_data_store
                data_offset_ms = market_request.market_data_offset_ms
            else:
                data_store = market_request.data_store
                data_offset_ms = market_request.data_offset_ms

            # See if we can fetch from the cache (typically Redis)
            start_date, finish_date, market_key, market_df = \
                volatile_cache.get_data_request_cache(market_request, data_store, 'market_df',
                                                     data_offset_ms)

            # If data is already cached, just return the existing CacheHandle (which is like a pointer to the reference
            # in Redis)
            if market_df is not None and start_date == old_start_date and finish_date == old_finish_date and return_cache_handles:
                return CacheHandle(market_key, add_time_expiry=False)

            if market_df is None:

                market_request_copy = MarketRequest(market_request=market_request)
                market_request_copy.start_date = start_date
                market_request_copy.finish_date = finish_date

                market_df = super(TCATickerLoaderImpl, self).get_market_data(market_request_copy)

                volatile_cache.put_data_request_cache(market_request_copy, market_key, market_df)

            market_df = self._strip_start_finish_dataframe(market_df, old_start_date, old_finish_date, market_request)
        else:
            market_df = super(TCATickerLoaderImpl, self).get_market_data(market_request)

        # Return as a cache handle (which can be easily passed across Celery for example)
        # Only if use_multithreading
        if return_cache_handles and market_request.use_multithreading:
            return volatile_cache.put_dataframe_handle(market_df,
                use_cache_handles=market_request.multithreading_params['cache_period_market_data'])

        return market_df

    def get_trade_order_data(self, tca_request, trade_order_type, start_date=None, finish_date=None, return_cache_handles=True):
        # return_cache_handles returns a pointer

        logger = LoggerManager().getLogger(__name__)
        volatile_cache = Mediator.get_volatile_cache(volatile_cache_engine=self._volatile_cache_engine)

        # by default, assume we want trade data (rather than order data)
        if trade_order_type is None:
            trade_order_type = 'trade_df'

        trade_order_contents = tca_request.trade_order_mapping[trade_order_type]

        cache = True

        # Don't attempt to catch DataFrames (or CSVs of trades)
        if isinstance(trade_order_contents, pd.DataFrame):
            cache = False
        elif isinstance(trade_order_contents, str):
            if 'csv' in trade_order_contents:
                cache = False

        # If we have allowed the caching of monthly/weekly trade data
        if tca_request.multithreading_params['cache_period_trade_data'] and cache:
            old_start_date = tca_request.start_date; old_finish_date = tca_request.finish_date

            # See if we can fetch from the cache (usually Redis)
            start_date, finish_date, trade_key, trade_df = \
                volatile_cache.get_data_request_cache(
                    tca_request, tca_request.trade_data_store, trade_order_type, tca_request.trade_data_offset_ms)

            # If data is already cached, just return the existing CacheHandle
            if trade_df is not None and start_date == old_start_date and finish_date == old_finish_date:
                return CacheHandle(trade_key, add_time_expiry=False)

            # If it wasn't in the cache then fetch it and push into the cache
            if trade_df is None:
                logger.debug('Key not found for ' + trade_key + ".. now need to load")

                # Call the superclass (get back DataFrames not return_cache_handles)
                trade_df = super(TCATickerLoaderImpl, self).get_trade_order_data(tca_request, trade_order_type,
                                                                                 start_date=start_date,
                                                                                 finish_date=finish_date)

                # Cache this periodic monthly/weekly data
                volatile_cache.put_data_request_cache(tca_request, trade_key, trade_df)

            # Strip off the start/finish dates (because when we load from cache, we get full months)
            trade_df = self._strip_start_finish_dataframe(trade_df, start_date, finish_date, tca_request)
        else:
            if start_date is None or finish_date is None:
                start_date = tca_request.start_date
                finish_date = tca_request.finish_date

            # Call the superclass (get back DataFrames not return_cache_handles)
            trade_df = super(TCATickerLoaderImpl, self).get_trade_order_data(tca_request, trade_order_type,
                                                                             start_date=start_date,
                                                                             finish_date=finish_date)

        if return_cache_handles and tca_request.use_multithreading:
            # Return as a cache handle (which can be easily passed across Celery for example)
            return volatile_cache.put_dataframe_handle(trade_df,
                                                       use_cache_handles=tca_request.multithreading_params['cache_period_trade_data'])

        return trade_df

    def calculate_metrics_single_ticker(self, market_trade_order_combo, tca_request, dummy_market):

        volatile_cache = Mediator.get_volatile_cache(volatile_cache_engine=self._volatile_cache_engine)

        market_df, trade_order_df_values, ticker, trade_order_df_keys \
            = super(TCATickerLoaderImpl, self).calculate_metrics_single_ticker(market_trade_order_combo, tca_request, dummy_market)

        if tca_request.use_multithreading:
            # Return as a cache handle (which can be easily passed across Celery for example) or not for the market
            # and trade/order data
            return volatile_cache.put_dataframe_handle(market_df, tca_request.multithreading_params['return_cache_handles_market_data']), \
                    volatile_cache.put_dataframe_handle(trade_order_df_values, tca_request.multithreading_params['return_cache_handles_trade_data']), \
                    ticker, trade_order_df_keys
        else:
            # For single threading, don't use cache handles (no point, because sharing in the same memory space)
            return market_df, trade_order_df_values, ticker, trade_order_df_keys

    def _get_correct_convention_market_data(self, market_request, start_date=None, finish_date=None):
        # Check that cross is in correct convention
        if self._fx_conv.correct_notation(market_request.ticker) != market_request.ticker:
            raise Exception('Method expecting only crosses in correct market convention')

        cache = True

        if isinstance(market_request.data_store, pd.DataFrame):
            cache = False

        if market_request.multithreading_params['cache_period_market_data'] and cache:
            volatile_cache = Mediator.get_volatile_cache(volatile_cache_engine=self._volatile_cache_engine)

            start_date, finish_date, market_key, market_df = \
                volatile_cache.get_data_request_cache(market_request, market_request.data_store, 'market_df',
                                                      market_request.data_offset_ms)

            if market_df is None:
                market_df = super(TCATickerLoaderImpl, self)._get_underlying_market_data(start_date, finish_date, market_request)

                volatile_cache.put_data_request_cache(market_request, market_key, market_df)

            return self._strip_start_finish_dataframe(market_df, start_date, finish_date, market_request)
        else:
            if start_date is None or finish_date is None:
                start_date = market_request.start_date
                finish_date = market_request.finish_date

            return super(TCATickerLoaderImpl, self)._get_underlying_market_data(start_date, finish_date,
                                                                                     market_request)


    def _calculate_additional_metrics(self, market_df, trade_order_df_dict, tca_request):
        logger = LoggerManager.getLogger(__name__)

        # Add candlesticks/sparse DataFrames for plotting if requested
        if tca_request.tca_type == 'detailed' or tca_request.summary_display == 'candlestick':

            trade_order_list = self._util_func.dict_key_list(trade_order_df_dict.keys())

            # only add the ticker name if we have a non-detailed plot to differentiate between currency pairs
            if tca_request.tca_type == 'detailed':
                ticker_label = ''
            else:
                ticker_label = tca_request.ticker + '_'

            logger.debug("Generating downsampled market data for potentional display")

            market_downsampled_df = self._time_series_ops.downsample_time_series_usable(market_df)

            # Combine downsampled market data with trade data
            fields = ['bid', 'ask', 'open', 'high', 'low', 'close', 'mid', 'vwap', 'twap',
                      'arrival', 'buy_trade', 'sell_trade', 'notional', 'executed_notional', 'executed_price',
                      'side']

            for f in tca_request.extra_lines_to_plot:
                fields.append(f)

            # create a sparse representation of the trades/orders which can later be displayed to users
            for trade_order in trade_order_list:
                if trade_order in trade_order_df_dict:
                    trade_order_df_dict[ticker_label + 'sparse_market_' + trade_order] = \
                        self._join_market_downsampled_trade_orders(market_downsampled_df,
                                                                   trade_order_df_dict[trade_order],
                                                                   fields=fields)

            trade_order_df_dict[ticker_label + 'market_df_downsampled'] = market_downsampled_df

            trade_order_df_dict[ticker_label + 'candlestick_fig'] = \
                    self._plot_render.generate_candlesticks(market_downsampled_df)

            if tca_request.summary_display == 'candlestick':
                for trade_order in trade_order_list:
                    if trade_order in trade_order_df_dict:
                        title = ticker_label + " " + trade_order
                        lines_to_plot = self._util_func.dict_key_list(constants.detailed_timeline_plot_lines.keys())
                        lines_to_plot.append('candlestick')

                        trade_order_df_dict[ticker_label + 'sparse_market_' + trade_order.replace('df', 'fig')]\
                            = self._plot_render.plot_market_trade_timeline(
                            title=title, sparse_market_trade_df=trade_order_df_dict[ticker_label + 'sparse_market_' + trade_order],
                            lines_to_plot=lines_to_plot,
                            candlestick_fig=trade_order_df_dict[ticker_label + 'candlestick_fig'])

        return trade_order_df_dict

    def _join_market_downsampled_trade_orders(self, market_downsampled_df, trade_order_df, fields=None):
        """Combine market data with trade/orders, into a sparse DataFrame. Typically, used when preparing to display
        a mixture of market/trades data together.

        Parameters
        ----------
        market_downsampled_df : DataFrame
            Market data which has been downsampled

        trade_order_df : DataFrame
            Trade/order data to be combined

        fields : str (list)
            Fields to keep

        Returns
        -------
        DataFrame
        """

        logger = LoggerManager.getLogger(__name__)

        if fields is not None:
            trade_order_df = self._time_series_ops.filter_time_series_by_matching_columns(trade_order_df, fields)

        logger.debug('About to join')

        sparse_market_trade_df = market_downsampled_df.join(trade_order_df, how='outer')

        # Add buy/sell trade prices in new columns (easier for plotting later)
        if 'executed_price' not in sparse_market_trade_df.columns:
            print('x')

        executed_price = sparse_market_trade_df['executed_price'].values
        side_to_match = sparse_market_trade_df['side'].values

        sparse_market_trade_df['buy_trade'] \
            = self._time_series_ops.nanify_array_based_on_other(side_to_match, -1, executed_price)  # make sells NaN (NOT buys!)
        sparse_market_trade_df['sell_trade'] \
            = self._time_series_ops.nanify_array_based_on_other(side_to_match, 1, executed_price)   # make buys NaN (NOT sells!)

        logger.debug('Finished joining')

        return sparse_market_trade_df

    def get_tca_version(self):
        return 'pro'

