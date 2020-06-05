from __future__ import division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc  # for abstract classes

# from tcapy project
from tcapy.analysis.tradeorderfilter import TradeOrderFilterTag
from tcapy.analysis.dataframeholder import DataFrameHolder
from tcapy.analysis.tcarequest import TCARequest, MarketRequest

from tcapy.conf.constants import Constants
from tcapy.util.customexceptions import *
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.mediator import Mediator
from tcapy.util.utilfunc import UtilFunc

constants = Constants()

# compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

class TCAMarketTradeLoader(ABC):
    """TCAMarketTradeLoader provides wrapper methods to load market and trade data and also allows adds additional calculated
    fields to the trade data such as metrics (slippage, market impact etc), benchmarks (mid, VWAP, TWAP etc.) etc. as well
    as ways to process this output for display for multiple tickers. Underneath it uses TCATickerLoader, for fetching
    data/calculating metrics for individual tickers.

    Typically, TCAMarketTradeLoader will be called by an instance of TCAEngine. However, it can also be called directly, if we
    simply want to download market or trade data by itself.
    """

    def __init__(self, version=constants.tcapy_version):
        self._util_func = UtilFunc()  # general utility operations (such as flatten lists)
        self._trade_order_tag = TradeOrderFilterTag()  # to filter trade/orders according to the values of certain tags

        self._version = version

    def get_market_data(self, market_request):
        """Gets market data for tickers. When we ask for non-standard FX crosses, only the mid-field is
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

        tca_ticker_loader = Mediator.get_tca_ticker_loader(version=self._version)

        if isinstance(market_request.ticker, list):
            if len(market_request.ticker) > 1:
                market_request_list = self._split_tca_request_into_list(market_request)

                market_df_dict = {}

                for market_request_single in market_request_list:
                    market_df_dict[market_request.ticker] = \
                        tca_ticker_loader(version=self._version).get_market_data(market_request_single)

        return tca_ticker_loader.get_market_data(market_request)

    def get_trade_order_data(self, tca_request, trade_order_type):
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

        tca_ticker_loader = Mediator.get_tca_ticker_loader(version=self._version)

        if isinstance(tca_request.ticker, list):
            if len(tca_request.ticker) > 1:
                tca_request_list = self._split_tca_request_into_list(tca_request)

                trade_df_list = []

                for tca_request_single in tca_request_list:
                    trade_df_list.append(
                        tca_ticker_loader(version=self._version).get_trade_order_data(tca_request_single, trade_order_type))

        df_dict = tca_ticker_loader.get_trade_order_data(tca_request, trade_order_type)

        return df_dict

    def get_trade_order_holder(self, tca_request):
        """Gets the trades/orders in the form of a TradeOrderHolder

        Parameters
        ----------
        tca_request : TCARequest
            Parameters for the TCA computation

        Returns
        -------
        TradeOrderHolder
        """

        tca_ticker_loader = Mediator.get_tca_ticker_loader(version=self._version)

        if isinstance(tca_request.ticker, list):
            if len(tca_request.ticker) > 1:
                tca_request_list = self._split_tca_request_into_list(tca_request)

                trade_order_holder = DataFrameHolder()

                for tca_request_single in tca_request_list:
                    trade_order_holder.add_dataframe_holder(
                        tca_ticker_loader(version=self._version).get_trade_order_holder(tca_request_single))

        return tca_ticker_loader(version=self._version).get_trade_order_holder(tca_request)

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

        return Mediator.get_tca_ticker_loader(version=self._version).get_market_trade_order_holder(
            tca_request)

    def load_market_calculate_summarize_metrics(self, tca_request, dummy_market=False):
        """Splits up the TCA request into individual tickers. Market/trade data is loaded for each ticker, before
        conducting TCA (ie. calculating metrics, benchmarks etc.). Returns a dictionary consisting of market data and
        another dictionary of trade/order data (and any additional results associated with the TCA)

        Parameters
        ----------
        tca_request : TCARequest
            Parameters defining the TCA calculation

        dummy_market : bool, default False
            Do we return market data for future use?

        Returns
        -------
        DataFrame (dict), DataFrame (dict)
        """

        # Load market/trade data and compute metrics/benchmarks etc. per ticker
        market_df_dict, trade_order_results_df_dict, tca_request_list = \
            self.get_market_trade_metrics(tca_request, dummy_market=dummy_market)

        # If every ticker we have selected doesn't have trades (and are analysis also requires trades), can't do any TCA at all
        if len(trade_order_results_df_dict) == 0 and tca_request.trade_data_store is not None \
                and tca_request.trade_order_mapping is None:
            logger = LoggerManager.getLogger(__name__)

            err_msg = "no trade data for specified ticker(s) and time range"

            logger.error(err_msg)

            raise DataMissingException(err_msg)

        # trade_df = trade_order_results_df_dict['trade_df']
        # Now summarize those metrics across all the tickers, for easier display
        return self.summarize_metrics(market_df_dict, trade_order_results_df_dict, tca_request_list,
                                      dummy_market=dummy_market)

    def summarize_metrics(self, market_df_dict, trade_order_results_df_dict, tca_request_list, dummy_market=False):
        """Takes in precomputed metrics across one or more tickers, and summarizes them for later user display
        (should be customised for users)

        Parameters
        ----------
        tca_request_list : TCARequest (list)
            List of TCARequests (typically, each is for a different ticker)

        dummy_market : bool
            Should we output market data for later use?

        Returns
        -------
        DataFrame, DataFrame, DataFrame
        """

        # Allow user defined summary of metrics
        trade_order_results_df_dict = self._apply_summary_metrics(tca_request_list, trade_order_results_df_dict, market_df_dict)

        # Warning: do not ask for market data when requesting more than one ticker, could cause memory leaks!
        if (dummy_market):
            return None, trade_order_results_df_dict

        # Dictionary of market data and a dictionary of trades/orders/results of TCA analysis

        # TODO convert into strings
        return market_df_dict, trade_order_results_df_dict

    def _apply_summary_metrics(self, tca_request_list, trade_order_results_df_dict, market_df_dict):
        return trade_order_results_df_dict

    def get_market_trade_metrics(self, tca_request, dummy_market=False):
        """Collects together all the market and trade data (and computes metrics) for each ticker specified in the
        TCARequest

        Parameters
        ----------
        tca_request : TCARequest
            Parameters for the TCA

        dummy_market : bool (default: False)
            Should dummy market data be returned (requires less memory)?

        Returns
        -------
        DataFrame (dict) , DataFrame (dict), TCARequest (list)
        """

        logger = LoggerManager.getLogger(__name__)

        logger.debug("Start loading trade/data/computation")

        # split up TCARequest into a list of TCA with different tickers
        tca_request_list = self._split_tca_request_into_list(tca_request)

        market_df_dict, trade_order_results_df_dict = self._get_market_trade_metrics(tca_request_list, dummy_market)

        logger.debug("Finished loading data and calculating metrics on individual tickers")

        return market_df_dict, trade_order_results_df_dict, tca_request_list

    def _get_market_trade_metrics(self, tca_request_list, dummy_market):
        """Gets the market and trade data, as well as computed metrics on them

        Parameters
        ----------
        tca_request_list : TCARequest (list)
            Requests for multiple TCARequests (eg. for different tickers)

        dummy_market : bool
            Return dummy market data?

        Returns
        -------
        DataFrame (dict), DataFrame (dict)
        """

        tca_ticker_loader = Mediator.get_tca_ticker_loader(version=self._version)

        market_df_dict = {}

        trade_order_holder_list = DataFrameHolder()

        for tca_request_single in tca_request_list:
            market_df, trade_order_df_dict = tca_ticker_loader.get_market_trade_order_holder(tca_request_single)

            market_df, trade_order_df_list, ticker, trade_order_keys = \
                tca_ticker_loader.calculate_metrics_single_ticker((market_df, trade_order_df_dict),
                                                                        tca_request_single, dummy_market)

            market_df_dict[ticker] = market_df

            trade_order_holder_list.add_dataframe_dict(dict(zip(trade_order_keys, trade_order_df_list)))

        # Unpack the DataFrameHolder into a dictionary (combining the lists of trade, orders etc. into single dataframes)
        # this may also decompress the trades
        trade_order_results_df_dict = trade_order_holder_list.get_combined_dataframe_dict()

        return market_df_dict, trade_order_results_df_dict

    def _split_tca_request_into_list(self, tca_request):
        """Splits a TCA request by ticker.

        Parameters
        ----------
        tca_request : TCARequest
            TCA request to broken up into tickers

        Returns
        -------
        TCARequest(list)
        """

        ticker = tca_request.ticker

        if not (isinstance(ticker, list)):
            ticker = [ticker]

        tca_request_list = []

        # go through every ticker (and also split into list)
        for tick in ticker:
            tca_request_temp = TCARequest(tca_request=tca_request)
            tca_request_temp.ticker = tick

            tca_request_list.append(tca_request_temp)

        return self._util_func.flatten_list_of_lists(tca_request_list)

    @abc.abstractmethod
    def get_tca_version(self):
        pass





