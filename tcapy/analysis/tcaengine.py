from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#


from tcapy.analysis.algos.benchmark import *

from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager
from tcapy.util.timeseries import TimeSeriesOps
from tcapy.util.utilfunc import UtilFunc

from tcapy.analysis.tradeorderfilter import TradeOrderFilterTag

from tcapy.analysis.tcarequest import TCARequest, ValidateRequest

constants = Constants()

from tcapy.util.mediator import Mediator

# compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

class TCAEngine(ABC):
    """TCAEngine is an abstract high level class for TCA computation. This is the entry class for most users.

    It takes in TCARequest objects, which define the precise TCA to be done (eg. what ticker to use? What are the
    start/finish dates?). Implementations of this are expected to use TCAMarketTradeLoader to do most of the computation work.

    It then sends back a large dictionary of DataFrames to the user, containing the results of TCA analysis.
    Typically this includes trades/orders, along with additional derived/calculated fields, which are useful for TCA analysis,
    such as metrics (like slippage), benchmarks (like arrival prices) etc. It also likely includes summaries of the TCA analysis
    such as average slippage across many trades/orders and also the distribution of these metrics.

    Users can also specify to include market data in the DataFrame, if they wish to do their own analysis.

    """

    def __init__(self, version=constants.tcapy_version):
        self._util_func = UtilFunc()

        self._tca_market_trade_loader = Mediator.get_tca_market_trade_loader(version=version)
        self._time_series_ops = TimeSeriesOps()
        self._trade_order_tag = TradeOrderFilterTag()

        logger = LoggerManager.getLogger(__name__)
        logger.info("Init TCAEngine version: " + self._tca_market_trade_loader.get_tca_version() + " - Env: " + constants.env)

    @abc.abstractmethod
    def calculate_tca(self, tca_request):
        """Kicks off the TCA computation, usually by invoking the MarketLoader object.

        Parameters
        ----------
        tca_request : TCARequest
            Parameters which describe TCA analysis

        Returns
        -------
        dict
        """
        pass

    @abc.abstractmethod
    def get_engine_description(self):
        """Returns a description of the TCA object

        Returns
        -------
        str
        """
        pass


class TCAEngineImpl(TCAEngine):
    """This does the computation for TCA style _calculations for a specific currency pair
    for the analysis of trades and orders over a number of days.

    It creates a number of different additional DataFrames, which can be used by a GUI or dumped to disk.
    - sparse combination of market prices and trades/orders alongside these
    - a markout table for all the trades

    """

    def __init__(self, version=constants.tcapy_version):
        super(TCAEngineImpl, self).__init__(version=version)

        self._util_func = UtilFunc()

    def calculate_tca(self, tca_request):
        """Does a full TCA calculation according to various criteria such as:

        - ticker to be examined (eg. EURUSD - must be a single ticker)
        - start and finish dates for the calculation?
        - what benchmarks to use for comparison for each side of the trade?

        Parameters
        ----------
        tca_request : TCARequest
            Defines the parameters for the TCA calculation

        """
        logger = LoggerManager.getLogger(__name__)

        if tca_request.tca_provider == 'internal_tcapy':

            # Check the inputs of the TCARequest are valid
            ValidateRequest().validate_request(tca_request)

            # For detailed TCA analysis
            # this is specifically only for ONE ticker (*always* return the market data to user)
            if tca_request.tca_type == 'detailed':

                # Only allow one ticker when we are doing detailed analysis
                if len(tca_request.ticker) > 1:
                    logger.info("More than 1 ticker specified for TCA detailed computation. Only working on first")

                if isinstance(tca_request.ticker, list):
                    tca_request.ticker = tca_request.ticker[0]

                # Load market/trade data and compute all the TCA metrics/benchmarks etc.
                market_df_dict, trade_order_results_df_dict = self._tca_market_trade_loader.load_market_calculate_summarize_metrics(
                    tca_request, dummy_market=tca_request.dummy_market)

                if market_df_dict is not None:
                    if tca_request.ticker in market_df_dict.keys():
                        trade_order_results_df_dict['market_df'] = market_df_dict[tca_request.ticker]

            # If we want aggregated TCA analysis, typically to later calculate many metrics across many trades and _tickers,
            # as opposed to one specific currency pair
            # Or for market-analysis (which involves purely _calculations on market data WITHOUT any trade/order data)
            elif tca_request.tca_type == 'aggregated' or tca_request.tca_type == 'compliance' or tca_request.tca_type == 'market-analysis':
                tca_request.ticker = self._util_func.populate_field(tca_request.ticker, constants.available_tickers_dictionary)
                tca_request.venue = self._util_func.populate_field(tca_request.venue, constants.available_venues_dictionary,
                                                                   exception_fields='All')

                # Load market/trade data and compute all the TCA metrics/benchmarks/displays
                market_df_dict, trade_order_results_df_dict = self._tca_market_trade_loader.load_market_calculate_summarize_metrics(
                    tca_request, dummy_market=tca_request.dummy_market)

                # Add the market data to our dictionary, for further user analysis later, if desired (generally don't do this
                # because the underlying tick data can be very large!
                if market_df_dict is not None:
                    for k in market_df_dict.keys():
                        trade_order_results_df_dict[k + '_df'] = market_df_dict[k]

        else:
            # In the future will support external TCA providers too
            logger.error("TCA provider " + tca_request.tca_provider + " is not implemented yet!")

            return None

        contains_data = False

        for t in trade_order_results_df_dict:
            if t is None:
                contains_data = contains_data or False
            else:
                if isinstance(trade_order_results_df_dict[t], pd.DataFrame):
                    if not(trade_order_results_df_dict[t].empty):
                        contains_data = True

        if not(contains_data):
            raise DataMissingException("Raise no data for " + str(tca_request.ticker) + " between " + str(tca_request.start_date) +
                                       " - " + str(tca_request.finish_date))

        return trade_order_results_df_dict

    def get_engine_description(self):
        return 'tca-engine-impl'