from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.analysis.tcaengine import TCAEngineImpl
from tcapy.analysis.tcarequest import TCARequest

from tcapy.util.loggermanager import LoggerManager

logger = LoggerManager.getLogger(__name__)

# 'dukascopy' or 'ncfx'
data_source = 'dukascopy'

# Change the market and trade data store as necessary
market_data_store = 'arctic-' + data_source

ticker = 'EURUSD'
tca_type = 'aggregated'
bid_mid_bp = 0.1;
ask_mid_bp = 0.1

use_multithreading = False


def get_sample_data():
    from tcapy.analysis.algos.benchmark import BenchmarkMarketSpreadToMid
    logger.info("About to load data for " + ticker)

    tca_request = TCARequest(start_date='01 May 2017', finish_date='15 May 2017', ticker=ticker, trade_data_store='mysql',
                             market_data_store=market_data_store,
                             benchmark_calcs=[BenchmarkMarketSpreadToMid(bid_mid_bp=bid_mid_bp, ask_mid_bp=ask_mid_bp)],
                             trade_order_mapping=['trade_df'], tca_type=tca_type, use_multithreading=use_multithreading)

    tca_engine = TCAEngineImpl()

    trade_order_results_df_dict = tca_engine.calculate_tca(tca_request)
    trade_df = trade_order_results_df_dict['trade_df']

    return trade_order_results_df_dict[ticker + '_df'], trade_df

def example_calculate_market_impact():
    """Calculates the transient market impact for a trade
    """
    from tcapy.analysis.algos.metric import MetricTransientMarketImpact

    market_df, trade_df = get_sample_data()

    metric_market_impact = MetricTransientMarketImpact()
    metric_market_impact.calculate_metric(trade_df, market_df)

    print(trade_df)

def example_calculate_slippage_with_bid_mid_spreads():
    """Calculate the slippage for trades given market data as a benchmark
    """
    from tcapy.analysis.algos.metric import MetricSlippage

    market_df, trade_df = get_sample_data()

    metric_slippage = MetricSlippage()
    trade_df, _ = metric_slippage.calculate_metric(trade_df, market_df)

    print(trade_df)


if __name__ == '__main__':
    import time

    start = time.time()

    example_calculate_market_impact()
    example_calculate_slippage_with_bid_mid_spreads()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
