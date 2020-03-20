from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import pandas as pd

from tcapy.conf.constants import Constants

from tcapy.analysis.tcarequest import TCARequest
from tcapy.util.mediator import Mediator

from tcapy.data.databasesource import DatabaseSourceNCFX
from tcapy.data.databasesource import DatabaseSourceArctic

from tcapy.util.loggermanager import LoggerManager

constants = Constants()

logger = LoggerManager.getLogger(__name__)

# 'dukascopy' or 'ncfx'
data_source = 'ncfx'

# Change the market and trade data store as necessary
market_data_store = 'arctic-' + data_source
ncfx_available = True

if constants.ncfx_url is None:
    ncfx_available = False

ticker = 'EURUSD'
reverse_ticker = 'USDEUR'
start_date = '01 May 2017'
finish_date = '06 May 2017'

short_start_date = "04 May 2017 00:00"
short_finish_date = "04 May 2017 00:10"

# TCAMarketTradeLoader examples
def example_market_data_convention():
    """Loads market data in the correct convention
    """
    market_loader = Mediator.get_tca_market_trade_loader()

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker=ticker,
                             market_data_store=market_data_store)

    market_correct_conv_df = market_loader.get_market_data(tca_request)

    tca_request.ticker = reverse_ticker
    market_reverse_conv_df = market_loader.get_market_data(tca_request)

    market_correct_conv_df, market_reverse_conv_df = \
        market_correct_conv_df.align(market_reverse_conv_df, join='inner')

    synthetic_market_df = market_correct_conv_df.copy()
    synthetic_market_df['mid'] = 1.0 / synthetic_market_df['mid']

    # Check time series are equal to each other
    assert (market_reverse_conv_df['mid'] - synthetic_market_df['mid']).sum() == 0


def example_market_data_non_usd_cross():
    """Example for loading market data which has more exotic crosses, which are unlikely to be collected. For these
    exotic crosses tcapy will calculate the cross rates via the USD legs, eg. NZDCAD would be calculated from
    NZDUSD and USDCAD data.
    """
    market_loader = Mediator.get_tca_market_trade_loader()

    tca_request = TCARequest(start_date=start_date, finish_date=finish_date, ticker='NZDUSD',
                             market_data_store=market_data_store)

    market_base_df = market_loader.get_market_data(tca_request)

    tca_request.ticker = 'USDCAD'
    market_terms_df = market_loader.get_market_data(tca_request)

    market_df = pd.DataFrame(market_base_df['mid'] * market_terms_df['mid']).dropna()

    tca_request.ticker = 'NZDCAD'
    market_direct_df = market_loader.get_market_data(tca_request)

    market_df, market_direct_df = market_df.align(market_direct_df, join='inner')

    # check time series are equal to each other
    assert (market_df['mid'] - market_direct_df['mid']).sum() == 0

#### DatabaseSource examples
def example_ncfx_download():
    """Example of how to download directly from New Change FX using their RestAPI with the lower level DatabaseSourceNCFX
    class. It is recommended to cache this data locally, typically in an Arctic (dependent on license terms), to reduce
    latency when running TCA computations.

    In order to run this, you need to have a subscription with New Change FX, otherwise this will not work.
    """
    start_date = "04 May 2017 00:00"
    finish_date = "04 May 2017 00:05"
    ticker = "EURUSD"

    if ncfx_available:
        data_loader = DatabaseSourceNCFX()

        df = data_loader.fetch_market_data(start_date, finish_date, ticker=ticker)

        print(df)

def example_arctic_ncfx_download():
    """Example of downloading from the lower level Arctic wrapper directly (DatabaseSourceArctic, rather than using any
    higher level classes such as TCAMarketTradeDataLoader
    """

    data_loader = DatabaseSourceArctic(postfix='ncfx')

    df = data_loader.fetch_market_data(short_start_date, short_finish_date, ticker=ticker)

    print(df)

def example_arctic_dukacopy_download():
    """Example of downloading from the lower level Arctic wrapper directly (DatabaseSourceArctic, rather than using any
    higher level classes such as TCAMarketTradeDataLoader
    """

    data_loader = DatabaseSourceArctic(postfix='dukascopy')

    df = data_loader.fetch_market_data('01 May 2017', '30 May 2017', ticker='EURUSD')

    print(df)

if __name__ == '__main__':
    import time

    start = time.time()

    # TCAMarketTradeLoadder
    example_market_data_convention()
    example_market_data_non_usd_cross()

    # DatabaseSource examples
    example_ncfx_download()
    example_arctic_ncfx_download()
    example_arctic_dukacopy_download()

    finish = time.time()
    print('Status: calculated ' + str(round(finish - start, 3)) + "s")
