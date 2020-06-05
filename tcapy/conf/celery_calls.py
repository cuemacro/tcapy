from __future__ import print_function

__author__ = 'saeedamen' # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2018 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import sys
import platform
import os
from celery import Celery

app = Celery('tcapy')

# get the path to tcapy project (two levels above this one)
PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    sys.path.append(PATH)
except:
    pass

from tcapy.conf.constants import Constants

constants = Constants()

if 'windows' in str(platform.platform()).lower():
    # Needs to put in quotes for Windows
    app.config_from_object('tcapy.conf.celeryconfig')
else:
    import tcapy.conf.celeryconfig

    app.config_from_object(tcapy.conf.celeryconfig)

from tcapy.analysis.tcatickerloaderimpl import TCATickerLoaderImpl

tca_ticker_loader = TCATickerLoaderImpl()

#### add wrapper methods here from the tcapy project that you want to run through Celery ###############################

#### CAREFUL! Certain objects cannot be pickled and sent to Celery. In particular, classes which contain "LoggerManager"
#### objects as field variables cannot be sent to Celery (we instead have create Loggers within each method, whenever
#### we want to use.

@app.task(name='get_market_trade_holder_via_celery')
def get_market_trade_holder_via_celery(tca_request):
    """Gets the both the market data and trade/order data associated with a TCA calculation as a tuple of
    (DataFrame, DataFrameHolder)

    Parameters
    ----------
    tca_request : TCARequest
        Parameters for a TCA calculation

    Returns
    ------
    DataFrame, DataFrameHolder
    """

    #from celery import group

    #return group(get_market_trade_holder_via_celery(tca_request), get_trade_order_holder_via_celery(tca_request))
    return tca_ticker_loader.get_market_trade_order_holder(tca_request)

@app.task(name='get_market_data_via_celery')
def get_market_data_via_celery(market_request):
    """Gets the market data associated with a TCA calculation as DataFrame

    Parameters
    ----------
    market_request : MarketRequest
        Parameters for a TCA calculation

    Returns
    ------
    DataFrame
    """

    return tca_ticker_loader.get_market_data(market_request)

@app.task(name='get_trade_order_holder_via_celery')
def get_trade_order_holder_via_celery(tca_request):
    """Gets the DataFrameHolder associated with a TCA calculation

    Parameters
    ----------
    tca_request : TCARequest
        Parameters for a TCA calculation

    Returns
    ------
    DataFrameHolder
    """

    try:
        return tca_ticker_loader.get_trade_order_holder(tca_request)
    except Exception as e:
        print(str(e))


@app.task(name='calculate_metrics_single_ticker_via_celery')
def calculate_metrics_single_ticker_via_celery(tuple, tca_request, dummy_market):
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

    return tca_ticker_loader.calculate_metrics_single_ticker(tuple, tca_request, dummy_market)

@app.task(name='get_market_trade_holder_and_calculate_metrics_single_ticker_via_celery')
def get_market_trade_holder_and_calculate_metrics_single_ticker_via_celery(tca_request, dummy_market):
    """Gets the both the market data and trade/order data associated with a TCA calculation as a tuple of
    (DataFrame, DataFrameHolder)

    Parameters
    ----------
    tca_request : TCARequest
        Parameters for a TCA calculation

    Returns
    ------
    DataFrame, DataFrameHolder
    """

    #from celery import group

    #return group(get_market_trade_holder_via_celery(tca_request), get_trade_order_holder_via_celery(tca_request))
    return tca_ticker_loader.calculate_metrics_single_ticker(tca_ticker_loader.get_market_trade_order_holder(tca_request),
        tca_request, dummy_market)
