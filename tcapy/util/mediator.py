from __future__ import print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from tcapy.conf.constants import Constants
from tcapy.util.deltaizeserialize import DeltaizeSerialize
from tcapy.util.singleton import Singleton
from tcapy.util.timeseries import TimeSeriesOps
from tcapy.util.utilfunc import UtilFunc

import threading

constants = Constants()

class Mediator(object):
    """Mediator acts as a source for static/one off objects such as the VolatileCache, TCAMarketTradeLoader,
    TCATickerLoader etc.  Also allows users to create their own subclasses of these and distribute universally to the
    project, without having to worry about importing the appropriate classes/implementations.

    """
    __metaclass__ = Singleton

    _volatile_cache = {}
    _volatile_cache_lock = threading.Lock()

    _deltaize_serialize = None
    _deltaize_serialize_lock = threading.Lock()

    _tca_market_trade_loader = {}
    _tca_market_trade_loader_lock = threading.Lock()

    _tca_ticker_loader = {}
    _tca_ticker_loader_lock = threading.Lock()

    _database_source_picker = {}
    _database_source_picker_lock = threading.Lock()

    _data_norm = {}
    _data_norm_lock = threading.Lock()

    _time_series_ops = None
    _time_series_ops_lock = threading.Lock()

    _util_func = None
    _util_func_lock = threading.Lock()

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get_volatile_cache(volatile_cache_engine=constants.volatile_cache_engine):
        if volatile_cache_engine not in Mediator._volatile_cache.keys():
            with Mediator._volatile_cache_lock:
                if volatile_cache_engine == 'redis':
                    from tcapy.data.volatilecache import VolatileRedis
                    Mediator._volatile_cache[volatile_cache_engine] = VolatileRedis()

                elif volatile_cache_engine == 'plasma':
                    from tcapy.data.volatilecache import VolatilePlasma
                    Mediator._volatile_cache[volatile_cache_engine] = VolatilePlasma()


        return Mediator._volatile_cache[volatile_cache_engine]

    @staticmethod
    def get_tca_market_trade_loader(version=constants.tcapy_version):

        if version not in Mediator._tca_market_trade_loader.keys():
            with Mediator._tca_market_trade_loader_lock:

                from tcapy.analysis.tcamarkettradeloaderimpl import TCAMarketTradeLoaderImpl as TCAMarketTradeLoader

                Mediator._tca_market_trade_loader[version] = TCAMarketTradeLoader(version=version)

        return Mediator._tca_market_trade_loader[version]

    @staticmethod
    def get_tca_ticker_loader(version=constants.tcapy_version):

        if version not in Mediator._tca_ticker_loader.keys():
            with Mediator._tca_ticker_loader_lock:

                from tcapy.analysis.tcatickerloaderimpl import TCATickerLoaderImpl as TCATickerLoader

                Mediator._tca_ticker_loader[version] = TCATickerLoader(version=version)

        return Mediator._tca_ticker_loader[version]

    @staticmethod
    def get_database_source_picker(version=constants.tcapy_version):

        if version not in Mediator._database_source_picker.keys():
            with Mediator._database_source_picker_lock:

                from tcapy.data.databasesource import DatabaseSourcePicker as DatabaseSourcePicker

                Mediator._database_source_picker[version] = DatabaseSourcePicker()

        return Mediator._database_source_picker[version]

    @staticmethod
    def get_data_norm(version=constants.tcapy_version):

        if version not in Mediator._data_norm.keys():
            with Mediator._data_norm_lock:
                if version == 'test_tcapy':
                    try:
                        from tcapygen.datafactorygen import DataNormGen as DataNorm
                    except:
                        from tcapy.data.datafactory import DataNorm
                elif version == 'user':

                    try:
                        from tcapyuser.datafactoryuser import DataNormUser as DataNorm
                    except:
                        from tcapy.data.datafactory import DataNorm

                Mediator._data_norm[version] = DataNorm()

        return Mediator._data_norm[version]

    @staticmethod
    def get_time_series_ops():
        with Mediator._time_series_ops_lock:

            if Mediator._time_series_ops is None:
                Mediator._time_series_ops = TimeSeriesOps()

        return Mediator._time_series_ops

    @staticmethod
    def get_deltaize_serialize():
        with Mediator._deltaize_serialize_lock:

            if Mediator._deltaize_serialize is None:
                Mediator._deltaize_serialize = DeltaizeSerialize()

        return Mediator._deltaize_serialize


    @staticmethod
    def get_util_func():
        with Mediator._util_func_lock:

            if Mediator._util_func is None:
                Mediator._util_func = UtilFunc()

        return Mediator._util_func


