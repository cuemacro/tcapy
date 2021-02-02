from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import abc

import plotly.graph_objs as go

# compatible with Python 2 *and* 3:
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

from tcapy.conf.constants import Constants
from tcapy.util.deltaizeserialize import DeltaizeSerialize
from tcapy.util.timeseries import TimeSeriesOps
from tcapy.util.utilfunc import UtilFunc

import threading

from concurrent.futures import ThreadPoolExecutor as PoolExecutor

constants = Constants()

import pandas as pd

class VolatileCache(ABC):
    """Abstract class which is used to cache objects to be shared throughout the project
    """

    # _volatile_cache_lock = threading.Lock()

    def __init__(self):
        self._time_series_ops = TimeSeriesOps()
        self._util_func = UtilFunc()
        self._deltaize_serialize = DeltaizeSerialize()

        pass
        # self.logger = LoggerManager.getLogger(__name__)

    @abc.abstractmethod
    def clear_cache(self):
        pass

    @abc.abstractmethod
    def clear_key_match(self, key_match):
        pass

    @abc.abstractmethod
    def delete(self, key):
        pass

    @abc.abstractmethod
    def put(self, key, obj):
        pass

    @abc.abstractmethod
    def get(self, key):
        pass

    @abc.abstractmethod
    def _put(self, key, obj):
        pass

    @abc.abstractmethod
    def _get(self, key):
        pass

########################################################################################################################

class VolatileDictionary(VolatileCache):

    _volatile_dictionary_lock = threading.Lock()

    _db = {}

    def __init__(self):
        super(VolatileDictionary, self).__init__()

    def put(self, key, obj):
        self._put(key, obj)

    def get(self, key):
        self._get(key)

    def _put(self, key, obj):
        if isinstance(key, list):
            for k in key:
                with VolatileDictionary._volatile_dictionary_lock:
                    VolatileDictionary._db[k] = obj
        else:
            with VolatileDictionary._volatile_dictionary_lock:
                VolatileDictionary._db[key] = obj

    def _get(self, key):
        if isinstance(key, list):
            with VolatileDictionary._volatile_dictionary_lock:
                return [VolatileDictionary._get(k) for k in key]

        with VolatileDictionary._volatile_dictionary_lock:
            if key in VolatileDictionary._db.keys():
                    return VolatileDictionary._db[key]

        return None

    def clear_cache(self):
        with VolatileDictionary._volatile_dictionary_lock:
            self._db = {}

    def clear_key_match(self, key_match):
        with VolatileDictionary._volatile_dictionary_lock:
            for k in VolatileDictionary._db.keys():
                if key_match in k:
                    self._db.pop(k)

    def delete(self, key):
        with VolatileDictionary._volatile_dictionary_lock:
            if key in VolatileDictionary._db.keys():
                VolatileDictionary._db.pop(key)


import abc
import copy
import math

from tcapy.conf.constants import Constants
from tcapy.util.loggermanager import LoggerManager

constants = Constants()

import datetime
import random

import io
import base64
import threading

########################################################################################################################

class CacheHandle(dict):
    """This class can be used as a "proxy" like point for objects to be passed around the application, which are stored
    in the cache for temporary usage.
    """

    def __init__(self, tag, add_time_expiry=True):
        if add_time_expiry:
            self.__handle_name = str(datetime.datetime.utcnow()) + "_" + tag  + "_" + str(random.randint(0, 100000)) + '_expiry_'
        else:
            self.__handle_name = tag

    #def _create_unique_handle_name(self, _tag):
    #    return str(datetime.datetime.utcnow()) + "_" + _tag  + "_" + str(random.randint(0, 100000)) + '_expiry_'

    @property
    def handle_name(self):
        return self.__handle_name

    @handle_name.setter
    def handle_name(self, tag):
        self.__handle_name = str(datetime.datetime.utcnow()) + "_" + tag  + "_" + str(random.randint(0, 100000)) + '_expiry_'

########################################################################################################################

class VolatileAdvCache(VolatileCache):
    """Adds additional features for the cache to pass DataFrame by their return_cache_handles and to catch DataRequests

    """

    def _create_data_request_cache_key(self, data_store, ticker, start_date, finish_date, tag, offset_ms, unique=False):
        """Creates a key for a particular dataset, which can be used as the key in the cache, when storing the associated
        dataset.

        Parameters
        ----------
        data_store : str
            What database have you used?

        ticker : str
            Ticker/asset

        start_date : str
            Start date of our dataset

        finish_date : str
            Finish date of our dataset

        tag : str
            Tag associated with the data

        Returns
        -------
        str
        """

        inc_str = "_str"

        if "market_df" == tag:
            inc_str = ''

        comp = ''

        if constants.use_compression_with_period_caches:
            comp = '_comp'

        key = str(data_store) + '_' + ticker + '_' + str(start_date) + '_' + str(finish_date) + '_' + tag + "_" + str(
            offset_ms) + inc_str + comp

        if unique:
            key = key + "_" + str(datetime.datetime.utcnow()) + str(random.randint(1, 101))

        return key

    def get_data_request_cache(self, data_request, data_store, market_trade_order, data_offset_ms):
        """Fetches the DataFrame associated with DataRequest from the cache. If it doesn't exist than None is returned.

        Parameters
        ----------
        data_request : DataRequest
            Request for market or trade/order data

        data_store : str
            Data store (eg. arctic-ncfx)

        market_trade_order : str
            Is it market data or trade data (eg. market_df or trade_df)

        data_offset_ms : int
            How much should we offset DataRequest by

        Returns
        -------
        DataFrame
        """
        start_date = data_request.start_date
        finish_date = data_request.finish_date

        period = data_request.multithreading_params['cache_period']

        # Get the start of the month and end of the month (we cache data on the basis of whole months)
        # we then need to strip this down eg. if we ask for 20th Mar-25th Mar, actually select 1st Mar-30th Mar
        # then later truncate it in pd
        if period == 'month':
            if data_request.start_date.month == data_request.finish_date.month:
                start_date, _ = self._util_func.period_bounds(data_request.start_date, period=period)
                _, finish_date = self._util_func.period_bounds(data_request.finish_date, period=period)
        elif period == 'week':
            if data_request.start_date.weekofyear == data_request.finish_date.weekofyear:
                start_date, _ = self._util_func.period_bounds(data_request.start_date, period=period)
                _, finish_date = self._util_func.period_bounds(data_request.finish_date, period=period)
        else:
            Exception("Invalid period chunk specified!")

        key = self._create_data_request_cache_key(data_store, data_request.ticker,
                                                  start_date, finish_date, market_trade_order, data_offset_ms)

        df = None

        # Only try cache if reload has been requested!
        if not(data_request.reload):
            df = self.get(key=key)

        return start_date, finish_date, key, df

    def put_data_request_cache(self, data_request, key, df, period="month"):
        """Stores a DataRequest associated with a DataFrame in the cache, provided it is a full calendar month
        (which makes it easier to reuse later). Unusually shaped DataFrames are ignored

        Parameters
        ----------
        data_request : DataRequest
            A request for market or trade data

        key : str
            Key for use in the cache

        df : DataFrame
            DataFrame to be cache

        Returns
        -------

        """

        if period == 'month':
            if data_request.start_date.month == data_request.finish_date.month \
                    and data_request.start_date.year == data_request.finish_date.year:

                # want to make sure that empty DataFrames are noted (no point hammering database for a dataset we know is empty!)
                if df is None:
                    df = pd.DataFrame()

                self.put(key, df)
        elif period == 'week':
            if data_request.start_date.weekofyear == data_request.finish_date.weekofyear \
                    and data_request.start_date.year == data_request.finish_date.year:

                # want to make sure that empty DataFrames are noted (no point hammering database for a dataset we know is empty!)
                if df is None:
                    df = pd.DataFrame()

                self.put(key, df)


    def _create_cache_handle(self, obj, comp_add):
        fix = '_df'

        if isinstance(obj, pd.DataFrame):
            fix = '_df'
        elif isinstance(obj, go.Figure):
            fix = '_fig'

        return CacheHandle(fix + comp_add)

    def put_dataframe_handle(self, df, use_cache_handles=None, compression=constants.use_compression_with_handles):
        """For a provided DataFrame, returns the associated CacheHandle (if cache handle has been specified and at the same
        time stores this DataFrame in the cache) or simply the DataFrame itself if cache handling has not been specified.

        Parameters
        ----------
        df : DataFrame (list)
            DataFrame to be cached

        use_cache_handles : bool (default: None)
            Should be use a cache handle?

        compression : bool
            Should compression be used

        Returns
        -------
        DataFrame or CacheHandle
        """
        comp_add = ''

        if compression:
            comp_add = '_comp'

        if use_cache_handles is not None:
            if use_cache_handles:
                if isinstance(df, list):
                    handle = [self._create_cache_handle(d, comp_add) for d in df]
                else:
                    handle = self._create_cache_handle(df, comp_add)

                self.put(handle, df)

                return handle

        return df

    def get_dataframe_handle(self, df, burn_after_reading=True):
        """Fetches DataFrame from its associated CacheHandle. If provided with a DataFrame, it simply returns the same
        DataFrame.

        Parameters
        ----------
        df : DataFrame or CacheHandle
            The key reference to the cache (or the DataFrame itself)

        burn_after_reading : bool (True)
            Deletes this element from the cache after fetching

        Returns
        -------
        DataFrame
        """

        # This is a key CacheHandle reference to the cache
        if isinstance(df, CacheHandle):
            df = self.get(df, burn_after_reading=burn_after_reading)

        # For a list of key CacheHandle references
        if isinstance(df, list):
            if all(isinstance(item, CacheHandle) for item in df):
                df = self.get(df, burn_after_reading=burn_after_reading)

        # For a dictionary of key CacheHandle references
        if isinstance(df, dict):
            if all(isinstance(item, CacheHandle) for item in df.values()):

                df_new = {}

                # Get each key from the cache
                for k in df.keys():
                    df_new[k] = self.get(df[k], burn_after_reading=burn_after_reading)

                df = df_new

        # print(df)
        # otherwise we were just asking for a dataframe and didn't need to look up the key
        return df

    def put(self, key, obj, convert_cache_handle=True):
        """Puts a Python object in the cache with an associated key.

        Parameters
        ----------
        key : str (list)
            Key(s) to store for Python object

        obj : str (list)
            Object(s) to be cached

        Returns
        -------

        """
        logger = LoggerManager.getLogger(__name__)

        key = copy.copy(key)

        if not(isinstance(key, list)):
            key = [key]

        if not(isinstance(obj, list)):
            obj = [obj]

        for i in range(0, len(key)):
            if isinstance(key[i], CacheHandle):
                key[i] = key[i].handle_name

        logger.debug("Attempting to push " + str(key) + " to cache")

        # Before caching the object, we must convert it into a binary form (and usually compress)
        # Get back (potentionally) altered key
        for i in range(0, len(obj)):
            if obj[i] is None:
                obj[i] = pd.DataFrame()

            # If it's a CacheHandle retrieve the original object
            if isinstance(obj[i], CacheHandle) and convert_cache_handle:
                obj[i] = self.get_dataframe_handle(obj[i], burn_after_reading=True)

            obj[i], key[i] = self._deltaize_serialize.convert_python_to_binary(obj[i], key[i])

            if not(isinstance(obj[i], list)):
                obj[i] = [obj[i]]

        logger.debug("Now pushing " + str(key) + " to cache")

        try:
            filter_keep = []

            if (not(convert_cache_handle)):

                # Do not try to put CacheHandle object in again (they should already be in volatile cache, so don't play with them!)
                for i in range(0, len(key)):
                    if isinstance(*obj[i], CacheHandle):
                        filter_keep.append(False)
                    else:
                        filter_keep.append(True)

                key = [i for (i, v) in zip(key, filter_keep) if v]
                obj = [i for (i, v) in zip(obj, filter_keep) if v]

            self._put(key, obj)
            logger.debug("Pushed " + str(key) + " to cache")
        except Exception as e:
            logger.warning("Couldn't push " + str(key) + " to cache: " + str(e))

    def get(self, key, burn_after_reading=False):
        """Gets the object(s) associated with the key(s) or CacheHandle(s)

        Parameters
        ----------
        key : str or CacheHandle (list)
            Key(s) to be fetched

        burn_after_reading : bool (default: False)
            Should the key be erased after reading?

        Returns
        -------
        object
        """
        logger = LoggerManager.getLogger(__name__)

        key = copy.copy(key)

        single = False

        if not(isinstance(key, list)):
            key = [key]

            single = True

        for i in range(0, len(key)):
            if isinstance(key[i], CacheHandle):
                key[i] = key[i].handle_name

        obj = None

        try:
            obj = self._get(key, burn_after_reading=burn_after_reading)
        except Exception as e:
            logger.warning("Couldn't retrieve " + str(key) + " from cache: " + str(e))

        if ('market_df' in key):
            print("market_df")

        if single and obj is not None:
            return obj[0]

        return obj

########################################################################################################################

import redis

class VolatileRedis(VolatileAdvCache):

    _volatile_redis_lock = threading.Lock()
    _db = None
    _pool = None

    def __init__(self):
        super(VolatileRedis, self).__init__()

        with VolatileRedis._volatile_redis_lock:
            if VolatileRedis._db is None or VolatileRedis._pool is None:
                VolatileRedis._pool = redis.ConnectionPool(host=constants.volatile_cache_host_redis,
                                                  port=constants.volatile_cache_port_redis,
                                                  db=constants.volatile_cache_redis_internal_database)

                VolatileRedis._db = redis.StrictRedis(connection_pool=VolatileRedis._pool,
                                             socket_timeout=constants.volatile_cache_timeout_redis,
                                             socket_connect_timeout=constants.volatile_cache_timeout_redis,
                                                      password=constants.volatile_cache_redis_password,
                                                      ssl=constants.volatile_cache_redis_ssl,
                                                      ssl_ca_certs=constants.volatile_cache_redis_ssl_ca_certs)

    def _get(self, key, burn_after_reading=False):

        logger = LoggerManager.getLogger(__name__)
        logger.debug('Attempting to get list from cache: ' + str(key))

        old_key = key

        # Use a pipeline which is quicker for multiple database operations
        pipeline = VolatileRedis._db.pipeline()

        # Check if the key is inside Redis (may have the "size" after it, which will be needed to decompress)
        for k in key:
            pipeline.keys(k + "*")

        key = pipeline.execute()
        key = self._util_func.flatten_list_of_lists(key)

        if key != []:
            # Convert byte to string
            key = [k.decode("utf-8") for k in key]

            pipeline = VolatileRedis._db.pipeline()

            # Get list of values for each element
            for k in key:
                pipeline.lrange(k, 0, -1)

            if burn_after_reading:
                key_burn = [k for k in key if '_expiry_' in k]

                self.delete(key_burn, pipeline=pipeline)

            cache_output = pipeline.execute()
        else:
            cache_output = [None] * len(old_key)
            key = old_key

        if burn_after_reading:
            if len(cache_output) == len(key) + 1:
                logger.debug("Deleted " + str(cache_output[-1]) + ' keys')

                cache_output = cache_output[:-1]

        for i in range(0, len(key)):
            if cache_output[i] is not None:
                try:
                    cache_output[i] = self._deltaize_serialize.convert_binary_to_python(cache_output[i], key[i])
                except Exception as e:
                    logger.error("Error converting binary object to Python for key: " + key[i] + " and " + str(e))

                    # print(cache_output[i])

                    cache_output[i] = None

        # print(cache_output)

        return cache_output

    def _put(self, key, obj):

        if obj is None:
            return

        # Use a pipeline which is quicker for multiple database operations
        pipeline = self._db.pipeline()

        self.delete(key, pipeline=pipeline)

        for i in range(0, len(key)):
            if obj[i] is not None:
                if obj[i] != [None]:
                    if obj[i] != []:
                        pipeline.rpush(key[i], *obj[i])

        if not(isinstance(key, list)):
            for k in key:
                if "_expiry_" in k:
                    pipeline.expire(k, constants.volatile_cache_expiry_seconds)

        try:
            pipeline.execute()
        except Exception as err:
            print(str(err))

    #def _get(self, key):
    #    return self._db.lrange(key, 0, -1)

    def clear_cache(self):
        try:
            VolatileRedis._db.flushall()
        except Exception as err:
            LoggerManager.getLogger(__name__).debug('Warn did not clear cache: ' + str(err))

    def clear_key_match(self, key_match):
        # Allow deletion of keys by pattern matching
        keys = VolatileRedis._db.keys(key_match)

        if len(keys) > 0:
            VolatileRedis._db.delete(*keys)

    def delete(self, key, pipeline=None):

        if key is not None:
            if key != []:

                execute = False

                if pipeline is None:
                    pipeline = VolatileRedis._db
                    execute = True

                # UNLINK is *much* quicker when removing large valued keys compared to "delete" (only works with Redis 4 onwards)
                try:
                    pipeline.execute_command('UNLINK', *key)
                except:
                    # If fails try delete which works with older versions of Redis
                    pipeline.delete(*key)

                if execute:
                    pipeline.execute()