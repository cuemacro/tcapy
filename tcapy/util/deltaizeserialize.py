from __future__ import print_function, division

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2020 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

import math
import json
import pandas as pd
import pyarrow as pa

from plotly.utils import PlotlyJSONEncoder
import plotly.graph_objs as go

from tcapy.conf.constants import Constants
from tcapy.util.utilfunc import UtilFunc
from tcapy.util.timeseries import TimeSeriesOps
from tcapy.util.loggermanager import LoggerManager

context = pa.default_serialization_context()

constants = Constants()

class DeltaizeSerialize(object):
    """This class can be used to serialize and deserialize pandas DataFrames and Plotly Figure objects with/without
    compression. It can also split up large DataFrames into chunks, which is often necessary if we are using Redis
    and there's a limit to how much we can store in each key.

    """

    def __init__(self):
        self._time_series_ops = TimeSeriesOps()
        self._util_func = UtilFunc()

    def convert_python_to_binary(self, obj, key):
        """

        Parameters
        ----------
        obj : DataFrame (or Figure)
            Object to serialize

        key : str
            Key to store object

        Returns
        -------
        binary, str
        """

        if obj is None:
            return None

        # For pandas DataFrames
        if '_df' in key and isinstance(obj, pd.DataFrame):
            obj_list = self._chunk_dataframes(obj, chunk_size_mb=constants.volatile_cache_redis_max_cache_chunk_size_mb)

            # If compression has been specified (recommended!)
            if '_comp' in key:
                if constants.volatile_cache_redis_format == 'msgpack':

                    for i in range(0, len(obj_list)):
                        if obj_list[i] is not None:
                            obj_list[i] = obj_list[i].to_msgpack(
                                compress=constants.volatile_cache_redis_compression[
                                    constants.volatile_cache_redis_format])

                elif constants.volatile_cache_redis_format == 'arrow':
                    # Set the size of each compressed object, so can read back later
                    # eg. key might be xxxx_size_354534_size_345345_endsize etc.
                    # Ignore bit before first '_size_' and after '_endsize'
                    for i in range(0, len(obj_list)):
                        if obj_list[i] is not None:
                            ser = context.serialize(obj_list[i]).to_buffer()

                            obj_list[i] = pa.compress(ser,
                                                      codec=constants.volatile_cache_redis_compression[
                                                          constants.volatile_cache_redis_format],
                                                      asbytes=True)

                            key = key + '_size_' + str(len(ser))

                    key = key + '_endsizearrow_'

                else:
                    raise Exception("Invalid volatile cache format specified.")
            elif '_comp' not in key:
                if constants.volatile_cache_redis_format == 'msgpack':

                    for i in range(0, len(obj_list)):
                        if obj_list[i] is not None:
                            obj_list[i] = obj_list[i].to_msgpack()
                elif constants.volatile_cache_redis_format == 'arrow':
                    # context = pa.default_serialization_context()

                    for i in range(0, len(obj_list)):
                        if obj_list[i] is not None:
                            obj_list[i] = context.serialize(obj_list[i]).to_buffer().to_pybytes()
                else:
                    raise Exception("Invalid volatile cache format specified.")

        # For Plotly JSON style objects (assume these will fit in the cache, as they tend to used downsampled data)
        elif '_fig' in key:
            # print("--------------- Converting " + key)
            # print(obj)
            obj_list = [self._plotly_fig_2_json(obj)]
        else:
            obj_list = [obj]

        return obj_list, key

    def convert_binary_to_python(self, obj, key):
        if obj is None: return None

        if '_df' in key:
            if not (isinstance(obj, list)):
                obj = [obj]

            if constants.volatile_cache_redis_format == 'msgpack':

                for i in range(0, len(obj)):
                    if obj[i] is not None:
                        obj[i] = pd.read_msgpack(obj[i])

            elif constants.volatile_cache_redis_format == 'arrow':

                # If compressed we need to know the size, to decompress it
                if '_comp' in key:
                    # Get the size of each compressed object
                    # eg. key might be xxxx_size_354534_size_345345_endsize etc.
                    # Ignore bit before first '_size_' and after '_endsize'

                    start = '_size_'
                    end = '_endsizearrow_'

                    if len(obj) > 0:
                        key = self._util_func.find_sub_string_between(key, start, end)
                        siz = self._util_func.keep_numbers_list(key.split('_size_'))

                    for i in range(0, len(obj)):
                        if obj[i] is not None:
                            obj[i] = pa.decompress(obj[i],
                                                   codec=constants.volatile_cache_redis_compression[
                                                       constants.volatile_cache_redis_format],
                                                   decompressed_size=siz[i])

                            obj[i] = context.deserialize(obj[i])
                else:
                    for i in range(0, len(obj)):
                        if obj[i] is not None:
                            obj[i] = context.deserialize(obj[i])

                # Need to copy because Arrow doesn't allow writing on a DataFrame
                for i in range(0, len(obj)):
                    if obj[i] is not None:
                        obj[i] = obj[i].copy()
            else:
                raise Exception("Invalid volatile cache format specified.")

            if len(obj) == 1:
                obj = obj[0]
            elif len(obj) > 1:
                obj = pd.concat(obj)
            else:
                obj = None

        elif '_fig' in key:
            # print("--------- " + len(obj) + " ---------")
            obj = self._plotly_from_json(obj[0].decode("utf-8"))

        return obj

    def encode_delta(self, df):
        pass

    def decode_delta(self, df):
        pass

    def _plotly_fig_2_json(self, fig):
        """Serialize a plotly figure object to JSON so it can be persisted to disk.
        Figure's persisted as JSON can be rebuilt using the plotly JSON chart API:

        http://help.plot.ly/json-chart-schema/

        If `fpath` is provided, JSON is written to file.

        Modified from https://github.com/nteract/nteract/issues/1229
        """

        return json.dumps({'data': json.loads(json.dumps(fig.data, cls=PlotlyJSONEncoder)),
                           'layout': json.loads(json.dumps(fig.layout, cls=PlotlyJSONEncoder))})

    def _plotly_from_json(self, fig):
        """Render a plotly figure from a json file"""

        v = json.loads(fig)

        return go.Figure(data=v['data'], layout=v['layout'])

    def _chunk_dataframes(self, obj, chunk_size_mb=constants.volatile_cache_redis_max_cache_chunk_size_mb):
        logger = LoggerManager.getLogger(__name__)

        # Can sometime have very large dataframes, which need to be split, otherwise won't fit in a single Redis key
        mem = obj.memory_usage(deep='deep').sum()
        mem_float = round(float(mem) / (1024.0 * 1024.0), 3)
        mem = '----------- ' + str(mem_float) + ' MB -----------'

        chunks = int(math.ceil(mem_float / chunk_size_mb))

        if chunks > 1:
            obj_list = self._time_series_ops.split_array_chunks(obj, chunks=chunks)
        else:

            obj_list = [obj]

        if obj_list != []:
            logger.debug("Pandas dataframe of size: " + mem + " in " + str(chunks) + " chunk(s)")

        return obj_list
