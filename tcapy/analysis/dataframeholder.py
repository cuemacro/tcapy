from __future__ import division, print_function

__author__ = 'saeedamen'  # Saeed Amen / saeed@cuemacro.com

#
# Copyright 2017 Cuemacro Ltd. - http//www.cuemacro.com / @cuemacro
#
# See the License for the specific language governing permissions and limitations under the License.
#

from collections import OrderedDict

from tcapy.conf.constants import Constants

from tcapy.util.loggermanager import LoggerManager
from tcapy.util.mediator import Mediator

constants = Constants()

tcapy_version = constants.tcapy_version

class DataFrameHolder(object):
    """DataFrameHolder can be used to store DataFrames which contain trading executions and orders (or market data
    for indiviudal tickers) etc. We can store different DataFrames for trades and orders (or for different market data tickers)

    """

    def __init__(self):
        """Initialising the class, creates an empty ordered dictionary, which we shall store trades and orders in later.
        """
        self._df_dict = OrderedDict()

    def add_dataframe(self, df, name):
        """Adds a trade/order to the holder. Internally, we add them to existing trades/orders.

        Parameters
        ----------
        df : DataFrame
            Contains market data or trades/orders

        name : str
            Name of the key eg. trades/orders (eg. 'trade_df' or 'order_df')

        Returns
        -------

        """

        # if we haven't already added this trade/order type, create a new key, otherwise append it to existing list
        if name not in self._df_dict.keys():
            self._df_dict[name] = [df]
        else:
            self._df_dict[name] = [self._df_dict[name], df]

    def replace_dataframe(self, df, name):
        """Replaces any existing market data or trade/order with the same name

        Parameters
        ----------
        df : DataFrame
            Trade/order

        name : str
            Name of the trade/order

        Returns
        -------

        """
        self._df_dict[name] = [df]

    def add_dataframe_holder(self, dataframe_holder):
        """Adds an existing market or trade/order holder

        Parameters
        ----------
        dataframe_holder : DataFrameHolder
            Object containing multiple trade/order dataset

        Returns
        -------

        """
        if dataframe_holder is not None:

            if not(dataframe_holder.is_empty()):
                for k in dataframe_holder.keys():
                    self.add_dataframe(dataframe_holder.get_dataframe_by_key(k), k)

    def add_dataframe_dict(self, df_dict):
        """Adds a dictionary of market or trades/orders internally

        Parameters
        ----------
        df_dict : dict
            Dictionary containing market data or trades/orders

        Returns
        -------

        """
        if df_dict is not None:
            if len(df_dict.keys()) > 0:
                for k in df_dict.keys():
                    self.add_dataframe(df_dict[k], k)

    def get_dataframe_by_key(self, key, combined=True, start_date=None, finish_date=None):
        """Gets a specific trade/order and combine it into a single DataFrame.

        Parameters
        ----------
        key : str
            Which market data ticker or trades/order to return

        combined : True
            Should we combine all the market data for a specific ticker or trades (or orders) into a single DataFrame before returning?

        Returns
        -------
        DataFrame
        """
        if key in self._df_dict.keys():
            dataframe_key_list = self._df_dict[key]

            if 'df' in key:

                try:
                    df = Mediator.get_volatile_cache(version=tcapy_version).get_dataframe_handle(
                        Mediator.get_util_func().flatten_list_of_lists(dataframe_key_list), burn_after_reading=True)
                except Exception as e:
                    # print("DATAFRAMEHOLDER ERROR" + str(e))
                    df = dataframe_key_list

                if combined:
                    df = Mediator.get_time_series_ops().concat_dataframe_list(df)

                if df is not None:
                    if not(df.empty):
                        df = df.sort_index()

                if start_date is not None and finish_date is not None:
                    df = Mediator.get_time_series_ops().filter_start_finish_dataframe(df, start_date, finish_date)

                return df
            # elif 'fig' in key:
            #     try:
            #         df = self._volatile_cache.get_dataframe_handle(
            #             self._util_func.flatten_list_of_lists(dataframe_key_list), burn_after_reading=True)
            #     except:
            #         df = dataframe_key_list
            #
            #     if combined:
            #
            #         xy_dict = {}
            #
            #         for fig in df:
            #             for trace in fig['data']:
            #                 name = trace['name']
            #
            #                 xy_dict[name + '_x'] = []
            #                 xy_dict[name + '_y'] = []
            #                 xy_dict['trace_name_list'] = []
            #
            #         for fig in df:
            #             for trace in fig['data']:
            #                 name = trace['name']
            #
            #                 xy_dict[name + '_x'].append(trace['x'])
            #                 xy_dict[name + '_y'].append(trace['y'])
            #
            #                 if name not in xy_dict['trace_name_list']:
            #                     xy_dict['trace_name_list'].append(name)
            #
            #         fig = df[0]
            #
            #         # aggregate all the x & y values
            #         for i in range(0, len(fig['data'])):
            #             name = fig['data'][i]['name']
            #
            #             for j in range(1, len(xy_dict[name + '_x'])):
            #                 fig['data'][i]['x'].extend(xy_dict[name + '_x'])
            #                 fig['data'][i]['y'].extend(xy_dict[name + '_y'])
            #
            #         return fig
            else:
                # otherwise different type of metadata (don't attempt to combine it)
                try:
                    df = Mediator.get_volatile_cache(version=tcapy_version).get_dataframe_handle(
                        Mediator.get_util_func().flatten_list_of_lists(dataframe_key_list), burn_after_reading=True)
                except Exception as e:
                    print(e)
                    df = dataframe_key_list

                if isinstance(df, list):
                    return df[0]

                return df

        return None

    def is_empty(self):
        """Is our internal store of trade/orders empty?

        Returns
        -------
        bool
        """
        return len(self._df_dict) == 0

    def check_empty_combined_dataframe_dict(self, df_dict=None):

        if df_dict is None:
            df_dict = self.get_combined_dataframe_dict()

        logger = LoggerManager().getLogger(__name__)

        valid_data = True

        if df_dict is not None:
            if len(df_dict.keys()) > 0:
                t_remove = []

                for t in df_dict.keys():
                    if df_dict[t] is None:
                        logger.warn("Market/trade/order data not in " + t)
                        t_remove.append(t)
                    else:
                        if df_dict[t].empty:
                            logger.warn("Market/trade/order data not in " + t)
                            t_remove.append(t)

                for t in t_remove:
                    df_dict.pop(t)
            else:
                valid_data = False

            if len(df_dict.keys()) == 0:
                valid_data = False
        else:
            valid_data = False

        return valid_data

    def get_combined_dataframe_dict(self, start_date=None, finish_date=None):
        """Combines all the trades into a single DataFrame (and the same for all orders). Then creates a dictionary of
        these DataFrames (does not attempt to combine metadata, which are left intact).

        Returns
        -------
        dict
        """
        df_dict = OrderedDict()

        for key in self._df_dict.keys():
            df_dict[key] = self.get_dataframe_by_key(key, combined=True, start_date=start_date, finish_date=finish_date)

        return df_dict

    def keys(self):
        """Returns the names of all the market or trades/orders stored internally.

        Returns
        -------
        str (list)
        """
        return Mediator.get_util_func().dict_key_list(self._df_dict.keys())
